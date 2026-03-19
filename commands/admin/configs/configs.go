// Package configs contains config altering and describing commands.
package configs

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "config",
		Aliases: []string{"configs"},
		Short:   "Alter or describe configs (topic, broker, broker logger, client-metrics, group).",
	}
	cmd.AddCommand(alterCommand(cl))
	cmd.AddCommand(describeCommand(cl))
	return cmd
}

type entity int8

const (
	entityUnknown       = 0
	entityTopic         = 2
	entityBroker        = 4
	entityBrokerLogger  = 8
	entityClientMetrics = 16
	entityGroup         = 32
)

// querier wraps a client with an entity and a requestor.
type querier struct {
	cl *client.Client

	rawEntity    string
	entity       entity
	resourceName string

	requestor client.Requestor
}

// parseEntity parses the entity and then, using that, sets resourceName and
// the requestor.
func (q *querier) parseEntity(args []string) {
	switch q.rawEntity {
	case "t", "topic":
		q.entity = entityTopic
	case "b", "broker":
		q.entity = entityBroker
	case "bl", "broker logger":
		q.entity = entityBrokerLogger
	case "cm", "client-metrics":
		q.entity = entityClientMetrics
	case "g", "group":
		q.entity = entityGroup
	default:
		out.Die("unrecognized entity type %q (allowed: t, topic, b, broker, bl, broker logger, cm, client-metrics, g, group)", q.rawEntity)
	}

	switch q.entity {
	case entityTopic, entityClientMetrics, entityGroup:
		if len(args) == 0 {
			out.Die("missing entity name")
		}
	}
	if len(args) > 0 {
		q.resourceName = args[0]
	}

	q.requestor = q.cl.Client()
	if q.entity == entityBroker && len(args) > 0 {
		bid, err := strconv.Atoi(args[0])
		out.MaybeDie(err, "unable to parse broker ID: %v", err)
		q.requestor = q.cl.Client().Broker(bid)
	}
}

func alterCommand(cl *client.Client) *cobra.Command {
	cfger := &cfger{
		querier: querier{
			cl: cl,
		},
	}

	cmd := &cobra.Command{
		Use:   "alter [ENTITY]",
		Short: "Alter configs (topic, broker, broker logger, client-metrics, group).",
		Long: `Alter configurations (0.11.0+).

Kafka has two modes of altering configurations: wholesale altering, and
incremental altering. The original altering method requires specifying all
configuration options; anything not specified will be lost. The incremental
method, introduced in Kafka 2.3.0, allows modifying individual values.

To opt into the new method, use the --inc flag. If using the old method, this
command by default checks for existing config key/value pairs that may be lost
in the alter and prompts if it is OK to lose those values. To skip this check,
use the --no-confirm flag. As well, with incremental altering, keys require
a prefix of either set, del, +, or - to indicate whether the value is to be
set, deleted, added to an array, or removed from an array.

Altering requires specifying the "entity type" being altered:
  t,  topic           topic configuration
  b,  broker          broker configuration
  bl, broker logger   broker logger configuration
  cm, client-metrics  client metrics subscription (Kafka 3.7+)
  g,  group           group configuration (Kafka 4.0+)

Altering topics, client-metrics, and groups always requires the entity name.
Altering brokers allows for leaving off the broker being altered; this will
update the dynamic configuration on all brokers. Updating an individual broker
causes the broker to reload its password files and allows for setting password
fields.
`,

		Example: `alter foo -s cleanup.policy=compact --delete preallocate

alter foo --dry --type topic --set preallocate=true --delete cleanup.policy

alter my-share-group -tg -s share.auto.offset.reset=earliest

alter my-subscription -tcm -s match=[client_software_name=kcl]`,

		Run: func(_ *cobra.Command, args []string) {
			cfger.alter(args)
		},
	}

	cmd.Flags().StringVarP(&cfger.rawEntity, "type", "t", "topic", "entity type (t, topic, b, broker, bl, broker logger, cm, client-metrics, g, group)")
	cmd.Flags().BoolVarP(&cfger.incremental, "inc", "i", false, "perform an incremental alter (Kafka 2.3.0+)")
	cmd.Flags().StringArrayVarP(&cfger.rawKVs, "kv", "k", nil, "key value config parameters; repeatable; if incremental, keys require prefix in [set:, del:, +:, -:]")
	cmd.Flags().MarkDeprecated("kv", "use --set, --delete, --append, --subtract instead")
	cmd.Flags().StringArrayVarP(&cfger.setKVs, "set", "s", nil, "set config key=value (incremental; repeatable)")
	cmd.Flags().StringArrayVar(&cfger.deleteKeys, "delete", nil, "delete config key (incremental; repeatable)")
	cmd.Flags().StringArrayVar(&cfger.appendKVs, "append", nil, "append to list config key=value (incremental; repeatable)")
	cmd.Flags().StringArrayVar(&cfger.subtractKVs, "subtract", nil, "subtract from list config key=value (incremental; repeatable)")
	cmd.Flags().BoolVarP(&cfger.dryRun, "dry", "d", false, "dry run: validate the config alter request, but do not apply")
	cmd.Flags().BoolVar(&cfger.noConfirm, "no-confirm", false, "skip confirmation of to-be-lost unspecified existing dynamic config keys")

	return cmd
}

type kv struct {
	k  string
	v  *string
	op int8 // 0: set, 1: remove, 2: add, 3: subtract
}

// cfger issues alter configs commands.
type cfger struct {
	querier

	rawKVs    []string
	parsedKVs []kv

	// New clean flags for incremental alter.
	setKVs      []string
	deleteKeys  []string
	appendKVs   []string
	subtractKVs []string

	incremental bool
	noConfirm   bool
	dryRun      bool
}

func (c *cfger) parseKVs() {
	// If any clean flags are used, auto-enable incremental mode and
	// prepend them to rawKVs with the appropriate prefix.
	if len(c.setKVs) > 0 || len(c.deleteKeys) > 0 || len(c.appendKVs) > 0 || len(c.subtractKVs) > 0 {
		c.incremental = true
		for _, kv := range c.setKVs {
			c.rawKVs = append(c.rawKVs, "set:"+kv)
		}
		for _, k := range c.deleteKeys {
			c.rawKVs = append(c.rawKVs, "del:"+k)
		}
		for _, kv := range c.appendKVs {
			c.rawKVs = append(c.rawKVs, "+:"+kv)
		}
		for _, kv := range c.subtractKVs {
			c.rawKVs = append(c.rawKVs, "-:"+kv)
		}
	}

	for _, rawKV := range c.rawKVs {
		split := strings.SplitN(rawKV, "=", 2)
		if c.incremental {
			colon := strings.IndexByte(split[0], ':')
			if colon == -1 {
				out.Die("missing op: prefix on key %q", split[0])
			}

			rawOp := split[0][:colon]
			split[0] = split[0][colon+1:]
			var op int8
			switch rawOp {
			case "s", "set":
				op = 0
			case "d", "del":
				op = 1
			case "+":
				op = 2
			case "-":
				op = 3
			default:
				out.Die("unrecognized incremental op %q; not in set [s, set, d, del, +, -]", rawOp)
			}

			var v *string
			if op == 0 || op == 2 {
				if len(split) != 2 {
					out.Die("set or append key %q missing value", split[0])
				}
				v = &split[1]
			}

			c.parsedKVs = append(c.parsedKVs, kv{k: split[0], v: v, op: op})

		} else {
			if len(split) != 2 {
				out.Die("key %q missing value", split[0])
			}
			if strings.Contains(split[0], ":") {
				out.Die("invalid incremental syntax on key %q", split[0])
			}
			c.parsedKVs = append(c.parsedKVs, kv{k: split[0], v: &split[1]})
		}
	}
}

// alter actually issues an alter config command, where args can contain
// either nothing or a single topic or broker name.
func (c *cfger) alter(args []string) {
	c.parseEntity(args)
	c.parseKVs()
	if c.incremental {
		c.alterIncremental()
	} else {
		c.alterOld()
	}
}

func (c *cfger) alterIncremental() {
	req := kmsg.IncrementalAlterConfigsRequest{
		ValidateOnly: c.dryRun,
		Resources: []kmsg.IncrementalAlterConfigsRequestResource{{
			ResourceType: kmsg.ConfigResourceType(c.entity),
			ResourceName: c.resourceName,
		}},
	}

	for _, kv := range c.parsedKVs {
		req.Resources[0].Configs = append(req.Resources[0].Configs, kmsg.IncrementalAlterConfigsRequestResourceConfig{
			Name:  kv.k,
			Op:    kmsg.IncrementalAlterConfigOp(kv.op),
			Value: kv.v,
		})
	}

	kresp, err := c.requestor.Request(context.Background(), &req)
	out.MaybeDie(err, "unable to alter config: %v", err)
	resp := kresp.(*kmsg.IncrementalAlterConfigsResponse)

	table := out.NewFormattedTable(c.cl.Format(), "config.alter", 1, "results",
		"RESOURCE", "ERROR", "ERROR MESSAGE")
	for _, resource := range resp.Resources {
		errMsg := ""
		if resource.ErrorMessage != nil {
			errMsg = *resource.ErrorMessage
		}
		table.Row(resource.ResourceName, resource.ErrorCode, errMsg)
	}
	table.Flush()
}

func (c *cfger) alterOld() {
	req := kmsg.AlterConfigsRequest{
		ValidateOnly: c.dryRun,
		Resources: []kmsg.AlterConfigsRequestResource{{
			ResourceType: kmsg.ConfigResourceType(c.entity),
			ResourceName: c.resourceName,
		}},
	}

	if !c.noConfirm {
		c.confirmAlterLoss()
	}

	for _, kv := range c.parsedKVs {
		req.Resources[0].Configs = append(req.Resources[0].Configs, kmsg.AlterConfigsRequestResourceConfig{
			Name:  kv.k,
			Value: kv.v,
		})
	}

	kresp, err := c.requestor.Request(context.Background(), &req)
	out.MaybeDie(err, "unable to alter config: %v", err)
	resp := kresp.(*kmsg.AlterConfigsResponse)

	table := out.NewFormattedTable(c.cl.Format(), "config.alter", 1, "results",
		"RESOURCE", "ERROR", "ERROR MESSAGE")
	for _, resource := range resp.Resources {
		errMsg := ""
		if resource.ErrorMessage != nil {
			errMsg = *resource.ErrorMessage
		}
		table.Row(resource.ResourceName, resource.ErrorCode, errMsg)
	}
	table.Flush()
}

// confirmAlterLoss prompts for yes or no when issuing alter configs
// for all config options that will be lost.
func (c *cfger) confirmAlterLoss() {
	existing := make(map[string]string, 10)
	_, describeResource := c.querier.issueDescribeConfig(false)
	for _, entry := range describeResource.Configs {
		switch entry.Source {
		case 4, 5: // static, default
			continue
		}
		val := "(null)"
		if entry.Value != nil {
			val = *entry.Value
		}
		if entry.IsSensitive {
			val = "(sensitive)"
		}
		existing[entry.Name] = val
	}

	for _, kv := range c.parsedKVs {
		delete(existing, kv.k)
	}

	if len(existing) > 0 {
		type kv struct {
			k, v string
		}
		losing := make([]kv, 0, len(existing))
		for k, v := range existing {
			losing = append(losing, kv{k, v})
		}
		sort.Slice(losing, func(i, j int) bool {
			return losing[i].k < losing[j].v
		})
		fmt.Println("THIS ALTER WILL LOSE THE FOLLOWING CONFIG KEY/VALUES, IS THAT OK?")
		fmt.Println()
		for _, toLose := range losing {
			fmt.Printf("%s=%s\n", toLose.k, toLose.v)
		}
		fmt.Println()

		for {
			fmt.Print("[y]es|[n]o > ")
			var s string
			fmt.Scanf("%s", &s)
			switch s {
			case "y", "yes":
				return
			case "n", "no":
				out.Die("aborting.")
			default:
				fmt.Printf("unrecognized input %q, valid options are y, yes, n, no\n", s)
			}
		}
	}
}

// issues a describe config for a single resource and returns
// the response and that resource.
func (q querier) issueDescribeConfig(withDocs bool) (
	*kmsg.DescribeConfigsResponse,
	*kmsg.DescribeConfigsResponseResource,
) {
	req := kmsg.DescribeConfigsRequest{
		Resources: []kmsg.DescribeConfigsRequestResource{{
			ResourceType: kmsg.ConfigResourceType(q.entity),
			ResourceName: q.resourceName,
		}},
		IncludeDocumentation: withDocs,
	}

	kresp, err := q.requestor.Request(context.Background(), &req)
	out.MaybeDie(err, "unable to describe config: %v", err)
	resp := kresp.(*kmsg.DescribeConfigsResponse)

	if len(resp.Resources) != 1 {
		out.Die("quitting; one resource requested but received %d", len(resp.Resources))
	}
	resource := resp.Resources[0]
	if resource.ErrorCode != 0 {
		out.ErrAndMsg(resource.ErrorCode, resource.ErrorMessage)
		out.Exit()
	}
	return resp, &resource
}

func describeCommand(cl *client.Client) *cobra.Command {
	q := querier{cl: cl}

	var withDocs, withTypes bool

	cmd := &cobra.Command{
		Use:     "describe [ENTITY]",
		Aliases: []string{"d"},
		Short:   "Describe configs (topic, broker, broker logger, client-metrics, group).",
		Long: `Describe configurations (Kafka 0.11.0+).

This command prints all key/value config values for a given entity. Read only
keys are suffixed with *.

Describing requires specifying the "entity type":
  t,  topic           topic configuration
  b,  broker          broker configuration
  bl, broker logger   broker logger configuration
  cm, client-metrics  client metrics subscription (Kafka 3.7+)
  g,  group           group configuration (Kafka 4.0+)

When describing brokers, if no broker ID is used, only dynamic (manually set)
key/value pairs are printed. If you wish to describe the full config for a
specific broker, be sure to pass a broker ID.
`,
		Example: `describe foo -tt

describe 1 -tb

describe --type broker // prints all dynamic broker key/value pairs

describe my-share-group -tg

describe my-subscription -tcm`,

		Run: func(_ *cobra.Command, args []string) {
			q.parseEntity(args)

			resp, resource := q.issueDescribeConfig(withDocs)

			kvs := resource.Configs
			sort.Slice(kvs, func(i, j int) bool {
				return kvs[i].Name < kvs[j].Name
			})

			var table *out.FormattedTable
			if resp.Version >= 3 && withTypes {
				table = out.NewFormattedTable(cl.Format(), "config.describe", 1, "configs",
					"KEY", "TYPE", "VALUE", "SOURCE")
			} else {
				table = out.NewFormattedTable(cl.Format(), "config.describe", 1, "configs",
					"KEY", "VALUE", "SOURCE")
			}
			for _, kv := range kvs {
				key := kv.Name
				if kv.ReadOnly {
					key += "*"
				}
				val := "(null)"
				if kv.Value != nil {
					val = *kv.Value
				}
				if kv.IsSensitive {
					val = "(sensitive)"
				}

				if resp.Version >= 3 && withTypes {
					table.Row(key, kv.ConfigType, val, kv.Source)
				} else {
					table.Row(key, val, kv.Source)
				}
			}
			table.Flush()

			if !withDocs || resp.Version < 3 {
				return
			}

			fmt.Println()
			for _, kv := range kvs {
				docs := kv.Documentation
				if docs == nil {
					continue
				}

				fmt.Println(kv.Name + ":")
				fmt.Println(*docs)
				fmt.Println()
			}
		},
	}

	cmd.Flags().StringVarP(&q.rawEntity, "type", "t", "topic", "entity type (t, topic, b, broker, bl, broker logger, cm, client-metrics, g, group)")
	cmd.Flags().BoolVar(&withDocs, "with-docs", false, "include documentation for config values (Kafka 2.6.0+)")
	cmd.Flags().BoolVar(&withTypes, "with-types", false, "include types of config values (Kafka 2.6.0+)")

	return cmd
}
