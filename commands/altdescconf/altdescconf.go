// Package altdescconf contains commands to alter or describe configs.
package altdescconf

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/twmb/kgo/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/kv"
	"github.com/twmb/kcl/out"
)

// AlterConfigsCommand returns a new command that will alter topic or broker
// configs.
//
// Since this is used for both brokers and topics, the common alter logic is in
// this function; topic and broker command packages are expected to pass the
// use/short/long text strings and whether this is for a broker.
func AlterConfigsCommand(cl *client.Client, use, short, long string, forBroker bool) *cobra.Command {
	cfger := &cfger{
		querier: querier{
			cl:        cl,
			forBroker: forBroker,
		},
	}

	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Long:  long,
		Args: func() cobra.PositionalArgs {
			if forBroker {
				return cobra.MaximumNArgs(1)
			}
			return cobra.ExactArgs(1)
		}(),
		ValidArgs: []string{
			"--kv",
			"--dry",
			"--no-confirm",
		},
		Run: func(_ *cobra.Command, args []string) {
			cfger.alter(args)
		},
	}

	cmd.Flags().StringSliceVarP(&cfger.configKVs, "kv", "k", nil, "list of (k)ey value config parameters to set (comma separtaed or repeated flag; e.g. cleanup.policy=compact,preallocate=true)")
	cmd.Flags().BoolVarP(&cfger.dryRun, "dry", "d", false, "dry run: validate the config alter request, but do not apply")
	cmd.Flags().BoolVar(&cfger.noConfirm, "no-confirm", false, "skip confirmation of to-be-lost unspecified existing dynamic config keys")

	return cmd
}

// querier wraps a client with whether we will be asking for a specific broker.
type querier struct {
	cl        *client.Client
	forBroker bool
}

// cfger issues alter configs commands.
type cfger struct {
	querier querier

	req       kmsg.AlterConfigsRequest
	configKVs []string
	noConfirm bool
	dryRun    bool
}

// alter actually issues an alter config command, where args can contain
// either nothing or a single topic or broker name.
func (cfger *cfger) alter(args []string) {
	req := kmsg.AlterConfigsRequest{
		ValidateOnly: cfger.dryRun,
		Resources: []kmsg.AlterConfigsRequestResource{
			{}, // reserve
		},
	}
	cfg := &req.Resources[0]

	kvs, err := kv.Parse(cfger.configKVs)
	out.MaybeDie(err, "unable to parse KVs: %v", err)

	if !cfger.noConfirm {
		cfger.confirmAlterLoss(args, kvs)
	}

	for _, kv := range kvs {
		cfg.ConfigEntries = append(cfg.ConfigEntries,
			kmsg.AlterConfigsRequestResourceConfigEntry{
				ConfigName:  kv.K,
				ConfigValue: kmsg.StringPtr(kv.V),
			})
	}

	var r client.Requestor = cfger.querier.cl.Client()

	if len(args) > 0 { // topic can have no args
		cfg.ResourceName = args[0]
	}
	cfg.ResourceType = 2 // topic
	if cfger.querier.forBroker {
		cfg.ResourceType = 4 // broker
		if cfg.ResourceName != "" {
			bid, err := strconv.Atoi(cfg.ResourceName)
			out.MaybeDie(err, "unable to parse broker ID: %v", err)
			r = cfger.querier.cl.Client().Broker(bid)
		}
	}

	kresp, err := r.Request(context.Background(), &req)
	out.MaybeDie(err, "unable to alter config: %v", err)

	if cfger.querier.cl.AsJSON() {
		out.ExitJSON(kresp)
	}
	resps := kresp.(*kmsg.AlterConfigsResponse).Resources
	if len(resps) != 1 {
		out.ExitErrJSON(kresp, "quitting; one alter requested but received %d responses", len(resps))
	}
	out.ErrAndMsg(resps[0].ErrorCode, resps[0].ErrorMessage)
}

// confirmAlterLoss prompts for yes or no when issuing alter configs
// for all config options that will be lost.
func (cfger *cfger) confirmAlterLoss(args []string, kvs []kv.KV) {
	existing := make(map[string]string, 10)
	_, describeResource := cfger.querier.issueDescribeConfig(args)
	for _, entry := range describeResource.ConfigEntries {
		switch entry.ConfigSource {
		case 4, 5: // static, default
			continue
		}
		val := "(null)"
		if entry.ConfigValue != nil {
			val = *entry.ConfigValue
		}
		if entry.IsSensitive {
			val = "(sensitive)"
		}
		existing[entry.ConfigName] = val
	}

	for _, kv := range kvs {
		delete(existing, kv.K)
	}

	if len(existing) > 0 {
		losing := make([]kv.KV, 0, len(existing))
		for k, v := range existing {
			losing = append(losing, kv.KV{k, v})
		}
		sort.Slice(losing, func(i, j int) bool {
			return losing[i].K < losing[j].K
		})
		fmt.Println("THIS ALTER WILL LOSE THE FOLLOWING CONFIG KEY/VALUES, IS THAT OK?")
		fmt.Println()
		for _, toLose := range losing {
			fmt.Printf("%s=%s\n", toLose.K, toLose.V)
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
//
// This dies if the response fails or the response has an ErrorCode
// or there is not just one resource in the response.
func (querier querier) issueDescribeConfig(maybeName []string) (
	*kmsg.DescribeConfigsResponse,
	*kmsg.DescribeConfigsResponseResource,
) {
	name := ""
	if len(maybeName) == 1 {
		name = maybeName[0]
	}

	resourceType := int8(2)
	var r client.Requestor = querier.cl.Client()

	if querier.forBroker {
		resourceType = 4
		if name != "" {
			if bid, err := strconv.Atoi(name); err != nil {
				out.Die("unable to parse broker ID: %v", err)
			} else {
				r = querier.cl.Client().Broker(bid)
			}
		}
	}

	req := kmsg.DescribeConfigsRequest{
		Resources: []kmsg.DescribeConfigsRequestResource{
			{
				ResourceType: resourceType,
				ResourceName: name,
			},
		},
	}

	kresp, err := r.Request(context.Background(), &req)
	out.MaybeDie(err, "unable to describe config: %v", err)
	resp := kresp.(*kmsg.DescribeConfigsResponse)

	if len(resp.Resources) != 1 {
		out.Die("quitting; one resource requested but received %d", len(resp.Resources))
	}
	resource := resp.Resources[0]
	if out.ErrAndMsg(resource.ErrorCode, resource.ErrorMessage) {
		out.Exit()
	}
	return resp, &resource
}

// DescribeConfigs issues a describe config command for a resource and prints
// the config.
//
// maybeName can either be nothing or a single topic or broker name.
// If it is a broker name, forBroker should be true.
//
// Directly describing an individual topic or broker dumps more information;
// without a name, this dumps information shared on all Kafka brokers.
func DescribeConfigs(cl *client.Client, maybeName []string, forBroker bool) {
	querier := querier{cl, forBroker}
	resp, resource := querier.issueDescribeConfig(maybeName)
	if cl.AsJSON() {
		out.ExitJSON(resp)
	}

	kvs := resource.ConfigEntries
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].ConfigName < kvs[j].ConfigName
	})

	tw := out.BeginTabWrite()
	defer tw.Flush()

	for _, kv := range kvs {
		key := kv.ConfigName
		if kv.ReadOnly {
			key += "*"
		}
		val := "(null)"
		if kv.ConfigValue != nil {
			val = *kv.ConfigValue
		}
		if kv.IsSensitive {
			val = "(sensitive)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t\n",
			key,
			val,
			configSourceForInt8(kv.ConfigSource),
		)
	}
}

func configSourceForInt8(i int8) string {
	switch i {
	case 0:
		return "UNKNOWN"
	case 1:
		return "DYNAMIC_TOPIC_CONFIG"
	case 2:
		return "DYNAMIC_BROKER_CONFIG"
	case 3:
		return "DYNAMIC_DEFAULT_BROKER_CONFIG"
	case 4:
		return "STATIC_BROKER_CONFIG"
	case 5:
		return "DEFAULT_CONFIG"
	}
	return ""
}
