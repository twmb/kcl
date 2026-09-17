package clientquotas

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "quota",
		Aliases: []string{"client-quotas"},
		Short:   "Alter, describe, or resolve client quotas.",
	}
	cmd.AddCommand(describeClientQuotas(cl))
	cmd.AddCommand(alterClientQuotas(cl))
	return cmd
}

func describeClientQuotas(cl *client.Client) *cobra.Command {
	var (
		names    []string
		defaults []string
		any      []string
		strict   bool
	)

	cmd := &cobra.Command{
		Use:     "describe",
		Aliases: []string{"d"},
		Short:   "Describe client quotas.",
		Long: `Describe client quotas.

Requires Kafka 2.6.0+.

As mentioned in KIP-546, "by default, quotas are defined in terms of a user and
client ID, where the user acts as an opaque principal name, and the client ID
as a generic group identifier". KIP-612 added support for ips ("ip").

Describe client quotas takes an input list of named entities, default entities,
or omitted (any) entities and returns a all matched quotas and their
keys and values.

Named entities are in the format key=value, where key is either user,
client-id, or ip, and value is the name to be matched. Default entities and
omitted (any) entities just use the key.

This command is a filtering type of command, where anything that passes the
filter specified by flags is returned. Rows are sorted by entity.

EXAMPLES:
  kcl quota describe                              # every quota
  kcl quota describe --name user=alice            # quotas for user alice
  kcl quota describe --default user               # the default user quota
  kcl quota describe --any client-id --strict     # any client-id entity, and nothing else

SEE ALSO:
  kcl quota alter    alter client quotas
`,
		Args: cobra.ExactArgs(0),

		RunE: func(_ *cobra.Command, _ []string) error {
			req := &kmsg.DescribeClientQuotasRequest{
				Strict: strict,
			}

			validType := map[string]bool{
				"user":      true,
				"client-id": true,
				"ip":        true,
			}

			for _, name := range names {
				split := strings.SplitN(name, "=", 2)
				if len(split) != 2 {
					return out.Errf(out.ExitUsage, "name %q missing value", split[0])
				}
				k, v := split[0], split[1]
				k = strings.ToLower(k)
				if !validType[k] {
					return out.Errf(out.ExitUsage, "name type %q is invalid (allowed: user, client-id, ip)", split[0])
				}
				req.Components = append(req.Components, kmsg.DescribeClientQuotasRequestComponent{
					EntityType: k,
					MatchType:  0,
					Match:      &v,
				})
			}

			for _, def := range defaults {
				if !validType[def] {
					return out.Errf(out.ExitUsage, "default type %q is invalid (allowed: user, client-id, ip)", def)
				}
				req.Components = append(req.Components, kmsg.DescribeClientQuotasRequestComponent{
					EntityType: def,
					MatchType:  1,
				})
			}

			for _, a := range any {
				if !validType[a] {
					return out.Errf(out.ExitUsage, "any type %q is invalid (allowed: user, client-id, ip)", a)
				}
				req.Components = append(req.Components, kmsg.DescribeClientQuotasRequestComponent{
					EntityType: a,
					MatchType:  2,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to describe client quotas: %v", err)
			}
			resp := kresp.(*kmsg.DescribeClientQuotasResponse)

			if resp.ErrorCode != 0 {
				additional := ""
				if resp.ErrorMessage != nil {
					additional = ": " + *resp.ErrorMessage
				}
				return fmt.Errorf("%s%s", kerr.ErrorForCode(resp.ErrorCode), additional)
			}

			type row struct {
				entity string
				key    string
				value  float64
			}
			var rows []row
			for _, entry := range resp.Entries {
				var parts []entityPart
				for _, e := range entry.Entity {
					parts = append(parts, entityPart{e.Type, e.Name})
				}
				entity := entityString(parts)
				for _, value := range entry.Values {
					rows = append(rows, row{entity, value.Key, value.Value})
				}
			}
			slices.SortFunc(rows, func(a, b row) int {
				return cmp.Or(strings.Compare(a.entity, b.entity), strings.Compare(a.key, b.key))
			})

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "quotas", describeHeaders...)
			for _, r := range rows {
				table.Row(r.entity, r.key, r.value)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, describeHeaders...)

	cmd.Flags().StringArrayVar(&names, "name", nil, "type=name pair for exact name matching, where type is user, client-id, or ip; repeatable")
	cmd.Flags().StringArrayVar(&defaults, "default", nil, "type for default matching, where type is user, client-id, or ip; repeatable")
	cmd.Flags().StringArrayVar(&any, "any", nil, "type for any matching (names or default), where type is user, client-id, or ip; repeatable")
	cmd.Flags().BoolVar(&strict, "strict", false, "whether matches are strict, if true, entities with unspecified entity types are excluded")

	return cmd
}

var (
	describeHeaders = []string{"ENTITY", "KEY", "VALUE"}
	alterHeaders    = []string{"ENTITY", "ERROR", "MESSAGE"}
)

// entityPart is one type=name of a quota entity; a nil name is the default
// for the type.
type entityPart struct {
	typ  string
	name *string
}

// entityString prints a quota entity as {user=alice, client-id=<default>}.
func entityString(parts []entityPart) string {
	strs := make([]string, len(parts))
	for i, p := range parts {
		name := "<default>"
		if p.name != nil {
			name = *p.name
		}
		strs[i] = p.typ + "=" + name
	}
	return "{" + strings.Join(strs, ", ") + "}"
}

func alterClientQuotas(cl *client.Client) *cobra.Command {
	var (
		names    []string
		defaults []string
		dryRun   bool
		adds     []string
		deletes  []string
	)

	cmd := &cobra.Command{
		Use:   "alter",
		Short: "Alter client quotas.",
		Long: `Alter client quotas.

Requires Kafka 2.6.0+.

This command alters client quotas; to see a bit more of a description on
quotas, see the help text for client-quotas or read KIP-546.

Similar to describing, this command matches. Where describing filters for only
matches, this runs an alter on anything that matches.

The result prints one row per entity with ERROR and MESSAGE. --dry-run
validates the request without applying it; the row then carries what the
validation answered.

EXAMPLES:
  kcl quota alter --name user=alice --add producer_byte_rate=1048576
  kcl quota alter --default client-id --add consumer_byte_rate=2097152
  kcl quota alter --name user=alice --delete producer_byte_rate

SEE ALSO:
  kcl quota describe    describe client quotas
`,
		Args: cobra.ExactArgs(0),

		RunE: func(_ *cobra.Command, _ []string) error {
			req := &kmsg.AlterClientQuotasRequest{
				Entries:      []kmsg.AlterClientQuotasRequestEntry{{}},
				ValidateOnly: dryRun,
			}

			if len(names) == 0 && len(defaults) == 0 {
				return out.Errf(out.ExitUsage, "at least one name or default must be specified")
			}
			if len(adds) == 0 && len(deletes) == 0 {
				return out.Errf(out.ExitUsage, "at least one add or delete must be specified")
			}

			ent := &req.Entries[0]

			validType := map[string]bool{
				"user":      true,
				"client-id": true,
				"ip":        true,
			}

			for _, name := range names {
				split := strings.SplitN(name, "=", 2)
				if len(split) != 2 {
					return out.Errf(out.ExitUsage, "name %q missing value", split[0])
				}
				k, v := split[0], split[1]
				k = strings.ToLower(k)
				if !validType[k] {
					return out.Errf(out.ExitUsage, "name type %q is invalid (allowed: user, client-id, ip)", split[0])
				}
				ent.Entity = append(ent.Entity, kmsg.AlterClientQuotasRequestEntryEntity{
					Type: k,
					Name: &v,
				})
			}

			for _, def := range defaults {
				if !validType[def] {
					return out.Errf(out.ExitUsage, "default type %q is invalid (allowed: user, client-id, ip)", def)
				}
				ent.Entity = append(ent.Entity, kmsg.AlterClientQuotasRequestEntryEntity{
					Type: def,
				})
			}

			for _, add := range adds {
				split := strings.SplitN(add, "=", 2)
				if len(split) != 2 {
					return out.Errf(out.ExitUsage, "add %q missing value", split[0])
				}
				k, v := split[0], split[1]
				f, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return out.Errf(out.ExitUsage, "unable to parse add %q: %v", k, err)
				}
				ent.Ops = append(ent.Ops, kmsg.AlterClientQuotasRequestEntryOp{
					Key:   k,
					Value: f,
				})
			}

			for _, del := range deletes {
				ent.Ops = append(ent.Ops, kmsg.AlterClientQuotasRequestEntryOp{
					Key:    del,
					Remove: true,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to alter client quotas: %v", err)
			}
			resp := kresp.(*kmsg.AlterClientQuotasResponse)

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results", alterHeaders...).ResultColumns()
			table.SetDryRun(dryRun)
			for _, entry := range resp.Entries {
				var parts []entityPart
				for _, e := range entry.Entity {
					parts = append(parts, entityPart{e.Type, e.Name})
				}
				var errName, msg string
				if entry.ErrorCode != 0 {
					errName = kerr.TypedErrorForCode(entry.ErrorCode).Message
					if entry.ErrorMessage != nil {
						msg = *entry.ErrorMessage
					}
				}
				table.Row(entityString(parts), errName, msg)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, alterHeaders...)

	cmd.Flags().StringArrayVar(&names, "name", nil, "type=name pair for exact name matching, where type is user, client-id, or ip; repeatable")
	cmd.Flags().StringArrayVar(&defaults, "default", nil, "type for default matching, where type is user, client-id, or ip; repeatable")
	cmd.Flags().StringArrayVar(&adds, "add", nil, "key=value quota to add, where the value is a float64; repeatable")
	cmd.Flags().StringArrayVar(&deletes, "delete", nil, "key quota to delete; repeatable")
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "validate the request without applying changes")

	return cmd
}
