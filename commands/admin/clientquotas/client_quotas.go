package clientquotas

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"
	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "client-quotas",
		Short: "Alter, describe, or resolve client quotas.",
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
		Long: `Describe client quotas (Kafka 2.6.0+)

As mentioned in KIP-546, "by default, quotas are defined in terms of a user and
client ID, where the user acts as an opaque principal name, and the client ID
as a generic group identifier". The values for the user and the client-id can
be named, defaulted, or omitted entirely.

Describe client quotas takes an input list of named entities, default entities,
or omitted (any) entities and returns a all matched quotas and their
keys and values.

Named entities are in the format key=value, where key is either user or
client-id and value is the name to be matched.

Default entities and omitted (any) entities are in the format key, where key is
the user or client-id.

This command is a filtering type of command, where anything that passes the
filter specified by flags is returned.
`,
		Args: cobra.ExactArgs(0),

		Run: func(_ *cobra.Command, _ []string) {
			req := &kmsg.DescribeClientQuotasRequest{
				Strict: strict,
			}

			validType := map[string]bool{
				"user":      true,
				"client-id": true,
			}

			for _, name := range names {
				split := strings.SplitN(name, "=", 2)
				if len(split) != 2 {
					out.Die("name %q missing value", split[0])
				}
				k, v := split[0], split[1]
				k = strings.ToLower(k)
				if !validType[k] {
					out.Die("name type %q is invalid (allowed: user, client-id)", split[0])
				}
				req.Components = append(req.Components, kmsg.DescribeClientQuotasRequestComponent{
					EntityType: k,
					MatchType:  0,
					Match:      &v,
				})
			}

			for _, def := range defaults {
				if !validType[def] {
					out.Die("default type %q is invalid (allowed: user, client-id)", def)
				}
				req.Components = append(req.Components, kmsg.DescribeClientQuotasRequestComponent{
					EntityType: def,
					MatchType:  1,
				})
			}

			for _, a := range any {
				if !validType[a] {
					out.Die("any type %q is invalid (allowed: user, client-id)", a)
				}
				req.Components = append(req.Components, kmsg.DescribeClientQuotasRequestComponent{
					EntityType: a,
					MatchType:  2,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to describe client quotas: %v", err)
			resp := kresp.(*kmsg.DescribeClientQuotasResponse)
			if cl.AsJSON() {
				out.ExitJSON(resp)
			}

			if resp.ErrorCode != 0 {
				out.ErrAndMsg(resp.ErrorCode, resp.ErrorMessage)
				out.Exit()
			}

			for _, entry := range resp.Entries {
				fmt.Print("{")
				for i, entity := range entry.Entity {
					if i > 0 {
						fmt.Print(", ")
					}
					fmt.Print(entity.Type)
					fmt.Print("=")
					name := "<default>"
					if entity.Name != nil {
						name = *entity.Name
					}
					fmt.Print(name)
				}
				fmt.Println("}")

				for _, value := range entry.Values {
					fmt.Printf("%s=%v", value.Key, value.Value)
				}
				fmt.Println()
			}
		},
	}

	cmd.Flags().StringArrayVar(&names, "name", nil, "type=name pair for exact name matching, where type is user or client-id; repeatable")
	cmd.Flags().StringArrayVar(&defaults, "default", nil, "type for default matching, where type is user or client-id; repeatable")
	cmd.Flags().StringArrayVar(&any, "any", nil, "type for any matching (names or default), where type is user or client-id; repeatable")
	cmd.Flags().BoolVar(&strict, "strict", false, "whether matches are strict, if true, entities with unspecified entity types are excluded")

	return cmd
}

func alterClientQuotas(cl *client.Client) *cobra.Command {
	var (
		names    []string
		defaults []string
		run      bool
		adds     []string
		deletes  []string
	)

	cmd := &cobra.Command{
		Use:   "alter",
		Short: "Alter client quotas.",
		Long: `Alter client quotas (Kafka 2.6.0+)

This command alters client quotas; to see a bit more of a description on
quotas, see the help text for client-quotas or read KIP-546.

Similar to describing, this command matches. Where describing filters for only
matches, this runs an alter on anything that matches.

`,
		Args: cobra.ExactArgs(0),

		Run: func(_ *cobra.Command, _ []string) {
			req := &kmsg.AlterClientQuotasRequest{
				Entries:      []kmsg.AlterClientQuotasRequestEntry{{}},
				ValidateOnly: !run,
			}

			if len(names) == 0 && len(defaults) == 0 {
				out.Die("at least one name or default must be specified")
			}
			if len(adds) == 0 && len(deletes) == 0 {
				out.Die("at least one add or delete must be specified")
			}

			ent := &req.Entries[0]

			validType := map[string]bool{
				"user":      true,
				"client-id": true,
			}

			for _, name := range names {
				split := strings.SplitN(name, "=", 2)
				if len(split) != 2 {
					out.Die("name %q missing value", split[0])
				}
				k, v := split[0], split[1]
				k = strings.ToLower(k)
				if !validType[k] {
					out.Die("name type %q is invalid (allowed: user, client-id)", split[0])
				}
				ent.Entity = append(ent.Entity, kmsg.AlterClientQuotasRequestEntryEntity{
					Type: k,
					Name: &v,
				})
			}

			for _, def := range defaults {
				if !validType[def] {
					out.Die("default type %q is invalid (allowed: user, client-id)", def)
				}
				ent.Entity = append(ent.Entity, kmsg.AlterClientQuotasRequestEntryEntity{
					Type: def,
				})
			}

			for _, add := range adds {
				split := strings.SplitN(add, "=", 2)
				if len(split) != 2 {
					out.Die("add %q missing value", split[0])
				}
				k, v := split[0], split[1]
				f, err := strconv.ParseFloat(v, 64)
				out.MaybeDie(err, "unable to parse add %q: %v", k, err)
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
			out.MaybeDie(err, "unable to alter client quotas: %v", err)
			resp := kresp.(*kmsg.AlterClientQuotasResponse)
			if cl.AsJSON() {
				out.ExitJSON(resp)
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()

			for _, entry := range resp.Entries {
				fmt.Fprint(tw, "{")
				for i, entity := range entry.Entity {
					if i > 0 {
						fmt.Fprint(tw, ", ")
					}
					fmt.Fprint(tw, entity.Type)
					fmt.Fprint(tw, "=")
					name := "<default>"
					if entity.Name != nil {
						name = *entity.Name
					}
					fmt.Fprint(tw, name)
				}
				fmt.Fprint(tw, "}\t")

				code := "OK"
				if err := kerr.ErrorForCode(entry.ErrorCode); err != nil {
					code = err.Error()
				}
				fmt.Fprintf(tw, "%s\t", code)

				msg := ""
				if entry.ErrorMessage != nil {
					msg = *entry.ErrorMessage
				}
				fmt.Fprintf(tw, "%s\n", msg)
			}
		},
	}

	cmd.Flags().StringArrayVar(&names, "name", nil, "type=name pair for exact name matching, where type is user or client-id; repeatable")
	cmd.Flags().StringArrayVar(&defaults, "default", nil, "type for default matching, where type is user or client-id; repeatable")
	cmd.Flags().StringArrayVar(&adds, "add", nil, "key=value quota to add, where the value is a float64; repeatable")
	cmd.Flags().StringArrayVar(&deletes, "delete", nil, "key quota to delete; repeatable")
	cmd.Flags().BoolVar(&run, "run", false, "whether to actually run the alter vs. the default to validate only")

	return cmd
}
