// Package admin contains admin commands.
package admin

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/commands/admin/acl"
	"github.com/twmb/kcl/commands/admin/configs"
	"github.com/twmb/kcl/commands/admin/dtoken"
	"github.com/twmb/kcl/commands/admin/group"
	"github.com/twmb/kcl/commands/admin/topic"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "admin",
		Aliases: []string{"adm", "a"},
		Short:   "Admin utility commands.",
	}

	cmd.AddCommand(alterCommand(cl))
	cmd.AddCommand(describeCommand(cl))

	cmd.AddCommand(topic.Command(cl))
	cmd.AddCommand(acl.Command(cl))
	cmd.AddCommand(dtoken.Command(cl))
	cmd.AddCommand(group.Command(cl))

	cmd.AddCommand(electLeaderCommand(cl))
	cmd.AddCommand(deleteRecordsCommand(cl))

	return cmd
}

func alterCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "alter",
		Short: "Altering admin commands (logdirs, partition assignments, quotas, scram).",
	}

	cmd.AddCommand(configs.AlterCommand(cl))
	cmd.AddCommand(logdirsAlterReplicasCommand(cl))
	cmd.AddCommand(alterPartitionAssignments(cl))
	cmd.AddCommand(alterClientQuotas(cl))

	return cmd
}

func describeCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "describe",
		Aliases: []string{"d"},
		Short:   "Describing admin commands (logdirs, partition assignments, quotas, scram).",
	}

	cmd.AddCommand(configs.DescribeCommand(cl))
	cmd.AddCommand(logdirsDescribeCommand(cl))
	cmd.AddCommand(listPartitionReassignments(cl))
	cmd.AddCommand(describeClientQuotas(cl))

	return cmd
}

func electLeaderCommand(cl *client.Client) *cobra.Command {
	var allPartitions bool
	var unclean bool
	var run bool

	cmd := &cobra.Command{
		Use:   "elect-leaders",
		Short: "Trigger leader elections for partitions",
		Long: `Trigger leader elections for topic partitions (Kafka 2.2.0+).

This command allows for triggering leader elections on any topic and any
partition, as well as on all topic partitions. To run on all, you must not
pass any topic flags, and you must use the --all-partitions flag.

The format for triggering topic partitions is "foo:1,2,3", where foo is a
topic and 1,2,3 are partition numbers.

To avoid accidental triggers, this command requires a --run flag to run.
`,
		Example: "elect-leaders --run foo:1,2,3 bar:9",
		Run: func(_ *cobra.Command, topicParts []string) {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			out.MaybeDie(err, "unable to parse topic partitions: %v", err)
			if !run {
				out.Die("use --run to actually run this command")
			}

			req := &kmsg.ElectLeadersRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			if unclean {
				req.ElectionType = 1
			}

			topics := []kmsg.ElectLeadersRequestTopic{}
			if allPartitions {
				topics = nil
			} else if len(topics) == 0 {
				out.Die("no topics requested for leader election, and not triggering all; nothing to do")
			}

			for topic, partitions := range tps {
				req.Topics = append(req.Topics, kmsg.ElectLeadersRequestTopic{
					Topic:      topic,
					Partitions: partitions,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to elect leaders: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			resp := kresp.(*kmsg.ElectLeadersResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.Die("%v", err)
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()

			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					errKind := ""
					var msg string
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						errKind = err.Error()
					}
					if partition.ErrorMessage != nil {
						msg = *partition.ErrorMessage
					}
					fmt.Fprintf(tw, "%s\t%d\t%v\t%s\n",
						topic.Topic,
						partition.Partition,
						errKind,
						msg,
					)
				}
			}
		},
	}

	cmd.Flags().BoolVar(&allPartitions, "all-partitions", false, "trigger leader election on all topics for all partitions")
	cmd.Flags().BoolVar(&unclean, "unclean", false, "allow unclean leader election (Kafka 2.4.0+)")
	cmd.Flags().BoolVar(&run, "run", false, "actually run the command (avoids accidental elections without this flag)")

	return cmd
}

func alterPartitionAssignments(cl *client.Client) *cobra.Command {
	var topicPartReplicas []string

	cmd := &cobra.Command{
		Use:   "partition-assignments",
		Short: "Alter partition assignments.",
		Long: `Alter which brokers partitions are assigned to (Kafka 2.4.0+).

The syntax for each topic is

  topic: 1->2,3,4 ; 2->1,2,3

where the first number is the partition, and -> points to the replicas you
want to move the partition to. Note that since this contains a >, you likely
need to quote your input to the flag.

If a replica list is empty for a specific partition, this cancels any active
reassignment for that partition.
`,
		Example: "partition-assignments -t 'foo:1->1,2,3' -t 'bar:2->3,4,5;5->3,4,5'",
		Run: func(_ *cobra.Command, args []string) {
			tprs, err := flagutil.ParseTopicPartitionReplicas(topicPartReplicas)
			out.MaybeDie(err, "unable to parse topic partitions replicas: %v", err)

			req := &kmsg.AlterPartitionAssignmentsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			for topic, partitions := range tprs {
				t := kmsg.AlterPartitionAssignmentsRequestTopic{
					Topic: topic,
				}
				for partition, replicas := range partitions {
					t.Partitions = append(t.Partitions, kmsg.AlterPartitionAssignmentsRequestTopicPartition{
						Partition: partition,
						Replicas:  replicas,
					})
				}
				req.Topics = append(req.Topics, t)
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to alter partition assignments: %v", err)
			resp := kresp.(*kmsg.AlterPartitionAssignmentsResponse)
			if cl.AsJSON() {
				out.ExitJSON(resp)
			}

			if resp.ErrorCode != 0 {
				out.ErrAndMsg(resp.ErrorCode, resp.ErrorMessage)
				out.Exit()
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					msg := "OK"
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						msg = err.Error()
					}
					detail := ""
					if partition.ErrorMessage != nil {
						detail = *partition.ErrorMessage
					}
					fmt.Fprintf(tw, "%s\t%d\t%s\t%s\n", topic.Topic, partition.Partition, msg, detail)
				}
			}
		},
	}

	cmd.Flags().StringArrayVarP(&topicPartReplicas, "topic", "t", nil, "topic, partitions, and replica destinations; repeatable")
	return cmd
}

func listPartitionReassignments(cl *client.Client) *cobra.Command {
	var topicParts []string

	cmd := &cobra.Command{
		Use:   "partition-reassignments",
		Short: "List partition reassignments.",
		Long: `List which partitions are currently being reassigned (Kafka 2.4.0+).

The syntax for each topic is

  topic:1,2,3

where the numbers correspond to partitions for a topic.

If no topics are specified, this lists all active reassignments.
`,
		Run: func(_ *cobra.Command, args []string) {
			tps, err := flagutil.ParseTopicPartitions(topicParts)
			out.MaybeDie(err, "unable to parse topic partitions: %v", err)

			req := &kmsg.ListPartitionReassignmentsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			for topic, partitions := range tps {
				req.Topics = append(req.Topics, kmsg.ListPartitionReassignmentsRequestTopic{
					Topic:      topic,
					Partitions: partitions,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to list partition reassignments: %v", err)
			resp := kresp.(*kmsg.ListPartitionReassignmentsResponse)
			if cl.AsJSON() {
				out.ExitJSON(resp)
			}

			if resp.ErrorCode != 0 {
				out.ErrAndMsg(resp.ErrorCode, resp.ErrorMessage)
				out.Exit()
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()
			fmt.Fprint(tw, "TOPIC\tPARTITION\tCURRENT REPLICAS\tADDING\tREMOVING\n")
			for _, topic := range resp.Topics {
				for _, p := range topic.Partitions {
					sort.Slice(p.Replicas, func(i, j int) bool { return p.Replicas[i] < p.Replicas[j] })
					sort.Slice(p.AddingReplicas, func(i, j int) bool { return p.AddingReplicas[i] < p.AddingReplicas[j] })
					sort.Slice(p.RemovingReplicas, func(i, j int) bool { return p.RemovingReplicas[i] < p.RemovingReplicas[j] })
					fmt.Fprintf(tw, "%s\t%d\t%v\t%v\t%v\n", topic.Topic, p.Partition, p.Replicas, p.AddingReplicas, p.RemovingReplicas)
				}
			}
		},
	}

	cmd.Flags().StringArrayVarP(&topicParts, "topic", "t", nil, "topic and partitions to list partition reassignments for; repeatable")

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
		Use:   "client-quotas",
		Short: "Describe client quotas.",
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
		Use:   "client-quotas",
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
