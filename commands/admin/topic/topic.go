// Package topic contains topic related utilities and subcommands.
package topic

import (
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/kv"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "topic",
		Aliases: []string{"t"},
		Short:   "Topic operations (list, describe, create, delete, trim-prefix).",
	}

	cmd.AddCommand(topicCreateCommand(cl))
	cmd.AddCommand(topicListCommand(cl))
	cmd.AddCommand(topicDeleteCommand(cl))
	cmd.AddCommand(topicAddPartitionsCommand(cl))
	cmd.AddCommand(topicDescribeCommand(cl))
	cmd.AddCommand(topicTrimPrefixCommand(cl))
	cmd.AddCommand(ListOffsetsCommand(cl))
	return cmd
}

func parseReplicaAssignment(s string) ([]kmsg.CreateTopicsRequestTopicReplicaAssignment, error) {
	var assignments []kmsg.CreateTopicsRequestTopicReplicaAssignment
	for i, group := range strings.Split(s, ",") {
		group = strings.TrimSpace(group)
		if len(group) == 0 {
			continue
		}
		var replicas []int32
		for _, idStr := range strings.Split(group, ":") {
			idStr = strings.TrimSpace(idStr)
			if len(idStr) == 0 {
				continue
			}
			id, err := strconv.Atoi(idStr)
			if err != nil {
				return nil, fmt.Errorf("unable to parse broker ID %q: %v", idStr, err)
			}
			replicas = append(replicas, int32(id))
		}
		if len(replicas) == 0 {
			continue
		}
		assignments = append(assignments, kmsg.CreateTopicsRequestTopicReplicaAssignment{
			Partition: int32(i),
			Replicas:  replicas,
		})
	}
	return assignments, nil
}

func topicCreateCommand(cl *client.Client) *cobra.Command {
	var (
		numPartitions     int32
		replicationFactor int16
		configKVs         []string
		validateOnly      bool
		replicaAssignment string
	)

	cmd := &cobra.Command{
		Use:     "create TOPICS...",
		Aliases: []string{"c"},
		Short:   "Create topics.",
		Long: `Create topics.

Requires Kafka 0.10.1+.

All topics created with this command will have the same number of partitions,
replication factor, and configs.

To manually assign replicas, use --replica-assignment with a comma-separated
list of colon-separated broker IDs. Each comma-separated group is a partition's
replica list. For example, "0:1:2,1:2:3,2:3:0" creates 3 partitions with 3
replicas each. When using --replica-assignment, do not use --num-partitions or
--replication-factor.

Each result row is TOPIC TOPIC-ID ERROR MESSAGE. The id is unknown below
CreateTopics v7 (Kafka 2.8) and on a dry run, which validates the request
without creating anything.

EXAMPLES:
  kcl topic create foo                          # cluster default partitions and replication
  kcl topic create foo -p 6 -r 3                # six partitions, three replicas each
  kcl topic create foo -c cleanup.policy=compact -c retention.ms=-1
  kcl topic create foo --dry-run                # validate only

SEE ALSO:
  kcl topic list            list topics
  kcl topic describe        describe topic partitions
  kcl topic add-partitions  add partitions to a topic
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			cl.SetCommand("topic.create")
			kvs, err := kv.Parse(configKVs)
			if err != nil {
				return out.Errf(out.ExitUsage, "unable to parse --config: %v", err)
			}
			req := kmsg.CreateTopicsRequest{TimeoutMillis: cl.TimeoutMillis()}
			req.ValidateOnly = validateOnly
			var configs []kmsg.CreateTopicsRequestTopicConfig
			for _, kv := range kvs {
				configs = append(configs, kmsg.CreateTopicsRequestTopicConfig{
					Name:  kv.K,
					Value: kmsg.StringPtr(kv.V),
				})
			}

			var assignments []kmsg.CreateTopicsRequestTopicReplicaAssignment
			if replicaAssignment != "" {
				assignments, err = parseReplicaAssignment(replicaAssignment)
				if err != nil {
					return out.Errf(out.ExitUsage, "unable to parse replica assignment: %v", err)
				}
				if len(assignments) == 0 {
					return out.Errf(out.ExitUsage, "--replica-assignment specified but no partitions parsed")
				}
				numPartitions = -1
				replicationFactor = -1
			}

			for _, topic := range args {
				req.Topics = append(req.Topics, kmsg.CreateTopicsRequestTopic{
					Topic:             topic,
					ReplicationFactor: replicationFactor,
					NumPartitions:     numPartitions,
					ReplicaAssignment: assignments,
					Configs:           configs,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), &req)
			if err != nil {
				return fmt.Errorf("unable to create topic %q: %v", args[0], err)
			}

			resp := kresp.(*kmsg.CreateTopicsResponse)
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "topics",
				"TOPIC", "TOPIC-ID", "ERROR", "MESSAGE").ResultColumns()
			table.SetDryRun(validateOnly)
			for _, topic := range resp.Topics {
				errStr, msg := errorCells(topic.ErrorCode, topic.ErrorMessage)
				table.Row(topic.Topic, topicIDCell(topic.TopicID), errStr, msg)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, "TOPIC", "TOPIC-ID", "ERROR", "MESSAGE")

	cmd.Flags().BoolVarP(&validateOnly, "dry-run", "d", false, "validate the topic creation request; do not create topics (Kafka 0.10.2+)")
	cmd.Flags().Int32VarP(&numPartitions, "num-partitions", "p", -1, "number of partitions to create (-1 uses the cluster default: num.partitions)")
	cmd.Flags().Int16VarP(&replicationFactor, "replication-factor", "r", -1, "replicas per partition (-1 uses the cluster default: default.replication.factor)")
	cmd.Flags().StringArrayVarP(&configKVs, "config", "c", nil, "topic config as key=value (repeatable, e.g. -c cleanup.policy=compact -c preallocate=true)")
	cmd.Flags().StringArrayVarP(&configKVs, "kv", "k", nil, "old name for --config")
	cmd.Flags().MarkHidden("kv")
	cmd.Flags().StringVar(&replicaAssignment, "replica-assignment", "", "manual replica assignment as comma-separated partition groups of colon-separated broker IDs (e.g. 0:1:2,1:2:3,2:3:0)")

	return cmd
}

// ListHeaders are the columns of a topic list row, which "kcl cluster
// metadata" prints as its topics section too, and ListKeys their JSON keys.
var (
	ListHeaders = []string{"TOPIC", "TOPIC-ID", "PARTITIONS", "REPLICATION", "INTERNAL", "ERROR"}
	ListKeys    = []string{"topic", "topic_id", "partition_count", "replication_factor", "internal", "error"}
)

// ListTable is the table of ListRows, with its JSON keys.
func ListTable(format, command string) *out.FormattedTable {
	return out.NewFormattedTable(format, command, 1, "topics", ListHeaders...).
		WithKeys(map[string]string{"PARTITIONS": "partition_count", "REPLICATION": "replication_factor"})
}

// ListRows is one row per topic in the metadata response, in name order, and
// whether any topic carried an error. A topic the broker answered with an
// error keeps its row, with what we do not know unknown and the error in
// ERROR. The id is unknown below Metadata v10 (Kafka 2.8). Internal topics
// are skipped unless internal is set.
func ListRows(version int16, topics []kmsg.MetadataResponseTopic, internal bool) (rows [][]any, failed bool) {
	SortTopics(topics)
	for _, t := range topics {
		if t.IsInternal && !internal {
			continue
		}
		var name any = out.Unknown
		if t.Topic != nil {
			name = *t.Topic
		}
		var id any = out.Unknown
		if version >= 10 {
			id = topicIDCell(t.TopicID)
		}
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			failed = true
			rows = append(rows, []any{name, id, out.Unknown, out.Unknown, out.Unknown, err.Error()})
			continue
		}
		replication := 0
		if len(t.Partitions) > 0 {
			replication = len(t.Partitions[0].Replicas)
		}
		rows = append(rows, []any{name, id, len(t.Partitions), replication, t.IsInternal, ""})
	}
	return rows, failed
}

// ListRowMaps is ListRows as JSON objects, for a document that carries the
// topics under a key of its own.
func ListRowMaps(rows [][]any) []map[string]any {
	return rowMaps(ListKeys, rows)
}

func topicListCommand(cl *client.Client) *cobra.Command {
	var (
		detailed     bool
		showInternal bool
		useRegex     bool
	)

	cmd := &cobra.Command{
		Use:     "list [TOPICS...]",
		Aliases: []string{"ls"},
		Short:   "List topics.",
		Long: `List topics.

With no argument, every topic is listed, internal topics only with -i. With
arguments, the named topics are listed, and one that does not exist prints
its row with the error and the command exits 1. With -r, the arguments are
regular expressions instead, and every topic matching any of them is listed.

Each row is TOPIC TOPIC-ID PARTITIONS REPLICATION INTERNAL ERROR. The id is
unknown below Metadata v10 (Kafka 2.8).

EXAMPLES:
  kcl topic list                    # all non-internal topics
  kcl topic list -i                 # include internal topics
  kcl topic list foo bar            # two topics by name
  kcl topic list -r 'logs\.'        # topics matching a regex

SEE ALSO:
  kcl topic describe     describe topic partitions
  kcl cluster metadata   brokers and topics from the Metadata request
`,
		RunE: func(_ *cobra.Command, args []string) error {
			cl.SetCommand("topic.list")
			var patterns []*regexp.Regexp
			if useRegex {
				for _, pat := range args {
					re, err := regexp.Compile(pat)
					if err != nil {
						return out.Errf(out.ExitUsage, "invalid regex %q: %v", pat, err)
					}
					patterns = append(patterns, re)
				}
			}

			req := kmsg.NewPtrMetadataRequest()
			if !useRegex {
				for _, t := range args {
					rt := kmsg.NewMetadataRequestTopic()
					rt.Topic = kmsg.StringPtr(t)
					req.Topics = append(req.Topics, rt)
				}
			}
			resp, err := req.RequestWith(context.Background(), cl.Client())
			if err != nil {
				return fmt.Errorf("unable to list topics: %v", err)
			}

			// A topic you named is listed even if it is internal; the
			// -i filter is for the unqualified listing.
			internal := showInternal || len(args) > 0 && !useRegex
			var topics []kmsg.MetadataResponseTopic
			for _, t := range resp.Topics {
				if len(patterns) > 0 {
					name := ""
					if t.Topic != nil {
						name = *t.Topic
					}
					if !matchesAny(patterns, name) {
						continue
					}
				}
				topics = append(topics, t)
			}

			if detailed {
				return Describe(cl, DescribeOpts{}, topicNames(topics))
			}
			table := ListTable(cl.Format(), cl.Command())
			rows, failed := ListRows(resp.Version, topics, internal)
			for _, row := range rows {
				table.Row(row...)
			}
			if err := table.Flush(); err != nil {
				return err
			}
			if failed {
				return out.ErrSilent
			}
			return nil
		},
	}
	out.ColumnsFunc(cmd, func() []string {
		if detailed {
			return describeHeaders("partitions")
		}
		return ListHeaders
	})
	cmd.Flags().BoolVar(&detailed, "detailed", false, "describe the listed topics, as kcl topic describe does")
	cmd.Flags().MarkHidden("detailed")
	cmd.Flags().BoolVarP(&showInternal, "internal", "i", false, "include internal topics")
	cmd.Flags().BoolVarP(&useRegex, "regex", "r", false, "treat the arguments as regular expressions to match topic names against")
	return cmd
}

func matchesAny(patterns []*regexp.Regexp, s string) bool {
	for _, re := range patterns {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

// topicNames are the names in a metadata response, a topic we know by id
// alone skipped.
func topicNames(topics []kmsg.MetadataResponseTopic) []string {
	names := make([]string, 0, len(topics))
	for _, t := range topics {
		if t.Topic != nil {
			names = append(names, *t.Topic)
		}
	}
	return names
}

func topicDeleteCommand(cl *client.Client) *cobra.Command {
	var ids bool
	var dryRun bool
	var useRegex bool
	cmd := &cobra.Command{
		Use:   "delete TOPICS...",
		Short: "Delete all listed topics (Kafka 0.10.1+).",
		Long: `Delete all listed topics (Kafka 0.10.1+).

Use --regex to treat arguments as regex patterns: all topics matching any
pattern will be deleted. A pattern is matched against every topic in the
cluster, so an unanchored one deletes more than it looks like it will; run
the same command with --dry-run first to see what it matches.

Each result row is TOPIC ERROR MESSAGE. A dry run prints the rows a real run
would, marked as a dry run, and deletes nothing.

EXAMPLES:
  kcl topic delete foo bar               # delete two topics by name
  kcl topic delete --regex '^tmp-'       # delete every topic starting with tmp-
  kcl topic delete --regex . --dry-run   # print every topic the pattern matches

SEE ALSO:
  kcl topic list         list topics
  kcl topic describe     describe topic partitions
  kcl topic trim-prefix  delete records without deleting the topic
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, topics []string) error {
			cl.SetCommand("topic.delete")
			if useRegex && ids {
				return out.Errf(out.ExitUsage, "--regex and --ids cannot be used together")
			}
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "topics",
				"TOPIC", "ERROR", "MESSAGE").ResultColumns()
			table.SetDryRun(dryRun)

			if !useRegex && !ids {
				var err error
				if topics, err = flagutil.ResolveTopics(context.Background(), cl.Client(), topics); err != nil {
					return err
				}
			}
			if useRegex {
				var patterns []*regexp.Regexp
				for _, pat := range topics {
					re, err := regexp.Compile(pat)
					if err != nil {
						return out.Errf(out.ExitUsage, "invalid regex %q: %v", pat, err)
					}
					patterns = append(patterns, re)
				}

				metaReq := kmsg.NewPtrMetadataRequest()
				metaResp, err := metaReq.RequestWith(context.Background(), cl.Client())
				if err != nil {
					return fmt.Errorf("unable to list topics: %v", err)
				}
				SortTopics(metaResp.Topics)

				topics = nil
				for _, t := range metaResp.Topics {
					if t.Topic != nil && matchesAny(patterns, *t.Topic) {
						topics = append(topics, *t.Topic)
					}
				}
				if len(topics) == 0 {
					fmt.Fprintln(os.Stderr, "No topics matched the provided regex patterns.")
					return table.Flush()
				}
			}

			if dryRun {
				for _, topic := range topics {
					table.Row(topic, "", "")
				}
				return table.Flush()
			}

			req := &kmsg.DeleteTopicsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
				TopicNames:    topics,
			}
			for _, topic := range topics {
				t := kmsg.NewDeleteTopicsRequestTopic()
				if ids {
					id, err := flagutil.ParseTopicID(topic)
					if err != nil {
						return out.Errf(out.ExitUsage, "invalid topic id %q: %v", topic, err)
					}
					t.TopicID = id
				} else {
					t.Topic = kmsg.StringPtr(topic)
				}
				req.Topics = append(req.Topics, t)
			}

			resp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to delete topics: %v", err)
			}

			for _, topicResp := range resp.(*kmsg.DeleteTopicsResponse).Topics {
				errStr, msg := errorCells(topicResp.ErrorCode, topicResp.ErrorMessage)
				var topic any = topicIDCell(topicResp.TopicID)
				if topicResp.Topic != nil {
					topic = *topicResp.Topic
				}
				table.Row(topic, errStr, msg)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, "TOPIC", "ERROR", "MESSAGE")
	cmd.Flags().BoolVar(&ids, "ids", false, "whether the input topics should be parsed as topic IDs")
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "print topics that would be deleted without actually deleting them")
	cmd.Flags().BoolVarP(&useRegex, "regex", "r", false, "treat topic arguments as regex patterns; match against all existing topics")
	return cmd
}

func topicAddPartitionsCommand(cl *client.Client) *cobra.Command {
	var topics []string
	var force bool
	var num, total int
	var assigns []string

	cmd := &cobra.Command{
		Use:   "add-partitions TOPIC",
		Short: "Add partitions to a topic.",
		Long: `Add partitions to a topic.

Requires Kafka 1.0.0+.

-n adds that many partitions and lets the broker place their replicas.
--total brings the topic to that many partitions: a topic already past it is
an error, one already there is nothing to do, otherwise the difference is
added. -a places the new partitions yourself, one -a per partition listing
the brokers its replicas go on, comma separated, leader first; -a '1,2:3,1'
in one value is the same as -a 1,2 -a 3,1. Each new partition must have as
many replicas as the existing ones.

Each result row is TOPIC ERROR MESSAGE.

EXAMPLES:
  kcl topic add-partitions foo -n 3                  # three more, broker places replicas
  kcl topic add-partitions foo --total 12            # up to twelve; nothing to do if already there
  kcl topic add-partitions foo -a 1,2 -a 3,1 -a 2,3  # three more, on brokers 1+2, 3+1, 2+3

SEE ALSO:
  kcl topic describe     partition leaders and replicas
  kcl topic create       create topics
`,

		Args: cobra.ArbitraryArgs,
		RunE: func(_ *cobra.Command, args []string) error {
			cl.SetCommand("topic.add-partitions")
			// With -t this is the old form: the positionals are the
			// assignments, "1,2 : 3,1". Without it the one positional is
			// the topic and -n or -a says what to add.
			var assignments []kmsg.CreatePartitionsRequestTopicAssignment
			var err error
			if len(topics) > 0 {
				if assignments, err = parseAssignments(strings.Join(args, "")); err != nil {
					return out.Errf(out.ExitUsage, "parse assignments failure: %v", err)
				}
				if len(assignments) == 0 {
					return out.Errf(out.ExitUsage, "no new partitions requested")
				}
			} else {
				if len(args) != 1 {
					return out.Errf(out.ExitUsage, "add-partitions takes one topic, then -n COUNT or -a BROKERS once per new partition")
				}
				var err error
				if topics, err = flagutil.ResolveTopics(context.Background(), cl.Client(), args); err != nil {
					return err
				}
				switch {
				case num > 0 && total > 0:
					return out.Errf(out.ExitUsage, "-n and --total are exclusive")
				case num > 0 && len(assigns) > 0:
					return out.Errf(out.ExitUsage, "-n and -a are exclusive: -n lets the broker place replicas, -a places them")
				case len(assigns) > 0:
					if assignments, err = parseAssignments(strings.Join(assigns, ":")); err != nil {
						return out.Errf(out.ExitUsage, "invalid -a: %v", err)
					}
				case num == 0 && total == 0:
					return out.Errf(out.ExitUsage, "nothing to add: pass -n COUNT, --total COUNT, or -a BROKERS once per new partition")
				}
			}

			for _, topic := range topics {
				if strings.HasPrefix(topic, "__") && !force {
					return out.Errf(out.ExitUsage, "topic %q is an internal topic (starts with \"__\"); use --force to modify internal topics", topic)
				}
				if strings.HasPrefix(topic, "__") && force {
					fmt.Fprintf(os.Stderr, "WARNING: modifying internal topic %q; this can cause system instability\n", topic)
				}
			}

			// Get the metadata so we can determine the final partition count.
			metaReq := new(kmsg.MetadataRequest)
			for _, topic := range topics {
				t := topic
				metaReq.Topics = append(metaReq.Topics, kmsg.MetadataRequestTopic{Topic: &t})
			}
			kmetaResp, err := cl.Client().Request(context.Background(), metaReq)
			if err != nil {
				return fmt.Errorf("unable to get topic metadata: %v", err)
			}
			metaResp := kmetaResp.(*kmsg.MetadataResponse)

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "topics",
				"TOPIC", "ERROR", "MESSAGE").ResultColumns()
			createReq := kmsg.CreatePartitionsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			for _, topic := range metaResp.Topics {
				if topic.Topic == nil {
					return fmt.Errorf("metadata returned nil topic, unknown topic ID!")
				}
				if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
					table.Row(*topic.Topic, err.Error(), "")
					continue
				}
				currentPartitionCount := len(topic.Partitions)
				adding := num
				if len(assignments) > 0 {
					adding = len(assignments)
				}
				if total > 0 {
					switch {
					case currentPartitionCount > total:
						return out.Errf(out.ExitError, "topic %s has %d partitions, more than --total %d", *topic.Topic, currentPartitionCount, total)
					case currentPartitionCount == total:
						table.Row(*topic.Topic, "", fmt.Sprintf("already has %d partitions", total))
						continue
					}
					if want := total - currentPartitionCount; len(assignments) > 0 && len(assignments) != want {
						return out.Errf(out.ExitUsage, "--total %d needs %d new partitions but -a lists %d", total, want, len(assignments))
					} else {
						adding = want
					}
				}
				if currentPartitionCount > 0 && len(assignments) > 0 {
					currentReplicaCount := len(topic.Partitions[0].Replicas)
					if currentReplicaCount != len(assignments[0].Replicas) {
						return out.Errf(out.ExitUsage, "topic %s has %d replicas per partition; each -a must list %d brokers", *topic.Topic, currentReplicaCount, currentReplicaCount)
					}
				}

				createReq.Topics = append(createReq.Topics, kmsg.CreatePartitionsRequestTopic{
					Topic:      *topic.Topic,
					Count:      int32(currentPartitionCount + adding),
					Assignment: assignments,
				})
			}

			if len(createReq.Topics) > 0 {
				createResp, err := cl.Client().Request(context.Background(), &createReq)
				if err != nil {
					return fmt.Errorf("unable to create topic partitions: %v", err)
				}
				for _, topic := range createResp.(*kmsg.CreatePartitionsResponse).Topics {
					errStr, msg := errorCells(topic.ErrorCode, topic.ErrorMessage)
					table.Row(topic.Topic, errStr, msg)
				}
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, "TOPIC", "ERROR", "MESSAGE")

	cmd.Flags().IntVarP(&num, "num", "n", 0, "number of partitions to add; the broker places their replicas")
	cmd.Flags().IntVar(&total, "total", 0, "partition count to bring the topic to; past it fails, at it does nothing")
	cmd.Flags().StringArrayVarP(&assigns, "assignment", "a", nil, "brokers for one new partition, comma separated, leader first; repeat once per partition")
	cmd.Flags().StringArrayVarP(&topics, "topic", "t", nil, "old form: topic to add partitions to, with the assignments as arguments")
	cmd.Flags().MarkHidden("topic")
	cmd.Flags().BoolVar(&force, "force", false, "allow modifying internal topics (those starting with \"__\")")

	return cmd
}

func parseAssignments(in string) ([]kmsg.CreatePartitionsRequestTopicAssignment, error) {
	var partitions []kmsg.CreatePartitionsRequestTopicAssignment
	var replicasSize int

	for _, partition := range strings.Split(in, ":") {
		partition = strings.TrimSpace(partition)
		if len(partition) == 0 {
			continue
		}

		var replicas []int32
		for _, replica := range strings.Split(partition, ",") {
			replica = strings.TrimSpace(replica)
			if len(replica) == 0 {
				continue
			}

			r, err := strconv.Atoi(replica)
			if err != nil {
				return nil, fmt.Errorf("unable to parse replica %s", replica)
			}

			for i := range replicas {
				if replicas[i] == int32(r) {
					return nil, errors.New("duplicate brokers not allowed in replica assignment")
				}
			}
			replicas = append(replicas, int32(r))
		}
		if len(replicas) == 0 {
			continue
		}

		if replicasSize == 0 {
			replicasSize = len(replicas)
		} else if len(replicas) != replicasSize {
			return nil, errors.New("all partitions must have the same number of replicas")
		}

		partitions = append(partitions, kmsg.CreatePartitionsRequestTopicAssignment{Replicas: replicas})
	}

	return partitions, nil
}
