// Package metadata provides the cluster metadata command.
package metadata

import (
	"context"
	"fmt"
	"slices"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/commands/admin/topic"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

var (
	clusterHeaders = []string{"CLUSTER-ID", "CONTROLLER"}
	brokerHeaders  = []string{"ID", "HOST", "PORT", "RACK"}
)

// Command is "kcl cluster metadata". It is also mounted, hidden, at the root
// as "kcl metadata", and names cluster.metadata from either path.
func Command(cl *client.Client) *cobra.Command {
	var pinternal, detailed bool
	var ids bool
	var section string

	cmd := &cobra.Command{
		Use:     "metadata [TOPICS...]",
		Aliases: []string{"info"},
		Short:   "Show cluster metadata.",
		Long: `Show cluster metadata.

Issues Metadata (Kafka 0.8.0+), the request every client starts with, and
prints the cluster id and controller, the brokers, and the topics with their
partition and replica counts. Name topics to list only those; by default
every topic is listed, internal topics with -i. A topic the broker answers
with an error keeps its row, with the error in ERROR, and the command exits 1.

--section picks one section: cluster, brokers, or topics. Text prints every
section by default; awk prints the topics section by default, since the
sections have different columns; json carries the sections asked for. The
rows are CLUSTER-ID CONTROLLER, ID HOST PORT RACK, and TOPIC TOPIC-ID
PARTITIONS REPLICATION ERROR. In text the controller broker and an internal
topic are marked with *; json carries internal as a key.

For what Metadata does not answer, the authorized operations or the
cluster's own view of its brokers, use "kcl cluster describe", which issues
DescribeCluster instead.

EXAMPLES:
  kcl cluster metadata                    # cluster, brokers, and topics
  kcl cluster metadata foo bar            # two topics
  kcl cluster metadata --section brokers  # the brokers alone
  kcl cluster metadata --format awk       # topic rows as TSV

SEE ALSO:
  kcl cluster describe   the DescribeCluster view of the cluster
  kcl topic list         list topics
  kcl topic describe     describe topic partitions
`,

		RunE: func(_ *cobra.Command, topics []string) error {
			cl.SetCommand("cluster.metadata")
			switch section {
			case "", "cluster", "brokers", "topics":
			default:
				return out.Errf(out.ExitUsage, "invalid --section %q: must be cluster, brokers, or topics", section)
			}

			pcluster := section == "" || section == "cluster"
			pbrokers := section == "" || section == "brokers"
			ptopics := section == "" || section == "topics"
			if len(topics) > 0 {
				ptopics = true
			}

			req := kmsg.NewPtrMetadataRequest()
			if !ptopics {
				req.Topics = []kmsg.MetadataRequestTopic{} // nil is all, empty is none
			}
			for _, t := range topics {
				rt := kmsg.NewMetadataRequestTopic()
				if ids {
					id, err := flagutil.ParseTopicID(t)
					if err != nil {
						return out.Errf(out.ExitUsage, "invalid topic id %q: %v", t, err)
					}
					rt.TopicID = id
				} else {
					rt.Topic = kmsg.StringPtr(t)
				}
				req.Topics = append(req.Topics, rt)
			}

			resp, err := req.RequestWith(context.Background(), cl.Client())
			if err != nil {
				return fmt.Errorf("unable to get metadata: %v", err)
			}
			sortBrokers(resp.Brokers)
			// A topic you named is listed even if it is internal.
			internal := pinternal || len(topics) > 0

			if detailed {
				topic.SortTopics(resp.Topics)
				var names []string
				for _, t := range resp.Topics {
					if t.Topic != nil && (internal || !t.IsInternal) {
						names = append(names, *t.Topic)
					}
				}
				return topic.Describe(cl, topic.DescribeOpts{}, names)
			}

			var clusterID any = out.Unknown
			if resp.ClusterID != nil {
				clusterID = *resp.ClusterID
			}
			var controller any = out.Unknown
			if resp.ControllerID >= 0 {
				controller = resp.ControllerID
			}
			brokerRows := func(star bool) [][]any {
				rows := make([][]any, 0, len(resp.Brokers))
				for _, b := range resp.Brokers {
					var id any = b.NodeID
					if star && b.NodeID == resp.ControllerID {
						id = fmt.Sprintf("%d*", b.NodeID)
					}
					var rack any = out.Unknown
					if b.Rack != nil {
						rack = *b.Rack
					}
					rows = append(rows, []any{id, b.Host, b.Port, rack})
				}
				return rows
			}
			topicRows, failed := topic.ListRows(resp.Version, resp.Topics, internal)
			if !ptopics {
				failed = false
			}

			switch cl.Format() {
			case out.FormatJSON:
				fields := make(map[string]any)
				if pcluster {
					fields["cluster_id"] = clusterID
					fields["controller"] = controller
				}
				if pbrokers {
					brokers := make([]map[string]any, 0, len(resp.Brokers))
					for _, row := range brokerRows(false) {
						brokers = append(brokers, map[string]any{"id": row[0], "host": row[1], "port": row[2], "rack": row[3]})
					}
					fields["brokers"] = brokers
				}
				if ptopics {
					fields["topics"] = topic.ListRowMaps(topicRows)
				}
				out.MarshalJSON(cl.Command(), 1, fields)

			case out.FormatAWK:
				awkSection := section
				if awkSection == "" {
					awkSection = "topics"
				}
				switch awkSection {
				case "cluster":
					table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "cluster", clusterHeaders...)
					table.Row(clusterID, controller)
					table.Flush()
				case "brokers":
					table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "brokers", brokerHeaders...)
					for _, row := range brokerRows(false) {
						table.Row(row...)
					}
					table.Flush()
				case "topics":
					topic.ListTable(cl.Format(), cl.Command(), topicRows).Flush()
				}

			default:
				var printed bool
				sectionBreak := func() {
					if printed {
						fmt.Println()
					}
					printed = true
				}
				if pcluster {
					sectionBreak()
					tw := out.NewTabWriter()
					fmt.Fprintf(tw, "CLUSTER-ID\t%v\n", clusterID)
					fmt.Fprintf(tw, "CONTROLLER\t%v\n", controller)
					tw.Flush()
				}
				if pbrokers {
					sectionBreak()
					table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "brokers", brokerHeaders...)
					for _, row := range brokerRows(true) {
						table.Row(row...)
					}
					table.Flush()
				}
				if ptopics {
					sectionBreak()
					topic.ListTable(cl.Format(), cl.Command(), topicRows).Flush()
				}
			}
			if failed {
				return out.ErrSilent
			}
			return nil
		},
	}
	out.ColumnsFunc(cmd, func() []string {
		switch {
		case detailed:
			return topic.DescribeHeaders("")
		case section == "cluster":
			return clusterHeaders
		case section == "brokers":
			return brokerHeaders
		}
		return topic.ListHeaders
	})

	cmd.Flags().StringVar(&section, "section", "", "output section (cluster, brokers, topics; default: all for text, topics for awk)")
	cmd.Flags().BoolVar(&ids, "ids", false, "whether the input topics should be parsed as topic IDs")
	cmd.Flags().BoolVarP(&pinternal, "internal", "i", false, "print internal topics if all topics are printed")
	cmd.Flags().BoolVar(&detailed, "detailed", false, "describe the listed topics, as kcl topic describe does")
	cmd.Flags().MarkHidden("detailed")
	return cmd
}

func sortBrokers(brokers []kmsg.MetadataResponseBroker) {
	slices.SortFunc(brokers, func(l, r kmsg.MetadataResponseBroker) int {
		return int(l.NodeID - r.NodeID)
	})
}
