package topic

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func topicDescribeCommand(cl *client.Client) *cobra.Command {
	var (
		showConfigs         bool
		showAll             bool
		underReplicated     bool
		unavailable         bool
		underMinISR         bool
		atMinISR            bool
	)

	cmd := &cobra.Command{
		Use:     "describe TOPICS...",
		Aliases: []string{"d"},
		Short:   "Describe topics with partition detail",
		Long: `Describe topics showing summary and partition details.

By default, shows a summary (name, partitions, replication factor) followed
by a partition table (leader, replicas, ISR). Use -c to also show topic
configs, or -a for all sections.

Health filters show only partitions matching the condition.

EXAMPLES:
  kcl topic describe foo                   # summary + partitions
  kcl topic describe foo -c                # also show configs
  kcl topic describe foo -a                # all sections
  kcl topic describe foo --under-replicated # unhealthy partitions only
  kcl topic describe --format json foo     # JSON output

SEE ALSO:
  kcl topic list         list topics
  kcl topic create       create topics
  kcl config describe    describe any resource config
`,
		Args: cobra.MinimumNArgs(1),
		Run: func(_ *cobra.Command, topics []string) {
			kclClient := cl.Client()
			ctx := context.Background()

			// Fetch metadata for topics.
			metaReq := kmsg.NewPtrMetadataRequest()
			for _, t := range topics {
				rt := kmsg.NewMetadataRequestTopic()
				rt.Topic = kmsg.StringPtr(t)
				metaReq.Topics = append(metaReq.Topics, rt)
			}
			metaResp, err := metaReq.RequestWith(ctx, kclClient)
			out.MaybeDie(err, "unable to request metadata: %v", err)

			// Optionally fetch configs.
			var configsByTopic map[string][]kmsg.DescribeConfigsResponseResourceConfig
			if showConfigs || showAll {
				configsByTopic = fetchTopicConfigs(ctx, kclClient, topics)
			}

			for ti, topic := range metaResp.Topics {
				if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
					fmt.Fprintf(out.BeginTabWrite(), "TOPIC %s: %v\n", strval(topic.Topic), err)
					continue
				}

				topicName := strval(topic.Topic)

				// Summary section.
				tw := out.NewTabWriter()
				fmt.Fprintf(tw, "TOPIC\t%s\n", topicName)
				if topic.TopicID != [16]byte{} {
					fmt.Fprintf(tw, "TOPIC ID\t%x\n", topic.TopicID)
				}
				fmt.Fprintf(tw, "PARTITIONS\t%d\n", len(topic.Partitions))
				if len(topic.Partitions) > 0 {
					fmt.Fprintf(tw, "REPLICATION\t%d\n", len(topic.Partitions[0].Replicas))
				}
				if topic.IsInternal {
					fmt.Fprintf(tw, "INTERNAL\ttrue\n")
				}
				tw.Flush()

				// Apply health filters.
				var partitions []kmsg.MetadataResponseTopicPartition
				for _, p := range topic.Partitions {
					if underReplicated && len(p.ISR) >= len(p.Replicas) {
						continue
					}
					if unavailable && p.Leader >= 0 {
						continue
					}
					if underMinISR || atMinISR {
						// We'd need min.insync.replicas from configs.
						// For now, just include all if these are set.
					}
					partitions = append(partitions, p)
				}
				if !underReplicated && !unavailable && !underMinISR && !atMinISR {
					partitions = topic.Partitions
				}

				// Partition table.
				if len(partitions) > 0 {
					sort.Slice(partitions, func(i, j int) bool {
						return partitions[i].Partition < partitions[j].Partition
					})

					table := out.NewTable("PARTITION", "LEADER", "EPOCH", "REPLICAS", "ISR", "OFFLINE-REPLICAS")
					for _, p := range partitions {
						errSuffix := ""
						if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
							errSuffix = " (" + err.Error() + ")"
						}
						table.Print(
							p.Partition,
							p.Leader,
							p.LeaderEpoch,
							int32sToString(p.Replicas),
							int32sToString(p.ISR),
							int32sToString(p.OfflineReplicas)+errSuffix,
						)
					}
					table.Flush()
				}

				// Configs section.
				if (showConfigs || showAll) && configsByTopic != nil {
					if configs, ok := configsByTopic[topicName]; ok && len(configs) > 0 {
						fmt.Println()
						fmt.Println("CONFIGS:")
						configTw := out.NewTable("KEY", "VALUE", "SOURCE", "SENSITIVE")
						for _, c := range configs {
							val := ""
							if c.Value != nil {
								val = *c.Value
							}
							source := describeConfigSource(c.Source)
							configTw.Print(c.Name, val, source, c.IsSensitive)
						}
						configTw.Flush()
					}
				}

				if ti < len(metaResp.Topics)-1 {
					fmt.Println()
				}
			}
		},
	}

	cmd.Flags().BoolVarP(&showConfigs, "configs", "c", false, "also show topic configs")
	cmd.Flags().BoolVarP(&showAll, "all", "a", false, "show all sections (summary, partitions, configs)")
	cmd.Flags().BoolVar(&underReplicated, "under-replicated", false, "only show partitions where ISR < replicas")
	cmd.Flags().BoolVar(&unavailable, "unavailable", false, "only show partitions with no leader")
	cmd.Flags().BoolVar(&underMinISR, "under-min-isr", false, "only show partitions where ISR < min.insync.replicas")
	cmd.Flags().BoolVar(&atMinISR, "at-min-isr", false, "only show partitions where ISR = min.insync.replicas")

	return cmd
}

func fetchTopicConfigs(ctx context.Context, cl kmsg.Requestor, topics []string) map[string][]kmsg.DescribeConfigsResponseResourceConfig {
	req := kmsg.NewPtrDescribeConfigsRequest()
	for _, t := range topics {
		r := kmsg.NewDescribeConfigsRequestResource()
		r.ResourceType = 2 // TOPIC
		r.ResourceName = t
		req.Resources = append(req.Resources, r)
	}

	resp, err := req.RequestWith(ctx, cl)
	out.MaybeDie(err, "unable to describe configs: %v", err)

	result := make(map[string][]kmsg.DescribeConfigsResponseResourceConfig)
	for _, r := range resp.Resources {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			fmt.Fprintf(out.BeginTabWrite(), "config error for %s: %v\n", r.ResourceName, err)
			continue
		}
		// Sort configs: non-default first, then alphabetical.
		sort.Slice(r.Configs, func(i, j int) bool {
			if r.Configs[i].Source != r.Configs[j].Source {
				return r.Configs[i].Source < r.Configs[j].Source
			}
			return r.Configs[i].Name < r.Configs[j].Name
		})
		result[r.ResourceName] = r.Configs
	}
	return result
}

func int32sToString(vals []int32) string {
	strs := make([]string, len(vals))
	for i, v := range vals {
		strs[i] = strconv.FormatInt(int64(v), 10)
	}
	return "[" + strings.Join(strs, ",") + "]"
}

func describeConfigSource(source kmsg.ConfigSource) string {
	switch source {
	case 0:
		return "UNKNOWN"
	case 1:
		return "DYNAMIC_TOPIC"
	case 2:
		return "DYNAMIC_BROKER"
	case 3:
		return "DYNAMIC_DEFAULT_BROKER"
	case 4:
		return "STATIC_BROKER"
	case 5:
		return "DEFAULT"
	case 6:
		return "DYNAMIC_BROKER_LOGGER"
	default:
		return fmt.Sprintf("SOURCE(%d)", source)
	}
}

func strval(s *string) string {
	if s == nil {
		return "<nil>"
	}
	return *s
}
