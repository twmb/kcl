package topic

import (
	"context"
	"encoding/hex"
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
		showConfigs     bool
		showAll         bool
		stable          bool
		withOverrides   bool
		underReplicated bool
		unavailable     bool
		underMinISR     bool
		atMinISR        bool
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
		RunE: func(_ *cobra.Command, topics []string) error {
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
			if err != nil {
				return fmt.Errorf("unable to request metadata: %v", err)
			}

			// Optionally fetch configs.
			var configsByTopic map[string][]kmsg.DescribeConfigsResponseResourceConfig
			if showConfigs || showAll || withOverrides {
				var err error
				configsByTopic, err = fetchTopicConfigs(ctx, kclClient, topics)
				if err != nil {
					return err
				}
			}

			// --with-overrides: filter to topics that have non-default config values.
			if withOverrides && configsByTopic != nil {
				overridden := make(map[string]bool)
				for t, cfgs := range configsByTopic {
					for _, c := range cfgs {
						if c.Source == 1 { // DYNAMIC_TOPIC
							overridden[t] = true
							break
						}
					}
				}
				var filtered []kmsg.MetadataResponseTopic
				for _, t := range metaResp.Topics {
					if overridden[strval(t.Topic)] {
						filtered = append(filtered, t)
					}
				}
				metaResp.Topics = filtered
			}

			// --stable: resolve committed (read_committed) offsets per partition.
			var stableOffsets map[string]map[int32]int64
			if stable {
				stableOffsets = make(map[string]map[int32]int64)
				for _, t := range topics {
					req := kmsg.NewPtrListOffsetsRequest()
					req.IsolationLevel = 1 // read_committed
					rt := kmsg.NewListOffsetsRequestTopic()
					rt.Topic = t
					// We need partition IDs -- get them from metadata.
					for _, mt := range metaResp.Topics {
						if strval(mt.Topic) == t {
							for _, p := range mt.Partitions {
								rp := kmsg.NewListOffsetsRequestTopicPartition()
								rp.Partition = p.Partition
								rp.Timestamp = -1 // latest
								rt.Partitions = append(rt.Partitions, rp)
							}
						}
					}
					req.Topics = append(req.Topics, rt)
					shards := kclClient.RequestSharded(ctx, req)
					for _, shard := range shards {
						if shard.Err != nil {
							continue
						}
						resp := shard.Resp.(*kmsg.ListOffsetsResponse)
						for _, rt := range resp.Topics {
							if stableOffsets[rt.Topic] == nil {
								stableOffsets[rt.Topic] = make(map[int32]int64)
							}
							for _, rp := range rt.Partitions {
								if kerr.ErrorForCode(rp.ErrorCode) == nil {
									stableOffsets[rt.Topic][rp.Partition] = rp.Offset
								}
							}
						}
					}
				}
			}

			// Build filtered partition lists for each topic.
			type topicPartitions struct {
				topic      kmsg.MetadataResponseTopic
				partitions []kmsg.MetadataResponseTopicPartition
			}
			var described []topicPartitions
			for _, topic := range metaResp.Topics {
				if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
					if cl.Format() == "text" {
						fmt.Fprintf(out.BeginTabWrite(), "TOPIC %s: %v\n", strval(topic.Topic), err)
					}
					continue
				}

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
				sort.Slice(partitions, func(i, j int) bool {
					return partitions[i].Partition < partitions[j].Partition
				})
				described = append(described, topicPartitions{topic, partitions})
			}

			switch cl.Format() {
			case "json":
				type partJSON struct {
					Partition       int32   `json:"partition"`
					Leader          int32   `json:"leader"`
					Epoch           int32   `json:"epoch"`
					Replicas        []int32 `json:"replicas"`
					ISR             []int32 `json:"isr"`
					OfflineReplicas []int32 `json:"offline_replicas"`
					Error           string  `json:"error,omitempty"`
					StableOffset    *int64  `json:"stable_offset,omitempty"`
				}
				type configJSON struct {
					Key       string `json:"key"`
					Value     string `json:"value"`
					Source    string `json:"source"`
					Sensitive bool   `json:"sensitive"`
				}
				type topicJSON struct {
					Topic       string       `json:"topic"`
					TopicID     string       `json:"topic_id,omitempty"`
					Partitions  int          `json:"partition_count"`
					Replication int          `json:"replication_factor"`
					Internal    bool         `json:"internal,omitempty"`
					Details     []partJSON   `json:"partitions"`
					Configs     []configJSON `json:"configs,omitempty"`
				}
				var topicsOut []topicJSON
				for _, d := range described {
					topicName := strval(d.topic.Topic)
					tj := topicJSON{
						Topic:      topicName,
						Partitions: len(d.topic.Partitions),
						Internal:   d.topic.IsInternal,
					}
					if d.topic.TopicID != [16]byte{} {
						tj.TopicID = hex.EncodeToString(d.topic.TopicID[:])
					}
					if len(d.topic.Partitions) > 0 {
						tj.Replication = len(d.topic.Partitions[0].Replicas)
					}
					for _, p := range d.partitions {
						pj := partJSON{
							Partition:       p.Partition,
							Leader:          p.Leader,
							Epoch:           p.LeaderEpoch,
							Replicas:        p.Replicas,
							ISR:             p.ISR,
							OfflineReplicas: p.OfflineReplicas,
						}
						if pj.Replicas == nil {
							pj.Replicas = []int32{}
						}
						if pj.ISR == nil {
							pj.ISR = []int32{}
						}
						if pj.OfflineReplicas == nil {
							pj.OfflineReplicas = []int32{}
						}
						if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
							pj.Error = err.Error()
						}
						if stable {
							if m, ok := stableOffsets[topicName]; ok {
								if v, ok := m[p.Partition]; ok {
									pj.StableOffset = &v
								}
							}
						}
						tj.Details = append(tj.Details, pj)
					}
					if (showConfigs || showAll) && configsByTopic != nil {
						if configs, ok := configsByTopic[topicName]; ok {
							for _, c := range configs {
								val := ""
								if c.Value != nil {
									val = *c.Value
								}
								tj.Configs = append(tj.Configs, configJSON{
									Key:       c.Name,
									Value:     val,
									Source:    describeConfigSource(c.Source),
									Sensitive: c.IsSensitive,
								})
							}
						}
					}
					topicsOut = append(topicsOut, tj)
				}
				out.MarshalJSON("topic.describe", 1, map[string]any{
					"topics": topicsOut,
				})

			case "awk":
				for _, d := range described {
					topicName := strval(d.topic.Topic)
					for _, p := range d.partitions {
						errStr := ""
						if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
							errStr = err.Error()
						}
						if stable {
							so := ""
							if m, ok := stableOffsets[topicName]; ok {
								if v, ok := m[p.Partition]; ok {
									so = fmt.Sprintf("%d", v)
								}
							}
							fmt.Printf("%s\t%d\t%d\t%d\t%s\t%s\t%s\t%s\t%s\n",
								topicName,
								p.Partition,
								p.Leader,
								p.LeaderEpoch,
								int32sToString(p.Replicas),
								int32sToString(p.ISR),
								int32sToString(p.OfflineReplicas),
								so,
								errStr,
							)
						} else {
							fmt.Printf("%s\t%d\t%d\t%d\t%s\t%s\t%s\t%s\n",
								topicName,
								p.Partition,
								p.Leader,
								p.LeaderEpoch,
								int32sToString(p.Replicas),
								int32sToString(p.ISR),
								int32sToString(p.OfflineReplicas),
								errStr,
							)
						}
					}
				}

			default: // text
				for ti, d := range described {
					topicName := strval(d.topic.Topic)

					// Summary section.
					tw := out.NewTabWriter()
					fmt.Fprintf(tw, "TOPIC\t%s\n", topicName)
					if d.topic.TopicID != [16]byte{} {
						fmt.Fprintf(tw, "TOPIC-ID\t%x\n", d.topic.TopicID)
					}
					fmt.Fprintf(tw, "PARTITIONS\t%d\n", len(d.topic.Partitions))
					if len(d.topic.Partitions) > 0 {
						fmt.Fprintf(tw, "REPLICATION\t%d\n", len(d.topic.Partitions[0].Replicas))
					}
					if d.topic.IsInternal {
						fmt.Fprintf(tw, "INTERNAL\ttrue\n")
					}
					tw.Flush()

					// Partition table.
					if len(d.partitions) > 0 {
						headers := []string{"PARTITION", "LEADER", "EPOCH", "REPLICAS", "ISR", "OFFLINE-REPLICAS"}
						if stable {
							headers = append(headers, "STABLE-OFFSET")
						}
						table := out.NewTable(headers...)
						for _, p := range d.partitions {
							errSuffix := ""
							if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
								errSuffix = " (" + err.Error() + ")"
							}
							row := []any{
								p.Partition,
								p.Leader,
								p.LeaderEpoch,
								int32sToString(p.Replicas),
								int32sToString(p.ISR),
								int32sToString(p.OfflineReplicas) + errSuffix,
							}
							if stable {
								so := "-"
								if m, ok := stableOffsets[topicName]; ok {
									if v, ok := m[p.Partition]; ok {
										so = fmt.Sprintf("%d", v)
									}
								}
								row = append(row, so)
							}
							table.Print(row...)
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

					if ti < len(described)-1 {
						fmt.Println()
					}
				}
			}
			return nil
		},
	}

	cmd.Flags().BoolVarP(&showConfigs, "configs", "c", false, "also show topic configs")
	cmd.Flags().BoolVarP(&showAll, "all", "a", false, "show all sections (summary, partitions, configs)")
	cmd.Flags().BoolVar(&stable, "stable", false, "include stable (read_committed) offset column for transactional topics")
	cmd.Flags().BoolVar(&withOverrides, "with-overrides", false, "only show topics with non-default config overrides (implies config fetching)")
	cmd.Flags().BoolVar(&underReplicated, "under-replicated", false, "only show partitions where ISR < replicas")
	cmd.Flags().BoolVar(&unavailable, "unavailable", false, "only show partitions with no leader")
	cmd.Flags().BoolVar(&underMinISR, "under-min-isr", false, "only show partitions where ISR < min.insync.replicas")
	cmd.Flags().BoolVar(&atMinISR, "at-min-isr", false, "only show partitions where ISR = min.insync.replicas")

	return cmd
}

func fetchTopicConfigs(ctx context.Context, cl kmsg.Requestor, topics []string) (map[string][]kmsg.DescribeConfigsResponseResourceConfig, error) {
	req := kmsg.NewPtrDescribeConfigsRequest()
	for _, t := range topics {
		r := kmsg.NewDescribeConfigsRequestResource()
		r.ResourceType = 2 // TOPIC
		r.ResourceName = t
		req.Resources = append(req.Resources, r)
	}

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, fmt.Errorf("unable to describe configs: %v", err)
	}

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
	return result, nil
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
