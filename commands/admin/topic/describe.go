package topic

import (
	"context"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/flagutil"
	"github.com/twmb/kcl/out"
)

// DescribeOpts are the flags of "kcl topic describe". "kcl topic list
// --detailed" and "kcl cluster metadata --detailed" run Describe with the
// defaults.
type DescribeOpts struct {
	Section         string // "", summary, partitions, or configs
	Stable          bool
	WithOverrides   bool
	UnderReplicated bool
	Unavailable     bool
	UnderMinISR     bool
	AtMinISR        bool
	TopicIDs        [][16]byte
}

var (
	describeSummaryHeaders    = []string{"TOPIC", "TOPIC-ID", "PARTITIONS", "REPLICATION", "ERROR"}
	describePartitionsHeaders = []string{"TOPIC", "PARTITION", "LEADER", "LEADER-EPOCH", "REPLICAS", "ISR", "OFFLINE-REPLICAS", "START-OFFSET", "END-OFFSET", "STABLE-OFFSET", "ERROR"}
	describeConfigsHeaders    = []string{"TOPIC", "KEY", "VALUE", "SOURCE", "SENSITIVE", "ERROR"}
)

// DescribeHeaders are the awk columns of a describe section, partitions when
// section is "".
func DescribeHeaders(section string) []string {
	return describeHeaders(section)
}

// describeHeaders are the awk columns of a describe section; the default
// section is partitions.
func describeHeaders(section string) []string {
	switch section {
	case "summary":
		return describeSummaryHeaders
	case "configs":
		return describeConfigsHeaders
	}
	return describePartitionsHeaders
}

func topicDescribeCommand(cl *client.Client) *cobra.Command {
	var (
		opts     DescribeOpts
		topicIDs []string
	)

	cmd := &cobra.Command{
		Use:     "describe [TOPICS...]",
		Aliases: []string{"d"},
		Short:   "Describe topics with partition detail.",
		Long: `Describe topics with partition detail.

Describe topics showing summary, partitions, and optionally configs.

By default in text mode, shows all sections. Use --section to select one.
JSON carries the sections asked for, all of them by default.

--format awk prints one section: the partition rows by default, one row per
partition, or the rows of the --section given. A partition row is TOPIC
PARTITION LEADER LEADER-EPOCH REPLICAS ISR OFFLINE-REPLICAS START-OFFSET
END-OFFSET STABLE-OFFSET ERROR; the offsets come from ListOffsets, and
STABLE-OFFSET is filled only with --stable. A summary row is TOPIC TOPIC-ID
PARTITIONS REPLICATION ERROR, and a configs row is TOPIC KEY VALUE SOURCE
SENSITIVE ERROR. The configs a topic runs with also come from "kcl config
describe TOPIC -tt". Text marks an internal topic with a * after its name;
json carries internal as a key.

Health filters show only partitions matching the condition; the min ISR
filters read min.insync.replicas from the topic's configs.

A topic the broker answers with an error keeps its row, with the error in
ERROR, and the command exits 1.

An argument is a topic name; one shaped like a topic id that names no topic is
looked up as an id instead. A name wins over an id, so --topic-id is how to
mean the id when a topic is named after one. Ids are 32 hex characters with
optional dashes.

EXAMPLES:
  kcl topic describe foo                          # all sections
  kcl topic describe foo --section configs        # configs only
  kcl topic describe foo --under-replicated       # unhealthy partitions
  kcl topic describe foo --format json            # JSON output
  kcl topic describe foo --format awk             # partitions as TSV
  kcl topic describe --topic-id 15fc1bf40a5c1c3cdd363ec28f5c0c69

SEE ALSO:
  kcl topic list          list topics
  kcl topic list-offsets  start, stable, and end offsets with their epochs
  kcl topic create        create topics
  kcl config describe     describe any resource config
`,
		RunE: func(_ *cobra.Command, topics []string) error {
			if len(topics) == 0 && len(topicIDs) == 0 {
				return out.Errf(out.ExitUsage, "at least one topic name or --topic-id is required")
			}
			opts.TopicIDs = opts.TopicIDs[:0]
			for _, raw := range topicIDs {
				id, err := flagutil.ParseTopicID(raw)
				if err != nil {
					return out.Errf(out.ExitUsage, "invalid --topic-id %q: %v", raw, err)
				}
				opts.TopicIDs = append(opts.TopicIDs, id)
			}
			switch opts.Section {
			case "", "summary", "partitions", "configs":
			default:
				return out.Errf(out.ExitUsage, "invalid --section %q: must be summary, partitions, or configs", opts.Section)
			}
			topics, err := flagutil.ResolveTopics(context.Background(), cl.Client(), topics)
			if err != nil {
				return err
			}
			return Describe(cl, opts, topics)
		},
	}
	out.ColumnsFunc(cmd, func() []string { return describeHeaders(opts.Section) })

	cmd.Flags().StringVar(&opts.Section, "section", "", "output section (summary, partitions, configs; default: all for text, partitions for awk)")
	cmd.Flags().BoolVar(&opts.Stable, "stable", false, "fill STABLE-OFFSET, the last stable (read_committed) offset, for transactional topics")
	cmd.Flags().BoolVar(&opts.WithOverrides, "with-overrides", false, "only show topics with non-default config overrides (implies config fetching)")
	cmd.Flags().BoolVar(&opts.UnderReplicated, "under-replicated", false, "only show partitions where ISR < replicas")
	cmd.Flags().BoolVar(&opts.Unavailable, "unavailable", false, "only show partitions with no leader")
	cmd.Flags().BoolVar(&opts.UnderMinISR, "under-min-isr", false, "only show partitions where ISR < min.insync.replicas")
	cmd.Flags().BoolVar(&opts.AtMinISR, "at-min-isr", false, "only show partitions where ISR = min.insync.replicas")
	cmd.Flags().StringArrayVar(&topicIDs, "topic-id", nil, "topic UUID to describe (repeatable; 32 hex chars with optional dashes)")

	return cmd
}

// describedTopic is one topic as describe prints it: its metadata, the
// partitions the health filters kept, and its configs when asked for.
type describedTopic struct {
	meta       kmsg.MetadataResponseTopic
	err        error
	partitions []kmsg.MetadataResponseTopicPartition
	configs    []kmsg.DescribeConfigsResponseResourceConfig
}

func (d *describedTopic) name() any {
	if d.meta.Topic == nil {
		return out.Unknown
	}
	return *d.meta.Topic
}

func (d *describedTopic) nameStr() string {
	if d.meta.Topic == nil {
		return ""
	}
	return *d.meta.Topic
}

func (d *describedTopic) replication() int {
	if len(d.meta.Partitions) > 0 {
		return len(d.meta.Partitions[0].Replicas)
	}
	return 0
}

// Describe is "kcl topic describe" for topics, which are names; ids come
// through opts. The document names topic.describe from whichever command
// ran it.
func Describe(cl *client.Client, opts DescribeOpts, topics []string) error {
	cl.SetCommand("topic.describe")

	showSummary := opts.Section == "" || opts.Section == "summary"
	showPartitions := opts.Section == "" || opts.Section == "partitions"
	showConfigs := opts.Section == "" || opts.Section == "configs"
	needConfigs := showConfigs || opts.WithOverrides || opts.UnderMinISR || opts.AtMinISR

	kclClient := cl.Client()
	ctx := context.Background()

	metaReq := kmsg.NewPtrMetadataRequest()
	for _, t := range topics {
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(t)
		metaReq.Topics = append(metaReq.Topics, rt)
	}
	for _, id := range opts.TopicIDs {
		rt := kmsg.NewMetadataRequestTopic()
		rt.TopicID = id // Topic left nil: the broker resolves the name (v10+)
		metaReq.Topics = append(metaReq.Topics, rt)
	}
	metaResp, err := metaReq.RequestWith(ctx, kclClient)
	if err != nil {
		return fmt.Errorf("unable to request metadata: %v", err)
	}
	SortTopics(metaResp.Topics)

	// The same topic answers twice when you pass both its name and its id;
	// keep the first. Downstream lookups go by name, so a topic resolved
	// from an id joins the names.
	var described []*describedTopic
	seen := make(map[string]bool)
	for _, mt := range metaResp.Topics {
		d := &describedTopic{meta: mt, err: kerr.ErrorForCode(mt.ErrorCode)}
		if name := d.nameStr(); name != "" && d.err == nil {
			if seen[name] {
				continue
			}
			seen[name] = true
			if !slices.Contains(topics, name) {
				topics = append(topics, name)
			}
		}
		described = append(described, d)
	}

	var configsFailed bool
	if needConfigs {
		configsByTopic, failed, err := fetchTopicConfigs(ctx, kclClient, topics)
		if err != nil {
			return err
		}
		configsFailed = failed
		for _, d := range described {
			d.configs = configsByTopic[d.nameStr()]
		}
		if opts.WithOverrides {
			described = slices.DeleteFunc(described, func(d *describedTopic) bool {
				return d.err == nil && !slices.ContainsFunc(d.configs, func(c kmsg.DescribeConfigsResponseResourceConfig) bool {
					return c.Source == kmsg.ConfigSourceDynamicTopicConfig
				})
			})
		}
	}

	// Health filters.
	for _, d := range described {
		minISR, haveMinISR := minInsyncReplicas(d.configs)
		for _, p := range d.meta.Partitions {
			switch {
			case opts.UnderReplicated && len(p.ISR) >= len(p.Replicas):
			case opts.Unavailable && p.Leader >= 0:
			case opts.UnderMinISR && (!haveMinISR || len(p.ISR) >= minISR):
			case opts.AtMinISR && (!haveMinISR || len(p.ISR) != minISR):
			default:
				d.partitions = append(d.partitions, p)
			}
		}
	}

	// One start and one end listing for every partition kept, and the
	// stable offsets only when asked, since that is a third request.
	var starts, ends, stables listedOffsets
	if showPartitions {
		tps := make(map[string][]int32)
		for _, d := range described {
			for _, p := range d.partitions {
				tps[d.nameStr()] = append(tps[d.nameStr()], p.Partition)
			}
		}
		listings := []listOffsetsAt{{readUncommitted, tsStart}, {readUncommitted, tsEnd}}
		if opts.Stable {
			listings = append(listings, listOffsetsAt{readCommitted, tsEnd})
		}
		listed := listOffsetsAll(ctx, kclClient, tps, listings...)
		starts, ends = listed[0], listed[1]
		if opts.Stable {
			stables = listed[2]
		}
	}

	// partitionRow is one partition in the shape every format prints.
	partitionRow := func(d *describedTopic, p kmsg.MetadataResponseTopicPartition) []any {
		topic := d.nameStr()
		var leader, epoch any = out.Unknown, out.Unknown
		if p.Leader >= 0 {
			leader = p.Leader
		}
		if p.LeaderEpoch >= 0 {
			epoch = p.LeaderEpoch
		}
		var start, end, stable any = out.Unknown, out.Unknown, out.Unknown
		errs := []error{kerr.ErrorForCode(p.ErrorCode)}
		if showPartitions {
			s, e := starts.get(topic, p.Partition), ends.get(topic, p.Partition)
			start, end = s.cell(), e.cell()
			errs = append(errs, s.err, e.err)
			if opts.Stable {
				st := stables.get(topic, p.Partition)
				stable = st.cell()
				errs = append(errs, st.err)
			}
		}
		errStr := ""
		for _, err := range errs {
			if err != nil {
				errStr = out.ErrCell(err)
				break
			}
		}
		return []any{
			topic, p.Partition, leader, epoch,
			int32sToString(p.Replicas), int32sToString(p.ISR), int32sToString(p.OfflineReplicas),
			start, end, stable, errStr,
		}
	}
	failed := configsFailed
	for _, d := range described {
		if d.err != nil {
			failed = true
		}
	}

	switch cl.Format() {
	case out.FormatJSON:
		type partJSON struct {
			Partition       int32   `json:"partition"`
			Leader          any     `json:"leader"`
			LeaderEpoch     any     `json:"leader_epoch"`
			Replicas        []int32 `json:"replicas"`
			ISR             []int32 `json:"isr"`
			OfflineReplicas []int32 `json:"offline_replicas"`
			StartOffset     any     `json:"start_offset"`
			EndOffset       any     `json:"end_offset"`
			StableOffset    any     `json:"stable_offset"`
			Error           string  `json:"error"`
		}
		type configJSON struct {
			Key       string `json:"key"`
			Value     string `json:"value"`
			Source    string `json:"source"`
			Sensitive bool   `json:"sensitive"`
		}
		type topicJSON struct {
			Topic             any          `json:"topic"`
			TopicID           any          `json:"topic_id"`
			PartitionCount    any          `json:"partition_count"`
			ReplicationFactor any          `json:"replication_factor"`
			Internal          any          `json:"internal"`
			Error             string       `json:"error"`
			Partitions        []partJSON   `json:"partitions,omitzero"`
			Configs           []configJSON `json:"configs,omitzero"`
		}
		topicsOut := make([]topicJSON, 0, len(described))
		for _, d := range described {
			tj := topicJSON{
				Topic:             d.name(),
				TopicID:           topicIDCell(d.meta.TopicID),
				PartitionCount:    len(d.meta.Partitions),
				ReplicationFactor: d.replication(),
				Internal:          d.meta.IsInternal,
			}
			if d.err != nil {
				tj.Error = out.ErrCell(d.err)
				tj.PartitionCount, tj.ReplicationFactor, tj.Internal = out.Unknown, out.Unknown, out.Unknown
			}
			if showPartitions {
				tj.Partitions = make([]partJSON, 0, len(d.partitions))
				for _, p := range d.partitions {
					row := partitionRow(d, p)
					if row[10] != "" {
						failed = true
					}
					tj.Partitions = append(tj.Partitions, partJSON{
						Partition:       p.Partition,
						Leader:          row[2],
						LeaderEpoch:     row[3],
						Replicas:        orEmpty(p.Replicas),
						ISR:             orEmpty(p.ISR),
						OfflineReplicas: orEmpty(p.OfflineReplicas),
						StartOffset:     row[7],
						EndOffset:       row[8],
						StableOffset:    row[9],
						Error:           row[10].(string),
					})
				}
			}
			if showConfigs {
				tj.Configs = make([]configJSON, 0, len(d.configs))
				for _, c := range d.configs {
					tj.Configs = append(tj.Configs, configJSON{
						Key:       c.Name,
						Value:     strval(c.Value),
						Source:    c.Source.String(),
						Sensitive: c.IsSensitive,
					})
				}
			}
			topicsOut = append(topicsOut, tj)
		}
		out.MarshalJSON(cl.Command(), 1, map[string]any{"topics": topicsOut})

	case out.FormatAWK:
		section := opts.Section
		if section == "" {
			section = "partitions"
		}
		table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "topics", describeHeaders(section)...)
		for _, d := range described {
			errStr := out.ErrCell(d.err)
			switch section {
			case "summary":
				if d.err != nil {
					table.Row(d.name(), topicIDCell(d.meta.TopicID), out.Unknown, out.Unknown, errStr)
					continue
				}
				table.Row(d.name(), topicIDCell(d.meta.TopicID), len(d.meta.Partitions), d.replication(), errStr)
			case "partitions":
				if d.err != nil {
					table.Row(d.name(), out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.Unknown, out.Unknown, errStr)
					continue
				}
				for _, p := range d.partitions {
					row := partitionRow(d, p)
					if row[10] != "" {
						failed = true
					}
					table.Row(row...)
				}
			case "configs":
				if d.err != nil {
					table.Row(d.name(), out.Unknown, out.Unknown, out.Unknown, out.Unknown, errStr)
					continue
				}
				for _, c := range d.configs {
					table.Row(d.name(), c.Name, strval(c.Value), c.Source.String(), c.IsSensitive, "")
				}
			}
		}
		if err := table.Flush(); err != nil {
			return err
		}

	default:
		for ti, d := range described {
			if ti > 0 {
				fmt.Println()
			}
			if showSummary || d.err != nil {
				tw := out.NewTabWriter()
				name := fmt.Sprint(d.name())
				if d.meta.IsInternal {
					name += "*"
				}
				fmt.Fprintf(tw, "TOPIC\t%s\n", name)
				if d.meta.TopicID != [16]byte{} {
					fmt.Fprintf(tw, "TOPIC-ID\t%x\n", d.meta.TopicID)
				}
				if d.err != nil {
					fmt.Fprintf(tw, "ERROR\t%s\n", out.ErrCell(d.err))
					tw.Flush()
					continue
				}
				fmt.Fprintf(tw, "PARTITIONS\t%d\n", len(d.meta.Partitions))
				fmt.Fprintf(tw, "REPLICATION\t%d\n", d.replication())
				tw.Flush()
			}

			if showPartitions && len(d.partitions) > 0 {
				if showSummary {
					fmt.Println()
				}
				// The topic is the summary's; STABLE-OFFSET is hidden
				// unless --stable filled it.
				keep := []int{1, 2, 3, 4, 5, 6, 7, 8, 10}
				if opts.Stable {
					keep = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
				}
				table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "partitions", pick(describePartitionsHeaders, keep)...)
				for _, p := range d.partitions {
					row := partitionRow(d, p)
					if row[10] != "" {
						failed = true
					}
					table.Row(pick(row, keep)...)
				}
				table.Flush()
			}

			if showConfigs && len(d.configs) > 0 {
				fmt.Println()
				fmt.Println("CONFIGS:")
				table := out.NewTable("KEY", "VALUE", "SOURCE", "SENSITIVE")
				for _, c := range d.configs {
					table.Print(c.Name, strval(c.Value), c.Source.String(), c.IsSensitive)
				}
				table.Flush()
			}
		}
	}
	if failed {
		return out.ErrSilent
	}
	return nil
}

// minInsyncReplicas is the topic's min.insync.replicas, if its configs carry
// one we can read.
func minInsyncReplicas(configs []kmsg.DescribeConfigsResponseResourceConfig) (int, bool) {
	for _, c := range configs {
		if c.Name != "min.insync.replicas" || c.Value == nil {
			continue
		}
		n, err := strconv.Atoi(*c.Value)
		return n, err == nil
	}
	return 0, false
}

func orEmpty(vals []int32) []int32 {
	if vals == nil {
		return []int32{}
	}
	return vals
}

// fetchTopicConfigs describes the configs of topics. A missing topic was
// already reported from the metadata response and is skipped; any other
// per-resource error goes to stderr, since the configs rows have no place
// for it, and failed reports it so the command exits 1.
func fetchTopicConfigs(ctx context.Context, cl kmsg.Requestor, topics []string) (configs map[string][]kmsg.DescribeConfigsResponseResourceConfig, failed bool, err error) {
	req := kmsg.NewPtrDescribeConfigsRequest()
	for _, t := range topics {
		r := kmsg.NewDescribeConfigsRequestResource()
		r.ResourceType = kmsg.ConfigResourceTypeTopic
		r.ResourceName = t
		req.Resources = append(req.Resources, r)
	}

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return nil, false, fmt.Errorf("unable to describe configs: %v", err)
	}

	result := make(map[string][]kmsg.DescribeConfigsResponseResourceConfig)
	for _, r := range resp.Resources {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			if err != kerr.UnknownTopicOrPartition {
				fmt.Fprintf(os.Stderr, "unable to describe configs for %s: %v\n", r.ResourceName, out.BrokerErr(err, r.ErrorMessage))
				failed = true
			}
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
	return result, failed, nil
}

func int32sToString(vals []int32) string {
	strs := make([]string, len(vals))
	for i, v := range vals {
		strs[i] = strconv.FormatInt(int64(v), 10)
	}
	return "[" + strings.Join(strs, ",") + "]"
}

func strval(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
