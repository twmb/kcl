package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"regexp"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func deleteRecordsCommand(cl *client.Client) *cobra.Command {
	var jsonFile string

	cmd := &cobra.Command{
		Use:   "delete-records",
		Short: "Delete records for topics.",
		Long: `Delete records for topics (Kafka 0.11.0+).

Normally, Kafka records are deleted based off time or log file size. Sometimes,
that does not play well with processing. To address this limitation, Kafka
added record deleting in 0.11.0.

Deleting records works by deleting all segments whose end offsets are earlier
than the requested delete offset, and then disallowing reads earlier than the
requested start offset in a delete request.

This function works either by reading file of record offsets to delete or by
parsing the passed args. The arg format is topic:p#,o# (hopefully obvious).
The file format is as follows:

  [
    {
      "topic": "string",
      "partition": int32,
      "offset": int64
    },
    ...
  ]

It is possible to use both args and the file.

Record deletion works on a fan out basis: each broker containing partitions for
record deletion needs to be issued a request. This does that appropriately.
`,

		Example: `delete-records foo:p0,o120 foo:p1,o3888
		
delete-records --json-file records.json`,

		Run: func(_ *cobra.Command, args []string) {
			tpos, err := parseTopicPartitionOffsets(args)
			out.MaybeDie(err, "unable to parse topic partition offsets: %v", err)

			if jsonFile != "" {
				type fileReq struct {
					Topic     string `json:"topic"`
					Partition int32  `json:"partition"`
					Offset    int64  `json:"offset"`
				}
				var fileReqs []fileReq
				raw, err := ioutil.ReadFile(jsonFile)
				out.MaybeDie(err, "unable to read json file: %v", err)
				err = json.Unmarshal(raw, &fileReqs)
				out.MaybeDie(err, "unable to unmarshal json file: %v", err)
				for _, fileReq := range fileReqs {
					tpos[fileReq.Topic] = append(tpos[fileReq.Topic], partitionOffset{fileReq.Partition, fileReq.Offset})
				}
			}

			if len(tpos) == 0 {
				out.Die("no records requested for deletion")
			}

			// Create and fire off a bunch of concurrent requests...
			type respErr struct {
				broker int32
				resp   kmsg.Response
				err    error
			}
			req := &kmsg.DeleteRecordsRequest{
				TimeoutMillis: cl.TimeoutMillis(),
			}
			for topic, partitionOffsets := range tpos {
				reqTopic := kmsg.DeleteRecordsRequestTopic{
					Topic: topic,
				}
				for _, partitionOffset := range partitionOffsets {
					reqTopic.Partitions = append(reqTopic.Partitions, kmsg.DeleteRecordsRequestTopicPartition{
						Partition: partitionOffset.partition,
						Offset:    partitionOffset.offset,
					})
				}
				req.Topics = append(req.Topics, reqTopic)
			}
			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to issue delete records request: %v", err)
			resp := kresp.(*kmsg.DeleteRecordsResponse)

			tw := out.BeginTabWrite()
			defer tw.Flush()
			fmt.Fprintf(tw, "TOPIC\tPARTITION\tNEW LOW WATERMARK\tERROR\n")
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					msg := "OK"
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						msg = err.Error()
					}
					fmt.Fprintf(tw, "%s\t%d\t%d\t%s\n",
						topic.Topic, partition.Partition, partition.LowWatermark, msg)
				}
			}
		},
	}

	cmd.Flags().StringVar(&jsonFile, "json-file", "", "if non-empty, a json file to read deletions from")

	return cmd
}

type partitionOffset struct {
	partition int32
	offset    int64
}

func parseTopicPartitionOffsets(list []string) (map[string][]partitionOffset, error) {
	rePo := regexp.MustCompile(`^p(\d+),o(\d+)$`)

	tpos := make(map[string][]partitionOffset)
	for _, item := range list {
		split := strings.SplitN(item, ":", 2)
		if len(split[0]) == 0 {
			return nil, fmt.Errorf("item %q invalid empty topic", item)
		}
		if len(split) == 1 {
			return nil, fmt.Errorf("item %q invalid empty partition offsets", item)
		}

		matches := rePo.FindStringSubmatch(split[1])
		if len(matches) == 0 {
			return nil, fmt.Errorf("item %q partition offset %q does not match p(\\d+)o(\\d+)", item, split[1])
		}
		partition, _ := strconv.Atoi(matches[1])
		offset, _ := strconv.Atoi(matches[2])

		tpos[split[0]] = append(tpos[split[0]], partitionOffset{int32(partition), int64(offset)})
	}
	return tpos, nil
}
