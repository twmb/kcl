package consume

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/twmb/kafka-go/pkg/kbin"
	"github.com/twmb/kafka-go/pkg/kgo"
	"github.com/twmb/kafka-go/pkg/kmsg"

	"github.com/twmb/kcl/out"
)

///////////////////////
// __consumer_offset //
///////////////////////

func (co *consumeOutput) buildConsumerOffsetsFormatFn() {
	var out []byte
	co.format = func(r *kgo.Record) {
		out = out[:0]
		out = co.formatConsumerOffsets(out, r)
		os.Stdout.Write(out)
	}
}

func (co *consumeOutput) formatConsumerOffsets(out []byte, r *kgo.Record) []byte {
	orig := out
	out = append(out, r.Topic...)
	out = append(out, " partition "...)
	out = strconv.AppendInt(out, int64(r.Partition), 10)
	out = append(out, " offset "...)
	out = strconv.AppendInt(out, r.Offset, 10)
	out = append(out, " at "...)
	out = r.Timestamp.AppendFormat(out, "[2006-01-02 15:04:05.999]\n")

	if len(r.Key) < 2 || r.Key[0] != 0 {
		out = append(out, "(corrupt short key)\n\n"...)
		return out
	}

	var keep bool
	switch v := r.Key[1]; v {
	case 0, 1:
		out, keep = co.formatOffsetCommit(out, r)
	case 2:
		out, keep = co.formatGroupMetadata(out, r)
	default:
		out = append(out, "(unknown offset key format "...)
		out = append(out, r.Key[1])
		out = append(out, ')')
	}
	if !keep {
		return orig
	}

	out = append(out, "\n\n"...)
	return out
}

func formatTransactionalControl(r *kgo.Record) string {
	// ControlRecordType
	if len(r.Key) < 4 {
		return fmt.Sprintf("Transaction Control(Invalid length %d)\n", len(r.Key))
	}

	reader := kbin.Reader{Src: r.Key}
	version := reader.Int16()
	typ := reader.Int16()

	var msg string
	switch typ {
	case 0:
		msg = "ABORT"
	case 1:
		msg = "COMMIT"
	default:
		msg = "UNKNOWN"
	}

	if version != 0 {
		msg += fmt.Sprintf(" (unhandled version %d bytes: %x)", version, reader.Src)
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Transactional Control Record(%d) %s\n", version, msg)

	// EndTransactionMarker class
	if len(r.Value) < 6 {
		fmt.Fprintf(&sb, "\tInvalid control record value length %d\n", len(r.Value))
	}

	reader = kbin.Reader{Src: r.Value}
	version = reader.Int16()
	epoch := reader.Int32()

	tw := out.BeginTabWriteTo(&sb)
	fmt.Fprintf(tw, "\tCoordinator Epoch\t%d\n", epoch)
	if version != 0 {
		fmt.Fprintf(tw, "\tExtra Bytes (v%d)\t%x\n", reader.Src)
	}
	tw.Flush()
	return sb.String()
}

func (co *consumeOutput) formatOffsetCommit(dst []byte, r *kgo.Record) ([]byte, bool) {
	{
		var k kmsg.OffsetCommitKey
		if err := k.ReadFrom(r.Key); err != nil {
			if len(r.Key) == 0 {
				return append(dst, "OffsetCommitKey (empty)\n"...), co.group == ""
			}
			if r.Attrs.IsControl() {
				return append(dst, formatTransactionalControl(r)...), co.group == ""
			}
			return append(dst, fmt.Sprintf("OffsetCommitKey (could not decode %d bytes)\n", len(r.Key))...), co.group == ""
		}

		// We can now apply our group filter: do so; if we are keeping
		// this info, all returns after are true.
		if co.group != "" && co.group != k.Group {
			return dst, false
		}

		w := bytes.NewBuffer(dst)
		fmt.Fprintf(w, "OffsetCommitKey(%d)\n", k.Version)

		tw := out.BeginTabWriteTo(w)
		fmt.Fprintf(tw, "\tGroup\t%s\n", k.Group)
		fmt.Fprintf(tw, "\tTopic\t%s\n", k.Topic)
		fmt.Fprintf(tw, "\tPartition\t%d\n", k.Partition)
		tw.Flush()

		dst = w.Bytes()
	}

	{
		var v kmsg.OffsetCommitValue
		if err := v.ReadFrom(r.Value); err != nil {
			if len(r.Value) == 0 {
				return append(dst, "OffsetCommitValue (empty)\n"...), true
			}
			return append(dst, fmt.Sprintf("OffsetCommitValue (could not decode %d bytes)\n", len(r.Value))...), true
		}

		w := bytes.NewBuffer(dst)
		fmt.Fprintf(w, "OffsetCommitValue(%d)\n", v.Version)

		tw := out.BeginTabWriteTo(w)
		fmt.Fprintf(tw, "\tOffset\t%d\n", v.Offset)
		if v.Version >= 3 {
			fmt.Fprintf(tw, "\tLeaderEpoch\t%d\n", v.LeaderEpoch)
		}
		fmt.Fprintf(tw, "\tMetadata\t%s\n", v.Metadata)
		fmt.Fprintf(tw, "\tCommitTimestamp\t%d\n", v.CommitTimestamp)
		if v.Version == 1 {
			fmt.Fprintf(tw, "\tExpireTimestamp\t%d\n", v.ExpireTimestamp)
		}
		tw.Flush()

		return w.Bytes(), true
	}
}

func (co *consumeOutput) formatGroupMetadata(dst []byte, r *kgo.Record) ([]byte, bool) {
	{
		var k kmsg.GroupMetadataKey
		if err := k.ReadFrom(r.Key); err != nil {
			if len(r.Key) == 0 {
				return append(dst, "GroupMetadataKey (empty)"...), co.group == ""
			}
			return append(dst, fmt.Sprintf("GroupMetadataKey (could not decode %d bytes)", len(r.Key))...), co.group == ""
		}

		// We can now apply our group filter: do so; if we are keeping
		// this info, all returns after are true.
		if co.group != "" && co.group != k.Group {
			return dst, false
		}

		w := bytes.NewBuffer(dst)
		fmt.Fprintf(w, "GroupMetadataKey(%d)\n", k.Version)

		tw := out.BeginTabWriteTo(w)
		fmt.Fprintf(tw, "\tGroup\t%s\n", k.Group)
		tw.Flush()

		dst = w.Bytes()
	}

	{
		var v kmsg.GroupMetadataValue
		if err := v.ReadFrom(r.Value); err != nil {
			if len(r.Value) == 0 {
				return append(dst, "GroupMetadataValue (empty)"...), true
			}
			return append(dst, fmt.Sprintf("GroupMetadataValue (could not decode %d bytes)", len(r.Value))...), true
		}

		w := bytes.NewBuffer(dst)
		fmt.Fprintf(w, "GroupMetadataValue(%d)\n", v.Version)

		tw := out.BeginTabWriteTo(w)
		fmt.Fprintf(tw, "\tProtocolType\t%s\n", v.ProtocolType)
		fmt.Fprintf(tw, "\tGeneration\t%d\n", v.Generation)
		if v.Protocol == nil {
			fmt.Fprintf(tw, "\tProtocol\t%s\n", "(null)")
		} else {
			fmt.Fprintf(tw, "\tProtocol\t%s\n", *v.Protocol)
		}
		if v.Leader == nil {
			fmt.Fprintf(tw, "\tLeader\t%s\n", "(null)")
		} else {
			fmt.Fprintf(tw, "\tLeader\t%s\n", *v.Leader)
		}

		if v.Version >= 2 {
			fmt.Fprintf(tw, "\tStateTimestamp\t%d\n", v.CurrentStateTimestamp)
		}

		fmt.Fprintf(tw, "\tMembers\t\n")
		tw.Flush()

		for _, member := range v.Members {
			tw = out.BeginTabWriteTo(w)
			fmt.Fprintf(tw, "\t\tMemberID\t%s\n", member.MemberID)
			if v.Version >= 3 {
				if member.InstanceID == nil {
					fmt.Fprintf(tw, "\t\tInstanceID\t%s\n", "(null)")
				} else {
					fmt.Fprintf(tw, "\t\tInstanceID\t%d\n", *member.InstanceID)
				}
			}
			fmt.Fprintf(tw, "\t\tClientID\t%s\n", member.ClientID)
			fmt.Fprintf(tw, "\t\tClientHost\t%s\n", member.ClientHost)
			fmt.Fprintf(tw, "\t\tRebalanceTimeout\t%d\n", member.RebalanceTimeoutMillis)
			fmt.Fprintf(tw, "\t\tSessionTimeout\t%d\n", member.SessionTimeoutMillis)

			if v.ProtocolType != "consumer" {
				fmt.Fprintf(tw, "\t\tSubscription\t(unknown proto type) %x\n", member.Subscription)
				fmt.Fprintf(tw, "\t\tAssignment\t(unknown proto type) %x\n", member.Assignment)
				continue
			}

			var m kmsg.GroupMemberMetadata
			if err := m.ReadFrom(member.Subscription); err != nil {
				fmt.Fprintf(tw, "\t\tSubscription\t(could not decode %d bytes)\n", len(member.Subscription))
			} else {
				fmt.Fprintf(tw, "\t\tSubscription\t\n")
				fmt.Fprintf(tw, "\t\t      Version\t%d\n", m.Version)
				sort.Strings(m.Topics)
				fmt.Fprintf(tw, "\t\t      Topics\t%v\n", m.Topics)
				if v.Protocol != nil && (*v.Protocol == "sticky" || *v.Protocol == "cooperative-sticky") {
					var s kmsg.StickyMemberMetadata
					if err := s.ReadFrom(m.UserData); err != nil {
						fmt.Fprintf(tw, "\t\t      UserData\t(could not read sticky user data)\n")
					} else {
						var sb strings.Builder
						fmt.Fprintf(&sb, "gen %d, current assignment:", s.Generation)
						sort.Slice(s.CurrentAssignment, func(i, j int) bool { return s.CurrentAssignment[i].Topic < s.CurrentAssignment[j].Topic })
						for _, topic := range s.CurrentAssignment {
							sort.Slice(topic.Partitions, func(i, j int) bool { return topic.Partitions[i] < topic.Partitions[j] })
							fmt.Fprintf(&sb, " %s=>%v", topic.Topic, topic.Partitions)
						}
						fmt.Fprintf(tw, "\t\t      UserData\t%s\n", sb.String())
					}
				} else {
					fmt.Fprintf(tw, "\t\t b64 UserData\t%s\n", base64.RawStdEncoding.EncodeToString(m.UserData))
				}
				if m.Version == 1 {
					var sb strings.Builder
					sort.Slice(m.OwnedPartitions, func(i, j int) bool { return m.OwnedPartitions[i].Topic < m.OwnedPartitions[j].Topic })
					for _, topic := range m.OwnedPartitions {
						sort.Slice(topic.Partitions, func(i, j int) bool { return topic.Partitions[i] < topic.Partitions[j] })
						fmt.Fprintf(&sb, "%s=>%v ", topic.Topic, topic.Partitions)
					}
					fmt.Fprintf(tw, "\t\t      OwnedPartitions\t%s\n", sb.String())
				}
			}
			var a kmsg.GroupMemberAssignment
			if err := a.ReadFrom(member.Assignment); err != nil {
				fmt.Fprintf(tw, "\t\tAssignment\t(could not decode %d bytes)\n", len(member.Assignment))
			} else {
				fmt.Fprintf(tw, "\t\tAssignment\t\n")
				fmt.Fprintf(tw, "\t\t      Version\t%d\n", a.Version)
				var sb strings.Builder
				for _, topic := range a.Topics {
					sort.Slice(topic.Partitions, func(i, j int) bool {
						return topic.Partitions[i] < topic.Partitions[j]
					})
					fmt.Fprintf(&sb, "%s=>%v ", topic.Topic, topic.Partitions)
				}
				fmt.Fprintf(tw, "\t\t      Assigned\t%s\n", sb.String())
			}
			tw.Flush()
			fmt.Fprintln(w)
		}

		return w.Bytes(), true
	}
}
