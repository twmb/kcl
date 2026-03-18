package cluster

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func addControllerCommand(cl *client.Client) *cobra.Command {
	var (
		controllerID    int32
		directoryID     string
		listeners       []string
	)

	cmd := &cobra.Command{
		Use:   "add-controller",
		Short: "Add a voter to the KRaft quorum (KIP-853).",
		Long: `Add a voter (controller) to the KRaft quorum (KIP-853, Kafka 4.0+).

Only one controller can be added at a time. The new controller must be
running and reachable via the specified listeners.

EXAMPLES:
  kcl cluster add-controller --controller-id 3 --directory-id abc123 --listeners PLAINTEXT://host:9093
`,
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			req := kmsg.NewPtrAddRaftVoterRequest()
			req.VoterID = controllerID
			req.VoterDirectoryID = parseDirectoryID(directoryID)
			for _, l := range listeners {
				name, hostport, ok := strings.Cut(l, "://")
				if !ok {
					out.Die("invalid listener %q: expected NAME://host:port", l)
				}
				host, portStr, err := net.SplitHostPort(hostport)
				out.MaybeDie(err, "invalid listener %q: %v", l, err)
				port, err := strconv.Atoi(portStr)
				out.MaybeDie(err, "invalid port in listener %q: %v", l, err)
				req.Listeners = append(req.Listeners, kmsg.AddRaftVoterRequestListener{
					Name: name,
					Host: host,
					Port: uint16(port),
				})
			}

			kresp, err := req.RequestWith(context.Background(), cl.Client())
			out.MaybeDie(err, "unable to add controller: %v", err)

			if err := kerr.ErrorForCode(kresp.ErrorCode); err != nil {
				msg := err.Error()
				if kresp.ErrorMessage != nil {
					msg += ": " + *kresp.ErrorMessage
				}
				if cl.Format() == "json" {
					out.DieJSON("cluster.add-controller", err.Error(), msg)
				}
				out.Die(msg)
			}
			switch cl.Format() {
			case "json":
				out.MarshalJSON("cluster.add-controller", 1, map[string]any{"status": "ok"})
			default:
				fmt.Println("OK")
			}
		},
	}

	cmd.Flags().Int32Var(&controllerID, "controller-id", 0, "ID of the controller to add")
	cmd.Flags().StringVar(&directoryID, "directory-id", "", "directory ID of the controller (hex UUID)")
	cmd.Flags().StringSliceVar(&listeners, "listeners", nil, "listener addresses (NAME://host:port; repeatable)")
	cmd.MarkFlagRequired("controller-id")
	cmd.MarkFlagRequired("listeners")

	return cmd
}

func removeControllerCommand(cl *client.Client) *cobra.Command {
	var (
		controllerID int32
		directoryID  string
	)

	cmd := &cobra.Command{
		Use:   "remove-controller",
		Short: "Remove a voter from the KRaft quorum (KIP-853).",
		Long: `Remove a voter (controller) from the KRaft quorum (KIP-853, Kafka 4.0+).

Only one controller can be removed at a time.

EXAMPLES:
  kcl cluster remove-controller --controller-id 3 --directory-id abc123
`,
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			req := kmsg.NewPtrRemoveRaftVoterRequest()
			req.VoterID = controllerID
			req.VoterDirectoryID = parseDirectoryID(directoryID)

			kresp, err := req.RequestWith(context.Background(), cl.Client())
			out.MaybeDie(err, "unable to remove controller: %v", err)

			if err := kerr.ErrorForCode(kresp.ErrorCode); err != nil {
				msg := err.Error()
				if kresp.ErrorMessage != nil {
					msg += ": " + *kresp.ErrorMessage
				}
				if cl.Format() == "json" {
					out.DieJSON("cluster.remove-controller", err.Error(), msg)
				}
				out.Die(msg)
			}
			switch cl.Format() {
			case "json":
				out.MarshalJSON("cluster.remove-controller", 1, map[string]any{"status": "ok"})
			default:
				fmt.Println("OK")
			}
		},
	}

	cmd.Flags().Int32Var(&controllerID, "controller-id", 0, "ID of the controller to remove")
	cmd.Flags().StringVar(&directoryID, "directory-id", "", "directory ID of the controller (hex UUID)")
	cmd.MarkFlagRequired("controller-id")

	return cmd
}

func parseDirectoryID(s string) [16]byte {
	var id [16]byte
	if s == "" {
		return id
	}
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		out.Die("directory-id must be a 32-char hex string (UUID), got %d chars", len(s))
	}
	_, err := hex.Decode(id[:], []byte(s))
	if err != nil {
		out.Die("directory-id is not valid hex: %v", err)
	}
	return id
}

