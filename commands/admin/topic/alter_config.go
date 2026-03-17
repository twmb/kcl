package topic

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func topicAlterConfigCommand(cl *client.Client) *cobra.Command {
	var (
		setKVs    []string
		deleteKVs []string
		appendKVs []string
		subtKVs   []string
		dryRun    bool
	)

	cmd := &cobra.Command{
		Use:   "alter-config TOPIC",
		Short: "Alter topic configs with clean syntax.",
		Long: `Alter topic configs using IncrementalAlterConfigs (Kafka 2.3.0+).

This is a porcelain wrapper over 'config alter' with cleaner flag syntax.

EXAMPLES:
  kcl topic alter-config foo --set cleanup.policy=compact
  kcl topic alter-config foo --set retention.ms=86400000 --delete preallocate
  kcl topic alter-config foo --append cleanup.policy=compact
  kcl topic alter-config foo --dry-run --set compression.type=zstd

SEE ALSO:
  kcl config alter       generic config alter for any resource type
  kcl config describe    describe configs
`,
		Args: cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			topicName := args[0]

			req := kmsg.NewPtrIncrementalAlterConfigsRequest()
			if dryRun {
				req.ValidateOnly = true
			}

			r := kmsg.NewIncrementalAlterConfigsRequestResource()
			r.ResourceType = 2 // TOPIC
			r.ResourceName = topicName

			for _, kv := range setKVs {
				k, v, ok := strings.Cut(kv, "=")
				if !ok {
					out.Die("--set value %q must be key=value", kv)
				}
				c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
				c.Name = k
				c.Value = kmsg.StringPtr(v)
				c.Op = 0 // SET
				r.Configs = append(r.Configs, c)
			}
			for _, k := range deleteKVs {
				c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
				c.Name = k
				c.Op = 1 // DELETE
				r.Configs = append(r.Configs, c)
			}
			for _, kv := range appendKVs {
				k, v, ok := strings.Cut(kv, "=")
				if !ok {
					out.Die("--append value %q must be key=value", kv)
				}
				c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
				c.Name = k
				c.Value = kmsg.StringPtr(v)
				c.Op = 2 // APPEND
				r.Configs = append(r.Configs, c)
			}
			for _, kv := range subtKVs {
				k, v, ok := strings.Cut(kv, "=")
				if !ok {
					out.Die("--subtract value %q must be key=value", kv)
				}
				c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
				c.Name = k
				c.Value = kmsg.StringPtr(v)
				c.Op = 3 // SUBTRACT
				r.Configs = append(r.Configs, c)
			}

			if len(r.Configs) == 0 {
				out.Die("no config changes specified; use --set, --delete, --append, or --subtract")
			}

			req.Resources = append(req.Resources, r)

			kresp, err := req.RequestWith(context.Background(), cl.Client())
			out.MaybeDie(err, "unable to alter configs: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			for _, r := range kresp.Resources {
				if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
					msg := err.Error()
					if r.ErrorMessage != nil {
						msg += ": " + *r.ErrorMessage
					}
					fmt.Fprintf(out.BeginTabWrite(), "%s: %s\n", r.ResourceName, msg)
				} else {
					if dryRun {
						fmt.Printf("%s: OK (dry run)\n", r.ResourceName)
					} else {
						fmt.Printf("%s: OK\n", r.ResourceName)
					}
				}
			}
		},
	}

	cmd.Flags().StringArrayVarP(&setKVs, "set", "s", nil, "set config key=value (repeatable)")
	cmd.Flags().StringArrayVarP(&deleteKVs, "delete", "d", nil, "delete config key (repeatable)")
	cmd.Flags().StringArrayVar(&appendKVs, "append", nil, "append to list config key=value (repeatable)")
	cmd.Flags().StringArrayVar(&subtKVs, "subtract", nil, "subtract from list config key=value (repeatable)")
	cmd.Flags().BoolVar(&dryRun, "dry-run", false, "validate without applying")

	return cmd
}
