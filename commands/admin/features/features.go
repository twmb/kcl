package features

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "features",
		Short: "Describe or update cluster feature flags.",
	}
	cmd.AddCommand(describeCommand(cl))
	cmd.AddCommand(updateCommand(cl))
	return cmd
}

var (
	describeHeaders = []string{"FEATURE", "SUPPORTED-MIN", "SUPPORTED-MAX", "FINALIZED", "EPOCH", "DESCRIPTION"}
	updateHeaders   = []string{"FEATURE", "ERROR", "MESSAGE"}
)

func describeCommand(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "describe",
		Short: "Describe cluster feature flags (Kafka 3.3+).",
		Long: `Describe cluster feature flags (Kafka 3.3+).

This command asks one broker for its ApiVersions and prints one row per
feature: the lowest and highest level the broker supports, the level the
cluster has finalized, the epoch the finalized levels were read at, and
what the finalized level means, with the KIP to search for.

FINALIZED is 0 for a feature the cluster has not enabled. Both FINALIZED and
EPOCH are unknown when the broker has not learned the cluster's finalized
features yet. DESCRIPTION is kcl's own wording and may change.

EXAMPLES:
  kcl cluster features describe
  kcl cluster features describe --format json

SEE ALSO:
  kcl cluster features update    update finalized feature versions
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			req := &kmsg.ApiVersionsRequest{
				ClientSoftwareName:    "kcl",
				ClientSoftwareVersion: "v0.0.0",
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to request api versions: %v", err)
			}
			resp := kresp.(*kmsg.ApiVersionsResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				return fmt.Errorf("%v", err)
			}

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "features", describeHeaders...)
			for _, row := range describeRows(resp) {
				table.Row(row...)
			}
			if err := table.Flush(); err != nil {
				return err
			}

			if len(resp.SupportedFeatures) == 0 && cl.Format() == out.FormatText {
				fmt.Fprintln(os.Stderr, "No feature flags found.")
			}
			return nil
		},
	}
	out.Columns(cmd, describeHeaders...)
	return cmd
}

// describeRows is one row per supported feature, in name order. A feature
// the cluster has not finalized is at level 0 once the broker knows the
// cluster's levels at all; before that, the level and epoch are unknown.
func describeRows(resp *kmsg.ApiVersionsResponse) [][]any {
	vs := kversion.FromApiVersionsResponse(resp)
	finalized := make(map[string]int16)
	vs.EachFinalizedFeature(func(name string, level int16) {
		finalized[name] = level
	})
	known := resp.FinalizedFeaturesEpoch >= 0

	var rows [][]any
	vs.EachSupportedFeature(func(name string, min, max int16) {
		var level, epoch, description any = out.Unknown, out.Unknown, out.Unknown
		if known {
			level = finalized[name]
			epoch = resp.FinalizedFeaturesEpoch
			description = kversion.FeatureLevelDescription(name, finalized[name])
		}
		rows = append(rows, []any{name, min, max, level, epoch, description})
	})
	return rows
}

func updateCommand(cl *client.Client) *cobra.Command {
	var (
		dryRun      bool
		upgradeType string
	)

	cmd := &cobra.Command{
		Use:   "update FEATURE=VERSION...",
		Short: "Update cluster feature flags (Kafka 3.3+).",
		Long: `Update cluster feature flags (Kafka 3.3+).

This command updates finalized feature flags. Each argument must be of the
form FEATURE=VERSION, where VERSION is the new max version level for the
feature. Set VERSION to 0 to delete a feature flag.

--upgrade-type controls whether downgrades are permitted (v1+ of the API):
  upgrade           only allow version increases (default)
  safe-downgrade    allow lossless downgrades
  unsafe-downgrade  allow lossy downgrades

The result prints one row per feature with ERROR and MESSAGE. --dry-run
validates the request without applying it; the rows then carry what the
validation answered. A broker answering UpdateFeatures v2 reports only a
request-wide error, so on success every feature prints as OK and on failure
the command errors as a whole.

EXAMPLES:
  kcl cluster features update metadata.version=17
  kcl cluster features update metadata.version=16 --upgrade-type safe-downgrade --dry-run

SEE ALSO:
  kcl cluster features describe    describe feature versions
`,
		Args: cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			var upgrade int8
			switch upgradeType {
			case "upgrade", "":
				upgrade = 1
			case "safe-downgrade":
				upgrade = 2
			case "unsafe-downgrade":
				upgrade = 3
			default:
				return out.Errf(out.ExitUsage, "invalid --upgrade-type %q: want upgrade, safe-downgrade, unsafe-downgrade", upgradeType)
			}

			req := kmsg.NewPtrUpdateFeaturesRequest()
			req.TimeoutMillis = cl.TimeoutMillis()
			req.ValidateOnly = dryRun

			for _, arg := range args {
				parts := strings.SplitN(arg, "=", 2)
				if len(parts) != 2 {
					return out.Errf(out.ExitUsage, "invalid argument %q: expected FEATURE=VERSION", arg)
				}
				version, err := strconv.ParseInt(parts[1], 10, 16)
				if err != nil {
					return out.Errf(out.ExitUsage, "invalid version in %q: %v", arg, err)
				}
				req.FeatureUpdates = append(req.FeatureUpdates, kmsg.UpdateFeaturesRequestFeatureUpdate{
					Feature:         parts[0],
					MaxVersionLevel: int16(version),
					UpgradeType:     upgrade,
					AllowDowngrade:  upgrade >= 2, // v0 fallback
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to update features: %v", err)
			}
			resp := kresp.(*kmsg.UpdateFeaturesResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				additional := ""
				if resp.ErrorMessage != nil {
					additional = ": " + *resp.ErrorMessage
				}
				return fmt.Errorf("%s%s", err, additional)
			}

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results", updateHeaders...).ResultColumns()
			table.SetDryRun(dryRun)
			for _, r := range resultRows(req, resp) {
				table.Row(r...)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, updateHeaders...)

	cmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "validate the request without applying changes")
	cmd.Flags().StringVar(&upgradeType, "upgrade-type", "upgrade", "upgrade | safe-downgrade | unsafe-downgrade")
	return cmd
}

// resultRows is one row per feature. Through v1 the response answers each
// feature; v2 dropped the per-feature results for one request-wide error,
// so a v2 response that reached here (no top-level error) means every
// feature in the request succeeded.
func resultRows(req *kmsg.UpdateFeaturesRequest, resp *kmsg.UpdateFeaturesResponse) [][]any {
	var rows [][]any
	if resp.Version >= 2 {
		for _, f := range req.FeatureUpdates {
			rows = append(rows, []any{f.Feature, "", ""})
		}
		return rows
	}
	for _, r := range resp.Results {
		rows = append(rows, []any{r.Feature, out.ErrName(r.ErrorCode), out.BrokerMessage(r.ErrorMessage)})
	}
	return rows
}
