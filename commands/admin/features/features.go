package features

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

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

func describeCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "describe",
		Short: "Describe cluster feature flags (Kafka 3.3+).",
		Long: `Describe cluster feature flags (Kafka 3.3+).

This command uses the ApiVersions response to print supported feature
version ranges and finalized feature version ranges.
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

			table := out.NewFormattedTable(cl.Format(), "features.describe", 1, "features",
				"KIND", "NAME", "MIN-VERSION", "MAX-VERSION")
			for _, f := range resp.SupportedFeatures {
				table.Row("SUPPORTED", f.Name, f.MinVersion, f.MaxVersion)
			}
			for _, f := range resp.FinalizedFeatures {
				table.Row("FINALIZED", f.Name, f.MinVersionLevel, f.MaxVersionLevel)
			}
			table.Flush()

			if len(resp.SupportedFeatures) == 0 && len(resp.FinalizedFeatures) == 0 {
				fmt.Println("No feature flags found.")
			}
			return nil
		},
	}
}

func updateCommand(cl *client.Client) *cobra.Command {
	var run, validateOnly bool

	cmd := &cobra.Command{
		Use:   "update FEATURE=VERSION...",
		Short: "Update cluster feature flags (Kafka 3.3+).",
		Long: `Update cluster feature flags (Kafka 3.3+).

This command updates finalized feature flags. Each argument must be of the
form FEATURE=VERSION, where VERSION is the new max version level for the
feature. Set VERSION to 0 to delete a feature flag.

To avoid accidental updates, this command requires a --run flag to run.
`,
		Example: "update --run metadata.version=17",
		Args:    cobra.MinimumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			if !run {
				return fmt.Errorf("use --run to actually run this command")
			}

			req := kmsg.NewPtrUpdateFeaturesRequest()
			req.TimeoutMillis = cl.TimeoutMillis()
			req.ValidateOnly = validateOnly

			for _, arg := range args {
				parts := strings.SplitN(arg, "=", 2)
				if len(parts) != 2 {
					return fmt.Errorf("invalid argument %q: expected FEATURE=VERSION", arg)
				}
				version, err := strconv.ParseInt(parts[1], 10, 16)
				if err != nil {
					return fmt.Errorf("invalid version in %q: %v", arg, err)
				}
				req.FeatureUpdates = append(req.FeatureUpdates, kmsg.UpdateFeaturesRequestFeatureUpdate{
					Feature:         parts[0],
					MaxVersionLevel: int16(version),
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

			table := out.NewFormattedTable(cl.Format(), "features.update", 1, "results",
				"FEATURE", "ERROR", "MESSAGE")
			for _, result := range resp.Results {
				var errStr, msg string
				if err := kerr.ErrorForCode(result.ErrorCode); err != nil {
					errStr = err.Error()
				}
				if result.ErrorMessage != nil {
					msg = *result.ErrorMessage
				}
				table.Row(result.Feature, errStr, msg)
			}
			table.Flush()
			return nil
		},
	}

	cmd.Flags().BoolVar(&run, "run", false, "actually run the command (avoids accidental updates without this flag)")
	cmd.Flags().BoolVar(&validateOnly, "validate-only", false, "validate the request without applying changes (Kafka 3.3+)")
	return cmd
}
