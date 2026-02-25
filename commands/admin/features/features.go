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
		Run: func(_ *cobra.Command, _ []string) {
			req := &kmsg.ApiVersionsRequest{
				ClientSoftwareName:    "kcl",
				ClientSoftwareVersion: "v0.0.0",
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to request api versions: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			resp := kresp.(*kmsg.ApiVersionsResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.Die("%v", err)
			}

			if len(resp.SupportedFeatures) > 0 {
				fmt.Println("SUPPORTED FEATURES:")
				tw := out.BeginTabWrite()
				fmt.Fprintf(tw, "  NAME\tMIN-VERSION\tMAX-VERSION\n")
				for _, f := range resp.SupportedFeatures {
					fmt.Fprintf(tw, "  %s\t%d\t%d\n", f.Name, f.MinVersion, f.MaxVersion)
				}
				tw.Flush()
			}

			if len(resp.FinalizedFeatures) > 0 {
				if len(resp.SupportedFeatures) > 0 {
					fmt.Println()
				}
				fmt.Printf("FINALIZED FEATURES (epoch %d):\n", resp.FinalizedFeaturesEpoch)
				tw := out.BeginTabWrite()
				fmt.Fprintf(tw, "  NAME\tMIN-VERSION\tMAX-VERSION\n")
				for _, f := range resp.FinalizedFeatures {
					fmt.Fprintf(tw, "  %s\t%d\t%d\n", f.Name, f.MinVersionLevel, f.MaxVersionLevel)
				}
				tw.Flush()
			}

			if len(resp.SupportedFeatures) == 0 && len(resp.FinalizedFeatures) == 0 {
				fmt.Println("No feature flags found.")
			}
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
		Run: func(_ *cobra.Command, args []string) {
			if !run {
				out.Die("use --run to actually run this command")
			}

			req := kmsg.NewPtrUpdateFeaturesRequest()
			req.TimeoutMillis = cl.TimeoutMillis()
			req.ValidateOnly = validateOnly

			for _, arg := range args {
				parts := strings.SplitN(arg, "=", 2)
				if len(parts) != 2 {
					out.Die("invalid argument %q: expected FEATURE=VERSION", arg)
				}
				version, err := strconv.ParseInt(parts[1], 10, 16)
				out.MaybeDie(err, "invalid version in %q: %v", arg, err)
				req.FeatureUpdates = append(req.FeatureUpdates, kmsg.UpdateFeaturesRequestFeatureUpdate{
					Feature:         parts[0],
					MaxVersionLevel: int16(version),
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to update features: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}

			resp := kresp.(*kmsg.UpdateFeaturesResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.ErrAndMsg(resp.ErrorCode, resp.ErrorMessage)
				out.Exit()
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()

			fmt.Fprintf(tw, "FEATURE\tERROR\tMESSAGE\n")
			for _, result := range resp.Results {
				var errStr, msg string
				if err := kerr.ErrorForCode(result.ErrorCode); err != nil {
					errStr = err.Error()
				}
				if result.ErrorMessage != nil {
					msg = *result.ErrorMessage
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\n", result.Feature, errStr, msg)
			}
		},
	}

	cmd.Flags().BoolVar(&run, "run", false, "actually run the command (avoids accidental updates without this flag)")
	cmd.Flags().BoolVar(&validateOnly, "validate-only", false, "validate the request without applying changes (Kafka 3.3+)")
	return cmd
}
