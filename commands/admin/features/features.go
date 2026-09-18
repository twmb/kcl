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
	updateHeaders   = []string{"FEATURE", "FROM", "TO", "ERROR", "MESSAGE"}
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
			resp, err := apiVersions(cl)
			if err != nil {
				return err
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

// apiVersions asks one broker for its ApiVersions, which carries the
// features it supports and the levels the cluster has finalized.
func apiVersions(cl *client.Client) (*kmsg.ApiVersionsResponse, error) {
	req := &kmsg.ApiVersionsRequest{
		ClientSoftwareName:    "kcl",
		ClientSoftwareVersion: "v0.0.0",
	}
	kresp, err := cl.Client().Request(context.Background(), req)
	if err != nil {
		return nil, fmt.Errorf("unable to request api versions: %v", err)
	}
	resp := kresp.(*kmsg.ApiVersionsResponse)
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return nil, fmt.Errorf("%v", err)
	}
	return resp, nil
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
		dryRun         bool
		upgradeType    string
		releaseVersion string
	)

	cmd := &cobra.Command{
		Use:   "update [FEATURE=VERSION...]",
		Short: "Update cluster feature flags (Kafka 3.3+).",
		Long: `Update cluster feature flags (Kafka 3.3+).

This command updates finalized feature flags. Each argument must be of the
form FEATURE=VERSION, where VERSION is the new max version level for the
feature. Set VERSION to 0 to delete a feature flag.

--release-version RELEASE updates every feature to the level a new cluster
of that Kafka release is formatted with, the way kafka-features.sh upgrade
--release-version does: 4.4 updates metadata.version, group.version,
transaction.version, and the rest to their 4.4 levels. RELEASE is major and
minor, with or without a patch or a leading v (4.4, 4.4.0, v4.4), and must
be a release kcl knows. It stands in for the FEATURE=VERSION arguments;
giving both is an error.

--upgrade-type controls whether downgrades are permitted (v1+ of the API):
  upgrade           only allow version increases (default)
  safe-downgrade    allow lossless downgrades
  unsafe-downgrade  allow lossy downgrades

The result prints one row per feature: FROM is the level the cluster has
finalized today, 0 for a feature it has not enabled and unknown when the
broker has not learned the cluster's levels yet, and TO is the level
requested, with ERROR and MESSAGE. --dry-run validates the request without
applying it; the rows then carry what the validation answered. A broker
answering UpdateFeatures v2 reports only a request-wide error, so on success
every feature prints as OK and on failure the command errors as a whole.

EXAMPLES:
  kcl cluster features update metadata.version=17
  kcl cluster features update metadata.version=16 --upgrade-type safe-downgrade --dry-run
  kcl cluster features update --release-version 4.4 --dry-run

SEE ALSO:
  kcl cluster features describe    describe feature versions
`,
		Args: cobra.ArbitraryArgs,
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

			type update struct {
				feature string
				level   int16
			}
			var updates []update
			switch {
			case releaseVersion != "" && len(args) > 0:
				return out.Errf(out.ExitUsage, "--release-version stands in for the FEATURE=VERSION arguments; give one or the other")
			case releaseVersion != "":
				vs := kversion.FromString(releaseVersion)
				if vs == nil {
					return out.Errf(out.ExitUsage, "unknown release %q: the newest kcl knows is %s", releaseVersion, newestRelease())
				}
				vs.EachFinalizedFeature(func(name string, level int16) {
					updates = append(updates, update{name, level})
				})
				if len(updates) == 0 {
					return out.Errf(out.ExitUsage, "release %s finalizes no features; 3.3 is the first release that does", releaseVersion)
				}
			case len(args) == 0:
				return out.Errf(out.ExitUsage, "requires FEATURE=VERSION arguments or --release-version")
			default:
				for _, arg := range args {
					parts := strings.SplitN(arg, "=", 2)
					if len(parts) != 2 {
						return out.Errf(out.ExitUsage, "invalid argument %q: expected FEATURE=VERSION", arg)
					}
					version, err := strconv.ParseInt(parts[1], 10, 16)
					if err != nil {
						return out.Errf(out.ExitUsage, "invalid version in %q: %v", arg, err)
					}
					updates = append(updates, update{parts[0], int16(version)})
				}
			}

			avResp, err := apiVersions(cl)
			if err != nil {
				return err
			}
			from := finalizedLevels(avResp)

			req := kmsg.NewPtrUpdateFeaturesRequest()
			req.TimeoutMillis = cl.TimeoutMillis()
			req.ValidateOnly = dryRun
			for _, u := range updates {
				req.FeatureUpdates = append(req.FeatureUpdates, kmsg.UpdateFeaturesRequestFeatureUpdate{
					Feature:         u.feature,
					MaxVersionLevel: u.level,
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
				return out.BrokerErr(err, resp.ErrorMessage)
			}

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results", updateHeaders...).ResultColumns()
			table.SetDryRun(dryRun)
			for _, r := range resultRows(req, resp, from) {
				table.Row(r...)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, updateHeaders...)

	cmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "validate the request without applying changes")
	cmd.Flags().StringVar(&upgradeType, "upgrade-type", "upgrade", "upgrade | safe-downgrade | unsafe-downgrade")
	cmd.Flags().StringVar(&releaseVersion, "release-version", "", "update every feature to the level a new cluster of this Kafka release is formatted with (4.4, 4.4.0, v4.4), in place of FEATURE=VERSION arguments")
	return cmd
}

// newestRelease is the newest Kafka release kversion knows, as a user types
// it: the highest major.minor of VersionStrings, without its v.
func newestRelease() string {
	var newest string
	var newestMajor, newestMinor int
	for _, v := range kversion.VersionStrings() {
		v = strings.TrimPrefix(v, "v")
		var major, minor int
		if _, err := fmt.Sscanf(v, "%d.%d", &major, &minor); err != nil {
			continue
		}
		if newest == "" || major > newestMajor || major == newestMajor && minor > newestMinor {
			newest, newestMajor, newestMinor = v, major, minor
		}
	}
	return newest
}

// finalizedLevels is the level the cluster has finalized each feature at,
// as a FROM cell: 0 for a feature it has not finalized, and nil when the
// broker has not learned the cluster's levels yet (epoch -1), so that every
// FROM is unknown.
func finalizedLevels(resp *kmsg.ApiVersionsResponse) map[string]int16 {
	if resp.FinalizedFeaturesEpoch < 0 {
		return nil
	}
	levels := make(map[string]int16)
	kversion.FromApiVersionsResponse(resp).EachFinalizedFeature(func(name string, level int16) {
		levels[name] = level
	})
	return levels
}

// resultRows is one row per feature: the level the cluster is at, the level
// asked for, and the result. Through v1 the response answers each feature;
// v2 dropped the per-feature results for one request-wide error, so a v2
// response that reached here (no top-level error) means every feature in
// the request succeeded.
func resultRows(req *kmsg.UpdateFeaturesRequest, resp *kmsg.UpdateFeaturesResponse, from map[string]int16) [][]any {
	to := make(map[string]int16, len(req.FeatureUpdates))
	for _, f := range req.FeatureUpdates {
		to[f.Feature] = f.MaxVersionLevel
	}
	fromCell := func(feature string) any {
		if from == nil {
			return out.Unknown
		}
		return from[feature]
	}
	var rows [][]any
	if resp.Version >= 2 {
		for _, f := range req.FeatureUpdates {
			rows = append(rows, []any{f.Feature, fromCell(f.Feature), f.MaxVersionLevel, "", ""})
		}
		return rows
	}
	for _, r := range resp.Results {
		rows = append(rows, []any{r.Feature, fromCell(r.Feature), to[r.Feature], out.ErrName(r.ErrorCode), out.BrokerMessage(r.ErrorMessage)})
	}
	return rows
}
