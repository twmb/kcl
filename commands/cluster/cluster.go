// Package cluster contains cluster-related commands.
package cluster

import (
	"github.com/spf13/cobra"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/commands/admin"
	"github.com/twmb/kcl/commands/admin/features"
	"github.com/twmb/kcl/commands/metadata"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cluster",
		Short: "Cluster operations (info, quorum, elections, features).",
	}

	info := metadata.Command(cl)
	info.Use = "info [TOPICS]"
	info.Aliases = []string{"metadata"}
	info.Short = "Show cluster metadata"

	cmd.AddCommand(
		info,
		admin.DescribeClusterCommand(cl),
		admin.DescribeQuorumCommand(cl),
		admin.ElectLeadersCommand(cl),
		features.Command(cl),
	)

	return cmd
}
