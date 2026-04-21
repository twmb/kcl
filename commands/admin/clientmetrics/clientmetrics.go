// Package clientmetrics manages client telemetry subscriptions (KIP-714).
package clientmetrics

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

const resourceTypeClientMetrics kmsg.ConfigResourceType = 16

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "client-metrics",
		Short: "Manage client telemetry subscriptions (KIP-714, Kafka 3.7+).",
	}

	cmd.AddCommand(
		listCommand(cl),
		describeCommand(cl),
		alterCommand(cl),
		deleteCommand(cl),
	)

	return cmd
}

func listCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls"},
		Short:   "List client metrics subscription resources.",
		RunE: func(_ *cobra.Command, _ []string) error {
			// Prefer ListConfigResources (KIP-1000, renamed in KIP-1142) to
			// enumerate client-metrics resources. Fall back to the older
			// DescribeConfigs-with-empty-name path for pre-4.1 brokers.
			names, err := listClientMetricsNames(cl)
			if err != nil {
				return err
			}
			table := out.NewFormattedTable(cl.Format(), "client-metrics.list", 1, "subscriptions",
				"NAME")
			for _, n := range names {
				table.Row(n)
			}
			table.Flush()
			return nil
		},
	}
}

// listClientMetricsNames returns the set of client-metrics resource
// names, preferring ListConfigResources (KIP-1000/KIP-1142) and falling
// back to DescribeConfigs with an empty name for older brokers.
func listClientMetricsNames(cl *client.Client) ([]string, error) {
	listReq := kmsg.NewPtrListConfigResourcesRequest()
	listReq.ResourceTypes = []int8{int8(resourceTypeClientMetrics)}
	if kresp, err := listReq.RequestWith(context.Background(), cl.Client()); err == nil {
		if ec := kerr.ErrorForCode(kresp.ErrorCode); ec == nil {
			var names []string
			for _, r := range kresp.ConfigResources {
				names = append(names, r.Name)
			}
			return names, nil
		}
	}

	req := kmsg.NewPtrDescribeConfigsRequest()
	r := kmsg.NewDescribeConfigsRequestResource()
	r.ResourceType = resourceTypeClientMetrics
	r.ResourceName = ""
	req.Resources = append(req.Resources, r)
	kresp, err := req.RequestWith(context.Background(), cl.Client())
	if err != nil {
		return nil, fmt.Errorf("unable to list client metrics: %v", err)
	}
	var names []string
	for _, rr := range kresp.Resources {
		if ec := kerr.ErrorForCode(rr.ErrorCode); ec != nil {
			return nil, fmt.Errorf("DescribeConfigs error: %v", ec)
		}
		names = append(names, rr.ResourceName)
	}
	return names, nil
}

func describeCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:     "describe NAME",
		Aliases: []string{"d"},
		Short:   "Describe a client metrics subscription.",
		Args:    cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			req := kmsg.NewPtrDescribeConfigsRequest()
			r := kmsg.NewDescribeConfigsRequestResource()
			r.ResourceType = resourceTypeClientMetrics
			r.ResourceName = args[0]
			req.Resources = append(req.Resources, r)

			kresp, err := req.RequestWith(context.Background(), cl.Client())
			if err != nil {
				return fmt.Errorf("unable to describe client metrics: %v", err)
			}

			for _, r := range kresp.Resources {
				if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
					msg := err.Error()
					if r.ErrorMessage != nil {
						msg += ": " + *r.ErrorMessage
					}
					return fmt.Errorf("%s", msg)
				}

				table := out.NewFormattedTable(cl.Format(), "client-metrics.describe", 1, "configs",
					"KEY", "VALUE", "SOURCE")
				for _, c := range r.Configs {
					val := ""
					if c.Value != nil {
						val = *c.Value
					}
					table.Row(c.Name, val, c.Source)
				}
				table.Flush()
			}
			return nil
		},
	}
}

func alterCommand(cl *client.Client) *cobra.Command {
	var (
		setKVs    []string
		deleteKVs []string
	)

	cmd := &cobra.Command{
		Use:   "alter NAME",
		Short: "Create or update a client metrics subscription.",
		Long: `Create or update a client metrics subscription (KIP-714).

Common config keys:
  interval.ms            push interval in milliseconds
  match                  client match selectors (key=value)
  metrics                comma-separated metric name prefixes

EXAMPLES:
  kcl client-metrics alter my-sub --set interval.ms=30000
  kcl client-metrics alter my-sub --set match=client_software_name=apache-kafka-java
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			req := kmsg.NewPtrIncrementalAlterConfigsRequest()
			r := kmsg.NewIncrementalAlterConfigsRequestResource()
			r.ResourceType = resourceTypeClientMetrics
			r.ResourceName = args[0]

			for _, kv := range setKVs {
				k, v, ok := strings.Cut(kv, "=")
				if !ok {
					return out.Errf(out.ExitUsage, "--set value %q must be key=value", kv)
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

			req.Resources = append(req.Resources, r)
			kresp, err := req.RequestWith(context.Background(), cl.Client())
			if err != nil {
				return fmt.Errorf("unable to alter client metrics: %v", err)
			}

			table := out.NewFormattedTable(cl.Format(), "client-metrics.alter", 1, "results",
				"NAME", "STATUS")
			for _, r := range kresp.Resources {
				if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
					msg := err.Error()
					if r.ErrorMessage != nil {
						msg += ": " + *r.ErrorMessage
					}
					table.Row(r.ResourceName, msg)
					continue
				}
				table.Row(r.ResourceName, "OK")
			}
			table.Flush()
			return nil
		},
	}

	cmd.Flags().StringArrayVarP(&setKVs, "set", "s", nil, "set config key=value (repeatable)")
	cmd.Flags().StringArrayVar(&deleteKVs, "delete", nil, "delete config key (repeatable)")

	return cmd
}

func deleteCommand(cl *client.Client) *cobra.Command {
	return &cobra.Command{
		Use:   "delete NAME",
		Short: "Delete a client metrics subscription.",
		Args:  cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			// Delete all configs for the resource to effectively delete the subscription.
			req := kmsg.NewPtrDescribeConfigsRequest()
			r := kmsg.NewDescribeConfigsRequestResource()
			r.ResourceType = resourceTypeClientMetrics
			r.ResourceName = args[0]
			req.Resources = append(req.Resources, r)

			kresp, err := req.RequestWith(context.Background(), cl.Client())
			if err != nil {
				return fmt.Errorf("unable to describe client metrics for deletion: %v", err)
			}

			alterReq := kmsg.NewPtrIncrementalAlterConfigsRequest()
			ar := kmsg.NewIncrementalAlterConfigsRequestResource()
			ar.ResourceType = resourceTypeClientMetrics
			ar.ResourceName = args[0]

			for _, res := range kresp.Resources {
				for _, c := range res.Configs {
					ac := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
					ac.Name = c.Name
					ac.Op = 1 // DELETE
					ar.Configs = append(ar.Configs, ac)
				}
			}

			alterReq.Resources = append(alterReq.Resources, ar)
			alterResp, err := alterReq.RequestWith(context.Background(), cl.Client())
			if err != nil {
				return fmt.Errorf("unable to delete client metrics: %v", err)
			}

			table := out.NewFormattedTable(cl.Format(), "client-metrics.delete", 1, "results",
				"NAME", "STATUS")
			for _, res := range alterResp.Resources {
				if err := kerr.ErrorForCode(res.ErrorCode); err != nil {
					msg := err.Error()
					if res.ErrorMessage != nil {
						msg += ": " + *res.ErrorMessage
					}
					table.Row(res.ResourceName, msg)
					continue
				}
				table.Row(res.ResourceName, "deleted")
			}
			table.Flush()
			return nil
		},
	}
}
