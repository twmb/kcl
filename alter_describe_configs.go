package main

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/spf13/cobra"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

func alterConfigsCmd(
	use string,
	short string,
	long string,
	isBroker bool,
) *cobra.Command {
	req := kmsg.AlterConfigsRequest{
		Resources: []kmsg.AlterConfigsRequestResource{
			{}, // reserve
		},
	}
	cfg := &req.Resources[0]

	var configKVs []string
	var noConfirm bool

	cmd := &cobra.Command{
		Use:   use,
		Short: short,
		Long:  long,
		Args: func() cobra.PositionalArgs {
			if isBroker {
				return cobra.MaximumNArgs(1)
			}
			return cobra.ExactArgs(1)
		}(),
		ValidArgs: []string{
			"--kv",
			"--validate",
			"--no-confirm",
		},
		Run: func(_ *cobra.Command, args []string) {
			kvs, err := parseKVs(configKVs)
			maybeDie(err, "unable to parse KVs: %v", err)

			if !noConfirm {
				confirmAlterLoss(args, isBroker, kvs)
			}

			for _, kv := range kvs {
				cfg.ConfigEntries = append(cfg.ConfigEntries,
					kmsg.AlterConfigsRequestResourceConfigEntry{
						ConfigName:  kv.k,
						ConfigValue: kmsg.StringPtr(kv.v),
					})
			}

			var r requestor = client()

			if len(args) > 0 { // topic can have no args
				cfg.ResourceName = args[0]
			}
			cfg.ResourceType = 2
			if isBroker {
				cfg.ResourceType = 4
				if cfg.ResourceName != "" {
					if bid, err := strconv.Atoi(cfg.ResourceName); err != nil {
						die("unable to parse broker ID: %v", err)
					} else {
						r = client().Broker(bid)
					}
				}
			}

			kresp, err := r.Request(&req)
			maybeDie(err, "unable to alter config: %v", err)

			if asJSON {
				dumpJSON(kresp)
				return
			}
			resps := kresp.(*kmsg.AlterConfigsResponse).Resources
			if len(resps) != 1 {
				dumpAndDie(kresp, "quitting; one alter requested but received %d responses", len(resps))
			}
			errAndMsg(resps[0].ErrorCode, resps[0].ErrorMessage)
		},
	}

	cmd.Flags().StringSliceVarP(&configKVs, "kv", "k", nil, "list of (k)ey value config parameters to set (comma separtaed or repeated flag; e.g. cleanup.policy=compact,preallocate=true)")
	cmd.Flags().BoolVarP(&req.ValidateOnly, "validate", "v", false, "(v)alidate the config alter request, but do not apply (dry run)")
	cmd.Flags().BoolVar(&noConfirm, "no-confirm", false, "skip confirmation of to-be-lost unspecified existing dynamic config keys")

	return cmd
}

func confirmAlterLoss(args []string, isBroker bool, kvs []kv) {
	existing := make(map[string]string, 10)
	_, describeResource := issueDescribeConfig(args, isBroker)
	for _, entry := range describeResource.ConfigEntries {
		switch entry.ConfigSource {
		case 4, 5: // static, default
			continue
		}
		val := "(null)"
		if entry.ConfigValue != nil {
			val = *entry.ConfigValue
		}
		if entry.IsSensitive {
			val = "(sensitive)"
		}
		existing[entry.ConfigName] = val
	}

	for _, kv := range kvs {
		delete(existing, kv.k)
	}

	if len(existing) > 0 {
		losing := make([]kv, 0, len(existing))
		for k, v := range existing {
			losing = append(losing, kv{k, v})
		}
		sort.Slice(losing, func(i, j int) bool {
			return losing[i].k < losing[j].k
		})
		fmt.Println("THIS ALTER WILL LOSE THE FOLLOWING CONFIG KEY/VALUES, IS THAT OK?")
		fmt.Println()
		for _, toLose := range losing {
			fmt.Printf("%s=%s\n", toLose.k, toLose.v)
		}
		fmt.Println()

		for {
			fmt.Print("[y]es|[n]o > ")
			var s string
			fmt.Scanf("%s", &s)
			switch s {
			case "y", "yes":
				return
			case "n", "no":
				die("aborting.")
			default:
				fmt.Printf("unrecognized input %q, valid options are y, yes, n, no\n", s)
			}
		}
	}
}

func configSourceForInt8(i int8) string {
	switch i {
	case 0:
		return "UNKNOWN"
	case 1:
		return "DYNAMIC_TOPIC_CONFIG"
	case 2:
		return "DYNAMIC_BROKER_CONFIG"
	case 3:
		return "DYNAMIC_DEFAULT_BROKER_CONFIG"
	case 4:
		return "STATIC_BROKER_CONFIG"
	case 5:
		return "DEFAULT_CONFIG"
	}
	return ""
}

// issues a describe config for a single resource and returns
// the response and that resource.
//
// This dies if the response fails or the response has an ErrorCode
// or there is not just one resource in the response.
func issueDescribeConfig(maybeName []string, isBroker bool) (
	*kmsg.DescribeConfigsResponse,
	*kmsg.DescribeConfigsResponseResource,
) {
	name := ""
	if len(maybeName) == 1 {
		name = maybeName[0]
	}

	resourceType := int8(2)
	var r requestor = client()

	if isBroker {
		resourceType = 4
		if name != "" {
			if bid, err := strconv.Atoi(name); err != nil {
				die("unable to parse broker ID: %v", err)
			} else {
				r = client().Broker(bid)
			}
		}
	}

	req := kmsg.DescribeConfigsRequest{
		Resources: []kmsg.DescribeConfigsRequestResource{
			{
				ResourceType: resourceType,
				ResourceName: name,
			},
		},
	}

	kresp, err := r.Request(&req)
	maybeDie(err, "unable to describe config: %v", err)
	resp := kresp.(*kmsg.DescribeConfigsResponse)

	if len(resp.Resources) != 1 {
		die("quitting; one resource requested but received %d", len(resp.Resources))
	}
	resource := resp.Resources[0]
	if err = kerr.ErrorForCode(resource.ErrorCode); err != nil {
		msg := ""
		if resource.ErrorMessage != nil {
			msg = " (" + *resource.ErrorMessage + ")"
		}
		die("%s%s", err, msg)
	}

	return resp, &resource
}

func describeConfig(maybeName []string, isBroker bool) {
	resp, resource := issueDescribeConfig(maybeName, isBroker)
	if asJSON {
		dumpJSON(resp)
		return
	}

	kvs := resource.ConfigEntries
	sort.Slice(kvs, func(i, j int) bool {
		return kvs[i].ConfigName < kvs[j].ConfigName
	})

	for _, kv := range kvs {
		key := kv.ConfigName
		if kv.ReadOnly {
			key += "*"
		}
		val := "(null)"
		if kv.ConfigValue != nil {
			val = *kv.ConfigValue
		}
		if kv.IsSensitive {
			val = "(sensitive)"
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t\n",
			key,
			val,
			configSourceForInt8(kv.ConfigSource),
		)
	}
	tw.Flush()
}
