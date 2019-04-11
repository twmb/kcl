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

	var configKVs string

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
			"--kvs",
			"--validate",
		},
		Run: func(_ *cobra.Command, args []string) {
			kvs, err := parseKVs(configKVs)
			maybeDie(err, "unable to parse KVs: %v", err)

			for _, kv := range kvs {
				cfg.ConfigEntries = append(cfg.ConfigEntries,
					kmsg.AlterConfigsRequestResourceConfigEntry{
						ConfigName:  kv.k,
						ConfigValue: kmsg.StringPtr(kv.v),
					})
			}

			var r requestor = client

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
						r = client.Broker(bid)
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

	cmd.Flags().StringVar(&configKVs, "kvs", "", "comma delimited list of key value config pairs to alter (e.g. cleanup.policy=compact,preallocate=true)")
	cmd.Flags().BoolVarP(&req.ValidateOnly, "validate", "v", false, "(v)alidate the config alter request, but do not apply (dry run)")

	return cmd
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

func describeConfig(maybeName []string, isBroker bool) {
	name := ""
	if len(maybeName) == 1 {
		name = maybeName[0]
	}

	resourceType := int8(2)
	var r requestor = client

	if isBroker {
		resourceType = 4
		if name != "" {
			if bid, err := strconv.Atoi(name); err != nil {
				die("unable to parse broker ID: %v", err)
			} else {
				r = client.Broker(bid)
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

	if asJSON {
		dumpJSON(kresp)
		return
	}

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
