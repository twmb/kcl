// Package acl contains acl related commands.
package acl

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "acl",
		Short: "Perform acl related actions",
		Long: `Perform acl related actions.

ACLs are one of the most undocumented aspects of Kafka.

To enable ACLs, you must set authorizer.class.name. Kafka has one out of the
box authorizer: "kafka.security.auth.SimpleAclAuthorizer".

Unless allow.everyone.if.no.acl.found is true, you need to do some initial
zookeeper setup that kcl cannot help with. Note you can also use super.users
to set admins on startup (and to get around the next paragraph if you grant
Kafka brokers super user status).

When Kafka starts up, it talks to itself and others in the cluster. They need
to be able to talk to each other before a controller is elected. kcl cannot
add ACLs for brokers themselves due to the brokers not having a controller.
Until kcl supports talking directly to zookeeper, you must use a Kafka shell
script to setup some initial ACLs.

Anything connecting over a non SASL connection has an anonymous user with
principal User:ANONYMOUS.

ACLs have two components: a resource and an entity. The resource defines what
an individual ACL is for: topics, groups, the cluster, transactional IDs, or
delegation tokens. The entity defines who what ACL is for, from where, and what
they can or cannot do.

Complicating things futher, ACL Kafka requests behave in two different ways.
The create request is the more obvious way: the request defines actual ACLs to
create. Describe and delete both work on a "filter" basis, in that any ACL
entry that matches the filter is either described or deleted.

THE RESOURCE PORTION:
In requests, a resource type has the following options:

  - UNKNOWN (what Kafka returns when it does not understand the requested type)
  - ANY     (only relevant in filters; matches anything)
  - TOPIC
  - GROUP
  - CLUSTER
  - TRANSACTIONAL_ID
  - DELEGATION_TOKEN

The resource name for topics would be topic names, for groups, group names, and
so on. For CLUSTER, the name must be "kafka-cluster". Lastly, Kafka understands
a wildcard name, "*".

Kafka 2.0.0 introduced a "resource pattern type" field in requests that allows
for changing how Kafka understands a resource name. In prior versions, Kafka
understands the name to be an exact literal match. Kafka 2.0.0 introduced a
"prefixed" match, such that anything that has the requested resource name as a
prefix is considered a match. For example, "f" matches topic "foo" and "fuzz".
Lastly, for filters, the "match" pattern type matches wildcard names, exact
matches, and prefixed matches.

In summary, the resource pattern type can be MATCH, LITERAL, or PREFIXED,
with Kafka replying with UNKNOWN when you use one that does not exist.

THE ENTITY PORTION:
Entities are made up of a principal, host, operation, and permission.

The principal is the user being matched, e.g. "User:admin". The wildcard
principal is "User:*". Host is equally simple: this is simply an exact host to
match, or the wildcard "*".

Operation is what "operation" is allowed. Valid operations are:

  - UNKNOWN (what Kafka returns when it does not understand the requested operation)
  - ANY     (only relevant in filters; matches anything)
  - ALL     (allows anything)
  - READ
  - WRITE
  - CREATE
  - DELETE
  - ALTER
  - DESCRIBE
  - CLUSTER_ACTION
  - DESCRIBE_CONFIGS
  - ALTER_CONFIGS
  - IDEMPOTENT_WRITE

Note that READ, WRITE, DELETE, and ALTER imply DESCRIBE, and ALTER_CONFIGS
implies DESCRIBE_CONFIGS.

Different resource types have different potential operations:

  - TOPIC can have READ, WRITE, CREATE, DESCRIBE,
                   DELETE, ALTER, DESCRIBE_CONFIGS,
                   and ALTER_CONFIGS

  - GROUP can have READ, DESCRIBE, and DELETE

  - CLUSTER can have CREATE, CLUSTER_ACTION, DESCRIBE, ALTER,
                     DESCRIBE_CONFIGS, ALTER_CONFIGS, and
                     IDEMPOTENT_WRITE.

  - TRANSACTIONAL_ID can have DESCRIBE and WRITE

  - DELEGATION_TOKEN can have DESCRIBE

Lastly, the permission type specifies whether an entity (user) is allowed to do
the operation; this is either DENY or ALLOW. Filters can also use ANY, and
Kafka replies to unknown permissions with UNKNOWN.

USAGE
For resource types, pattern types, operations, and permissions, kcl accepts any
casing and any underscores or periods.

Note that if combining with delegation tokens, you do not create ACLs for the
delegation token ID. The principal of the client using the token is the same
as the principal of the user that created the token.
`,
		Args: cobra.ExactArgs(0),
	}

	cmd.AddCommand(
		describeCommand(cl),
		createCommand(cl),
		deleteCommand(cl),
	)

	return cmd
}

func describeCommand(cl *client.Client) *cobra.Command {
	var (
		resourceType    string
		resourceName    string
		resourcePattern string
		principal       string
		host            string
		operation       string
		permission      string
	)

	cmd := &cobra.Command{
		Use:     "describe",
		Aliases: []string{"d"},
		Short:   "Describe ACLs.",
		Long: `Describe ACLs on a filter basis (Kafka 0.11.0+).

Describing ACLs works on a filter basis: anything matching the requested filter
is described. Note that for resource names, principals, and hosts, using a
wildcard matches ACLs with wildcards; to match everything, leave the principal
and host empty.

For more detailed information about ACLs, read kcl acl --help.
`,

		Example: "describe --type any --pattern match --op any --perm any // matches all",
		Args:    cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			var pname, pprincipal, phost *string
			if resourceName != "" {
				pname = &resourceName
			}
			if principal != "" {
				pprincipal = &principal
			}
			if host != "" {
				phost = &host
			}

			req := &kmsg.DescribeACLsRequest{
				ResourceType:        atoiResourceType(resourceType),
				ResourceName:        pname,
				ResourcePatternType: atoiResourcePattern(resourcePattern),
				Principal:           pprincipal,
				Host:                phost,
				Operation:           atoiOperation(operation),
				PermissionType:      atoiPermission(permission),
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to describe acls: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}
			resp := kresp.(*kmsg.DescribeACLsResponse)
			out.MaybeExitErrMsg(resp.ErrorCode, resp.ErrorMessage)

			tw := out.BeginTabWrite()
			defer tw.Flush()

			fmt.Fprintf(tw, "TYPE\tNAME\tPATTERN\tPRINCIPAL\tHOST\tOPERATION\tPERMISSION\tERROR\tERROR MESSAGE\n")

			for _, resource := range resp.Resources {
				for _, acl := range resource.ACLs {
					fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
						resource.ResourceType,
						resource.ResourceName,
						resource.ResourcePatternType,
						acl.Principal,
						acl.Host,
						acl.Operation,
						acl.PermissionType,
					)
				}
			}
		},
	}

	cmd.Flags().StringVar(&resourceType, "type", "", "resource type filter; any matches all")
	cmd.Flags().StringVar(&resourceName, "name", "", "resource name filter; empty matches all")
	cmd.Flags().StringVar(&resourcePattern, "pattern", "match", "resource name pattern filter; match means all (Kafka 2.0.0+)")
	cmd.Flags().StringVar(&principal, "principal", "", "principal filter; empty matches all")
	cmd.Flags().StringVar(&host, "host", "", "host filter; empty matches all")
	cmd.Flags().StringVar(&operation, "op", "any", "operation filter; any matches all")
	cmd.Flags().StringVar(&permission, "perm", "any", "permission filter; any matches all")

	return cmd
}

func createCommand(cl *client.Client) *cobra.Command {
	var (
		types      []string
		names      []string
		pattern    string
		principals []string
		hosts      []string
		operations []string
		permission string
	)

	cmd := &cobra.Command{
		Use:     "create",
		Aliases: []string{"c"},
		Short:   "Create ACLs.",
		Long: `Create ACLs on a combinatorial basis (Kafka 0.11.0+).

In a request to Kafka, creating ACLs works on an individual basis: one ACL
creation is one ACL entry. This command, however, works on a combinatorial
basis: everything multiplies with each other to create many ACLs in one go.

To create the same ACL for two principals, just pass the principal flag twice.
To give both of those principals access to multiple resource names, pass the
name flag multiple times. And so on (minus the pattern, and permission flags,
which each can only occur once). Note that some combinations do not necessarily
make sense; those will fail.

This command will not do anything unless all array flags have at least one
entry. That is, this command will not do anything until at least one ACL
creation is fully specifiable.

For more detailed information about ACLs, read kcl acl --help.
`,

		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			req := new(kmsg.CreateACLsRequest)

			// I heard you liked loops?
			for _, typ := range types {
				for _, name := range names {
					for _, principal := range principals {
						for _, host := range hosts {
							for _, op := range operations {
								req.Creations = append(req.Creations, kmsg.CreateACLsRequestCreation{
									ResourceType:        atoiResourceType(typ),
									ResourceName:        name,
									ResourcePatternType: atoiResourcePattern(pattern),
									Principal:           principal,
									Host:                host,
									Operation:           atoiOperation(op),
									PermissionType:      atoiPermission(permission),
								})
							}
						}
					}
				}
			}

			if len(req.Creations) == 0 {
				out.Die("no ACL creations requested")
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to describe acls: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}
			resp := kresp.(*kmsg.CreateACLsResponse)

			if len(resp.Results) != len(req.Creations) {
				fmt.Fprintf(os.Stderr, "Kafka replied with only %d responses to our %d creations! Dumping response as JSON...",
					len(resp.Results), len(req.Creations))
				out.ExitJSON(kresp)
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()

			fmt.Fprintf(tw, "TYPE\tNAME\tPATTERN\tPRINCIPAL\tHOST\tOPERATION\tPERMISSION\tERROR\tERROR MSG\n")

			for i, result := range resp.Results {
				errStr, errMsg := "OK", ""
				if err := kerr.ErrorForCode(result.ErrorCode); err != nil {
					errStr = err.Error()
					if result.ErrorMessage != nil {
						errMsg = *result.ErrorMessage
					}
				}
				creation := req.Creations[i]
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					creation.ResourceType,
					creation.ResourceName,
					creation.ResourcePatternType,
					creation.Principal,
					creation.Host,
					creation.Operation,
					creation.PermissionType,
					errStr,
					errMsg,
				)
			}
		},
	}

	cmd.Flags().StringArrayVar(&types, "type", nil, "resource type this ACL will be for; repeatable")
	cmd.Flags().StringArrayVar(&names, "name", nil, "resource name this ACL will be for; repeatable")
	cmd.Flags().StringVar(&pattern, "pattern", "prefixed", "how the resource names are understood (Kafka 2.0.0+)")
	cmd.Flags().StringArrayVar(&principals, "principal", nil, "principal to create the ACL for; repeatable")
	cmd.Flags().StringArrayVar(&hosts, "host", nil, "host to create the ACL for; repeatable")
	cmd.Flags().StringArrayVar(&operations, "op", nil, "operation to allow or deny for the principal on this resource")
	cmd.Flags().StringVar(&permission, "perm", "allow", "permission; either allow or deny")

	return cmd
}

func deleteCommand(cl *client.Client) *cobra.Command {
	var (
		resourceType    string
		resourceName    string
		resourcePattern string
		principal       string
		host            string
		operation       string
		permission      string
	)

	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete ACLs.",
		Long: `Delete ACLs on a filter basis (Kafka 0.11.0+).

Like describing, deleting ACLs works on a filter basis: anything matching the
requested filter is described. Note that for resource names, principals, and
hosts, using a wildcard matches ACLs with wildcards; to match everything, leave
the principal and host empty.

The delete request actually allows many filters to be passed at once, but it is
a bit difficult to express that from a CLI. So, kcl only allows one filter at a
time.

For more detailed information about ACLs, read kcl acl --help.
`,

		Example: "delete --type any --pattern match --op any --perm any // removes all",
		Args:    cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			if resourceType == "" {
				out.Die("missing resource type filter")
			}
			if resourcePattern == "" {
				out.Die("missing resource pattern filter")
			}
			if operation == "" {
				out.Die("missing operation filter")
			}
			if permission == "" {
				out.Die("missing permission filter")
			}
			var pname, pprincipal, phost *string
			if resourceName != "" {
				pname = &resourceName
			}
			if principal != "" {
				pprincipal = &principal
			}
			if host != "" {
				phost = &host
			}

			req := &kmsg.DeleteACLsRequest{
				Filters: []kmsg.DeleteACLsRequestFilter{{
					ResourceType:        atoiResourceType(resourceType),
					ResourceName:        pname,
					ResourcePatternType: atoiResourcePattern(resourcePattern),
					Principal:           pprincipal,
					Host:                phost,
					Operation:           atoiOperation(operation),
					PermissionType:      atoiPermission(permission),
				}},
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to describe acls: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}
			resp := kresp.(*kmsg.DeleteACLsResponse)

			tw := out.BeginTabWrite()
			defer tw.Flush()

			if len(resp.Results) != 1 {
				out.Die("we requested one filter, but got %d responses; dumping JSON", len(resp.Results))
				out.ExitJSON(resp)
			}

			result := resp.Results[0]
			out.MaybeExitErrMsg(result.ErrorCode, result.ErrorMessage)

			fmt.Fprintf(tw, "TYPE\tNAME\tPATTERN\tPRINCIPAL\tHOST\tOPERATION\tPERMISSION\tERROR\tERROR MSG\n")

			for _, acl := range result.MatchingACLs {
				errStr, errMsg := "OK", ""
				if err = kerr.ErrorForCode(acl.ErrorCode); err != nil {
					errStr = err.Error()
					if acl.ErrorMessage != nil {
						errMsg = *acl.ErrorMessage
					}
				}
				fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
					acl.ResourceType,
					acl.ResourceName,
					acl.ResourcePatternType,
					acl.Principal,
					acl.Host,
					acl.Operation,
					acl.PermissionType,
					errStr,
					errMsg,
				)
			}
		},
	}

	cmd.Flags().StringVar(&resourceType, "type", "", "resource type filter; any matches all")
	cmd.Flags().StringVar(&resourceName, "name", "", "resource name filter; empty matches all")
	cmd.Flags().StringVar(&resourcePattern, "pattern", "", "resource name pattern filter; match means all (Kafka 2.0.0+)")
	cmd.Flags().StringVar(&principal, "principal", "", "principal filter; empty matches all")
	cmd.Flags().StringVar(&host, "host", "", "host filter; empty matches all")
	cmd.Flags().StringVar(&operation, "op", "", "operation filter; any matches all")
	cmd.Flags().StringVar(&permission, "perm", "", "permission filter; any matches all")

	return cmd
}

/////////////////
// STR <-> INT //
/////////////////

func atoiResourceType(s string) kmsg.ACLResourceType {
	switch client.Strnorm(s) {
	case "any":
		return 1
	case "topic":
		return 2
	case "group":
		return 3
	case "cluster":
		return 4
	case "transactionalid":
		return 5
	case "delegationtoken":
		return 6
	default:
		return 0
	}
}

func atoiResourcePattern(p string) kmsg.ACLResourcePatternType {
	switch client.Strnorm(p) {
	case "any":
		return 1
	case "match":
		return 2
	case "literal":
		return 3
	case "prefixed":
		return 4
	default:
		return 0
	}
}

func atoiOperation(o string) kmsg.ACLOperation {
	switch client.Strnorm(o) {
	case "any":
		return 1
	case "all":
		return 2
	case "read":
		return 3
	case "write":
		return 4
	case "create":
		return 5
	case "delete":
		return 6
	case "alter":
		return 7
	case "describe":
		return 8
	case "clusteraction":
		return 9
	case "describeconfigs":
		return 10
	case "alterconfigs":
		return 11
	case "idempotentwrite":
		return 12
	default:
		return 0
	}
}

func atoiPermission(t string) kmsg.ACLPermissionType {
	switch client.Strnorm(t) {
	case "any":
		return 1
	case "deny":
		return 2
	case "allow":
		return 3
	default:
		return 0
	}
}
