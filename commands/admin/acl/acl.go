// Package acl contains acl related commands.
package acl

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "acl",
		Short: "Perform acl related actions.",
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
	}

	cmd.AddCommand(
		describeCommand(cl),
		createCommand(cl),
		deleteCommand(cl),
	)

	return cmd
}

// aclHeaders are the identity columns of every acl table: the resource,
// then who may do what from where.
var aclHeaders = []string{"TYPE", "NAME", "PATTERN", "PRINCIPAL", "HOST", "OPERATION", "PERMISSION"}

// aclResultHeaders are the columns of a create or delete, the identity and
// then the per-ACL result.
var aclResultHeaders = append(slices.Clone(aclHeaders), "ERROR", "MESSAGE")

// aclRow is one ACL as the tables print it, with the result of a mutation
// when there is one. A dry run leaves err and msg Unknown: nothing was
// asked, so there is no result to report.
type aclRow struct {
	typ        string
	name       string
	pattern    string
	principal  string
	host       string
	operation  string
	permission string
	err        any
	msg        any
}

func (r aclRow) identity() []any {
	return []any{r.typ, r.name, r.pattern, r.principal, r.host, r.operation, r.permission}
}

func (r aclRow) result() []any {
	return append(r.identity(), r.err, r.msg)
}

// sortACLs orders rows by principal and then by resource, so that a listing
// reads as what each principal may do.
func sortACLs(rows []aclRow) {
	slices.SortFunc(rows, func(a, b aclRow) int {
		return cmp.Or(
			strings.Compare(a.principal, b.principal),
			strings.Compare(a.typ, b.typ),
			strings.Compare(a.name, b.name),
			strings.Compare(a.pattern, b.pattern),
			strings.Compare(a.host, b.host),
			strings.Compare(a.operation, b.operation),
			strings.Compare(a.permission, b.permission),
		)
	})
}

// aclFilter is the one filter list and delete send, as the flags spell it.
type aclFilter struct {
	resourceType    string
	resourceName    string
	resourcePattern string
	principal       string
	host            string
	operation       string
	permission      string
}

func (f aclFilter) validate() error {
	return validateFilters(f.resourceType, f.resourcePattern, f.operation, f.permission)
}

func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func (f aclFilter) describeRequest() *kmsg.DescribeACLsRequest {
	return &kmsg.DescribeACLsRequest{
		ResourceType:        atoiResourceType(f.resourceType),
		ResourceName:        optional(f.resourceName),
		ResourcePatternType: atoiResourcePattern(f.resourcePattern),
		Principal:           optional(f.principal),
		Host:                optional(f.host),
		Operation:           atoiOperation(f.operation),
		PermissionType:      atoiPermission(f.permission),
	}
}

func (f aclFilter) deleteRequest() *kmsg.DeleteACLsRequest {
	return &kmsg.DeleteACLsRequest{
		Filters: []kmsg.DeleteACLsRequestFilter{{
			ResourceType:        atoiResourceType(f.resourceType),
			ResourceName:        optional(f.resourceName),
			ResourcePatternType: atoiResourcePattern(f.resourcePattern),
			Principal:           optional(f.principal),
			Host:                optional(f.host),
			Operation:           atoiOperation(f.operation),
			PermissionType:      atoiPermission(f.permission),
		}},
	}
}

// describe asks for every ACL the filter matches, sorted.
func (f aclFilter) describe(cl *client.Client) ([]aclRow, error) {
	kresp, err := cl.Client().Request(context.Background(), f.describeRequest())
	if err != nil {
		return nil, fmt.Errorf("unable to describe acls: %v", err)
	}
	resp := kresp.(*kmsg.DescribeACLsResponse)
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return nil, fmt.Errorf("%s%s", err, brokerMessage(resp.ErrorMessage))
	}
	rows := []aclRow{}
	for _, resource := range resp.Resources {
		for _, acl := range resource.ACLs {
			rows = append(rows, aclRow{
				typ:        resource.ResourceType.String(),
				name:       resource.ResourceName,
				pattern:    resource.ResourcePatternType.String(),
				principal:  acl.Principal,
				host:       acl.Host,
				operation:  acl.Operation.String(),
				permission: acl.PermissionType.String(),
			})
		}
	}
	sortACLs(rows)
	return rows, nil
}

// brokerMessage is ": " and the message a broker attached to an error, or
// nothing.
func brokerMessage(msg *string) string {
	if msg == nil || *msg == "" {
		return ""
	}
	return ": " + *msg
}

// errorCells are the ERROR and MESSAGE cells for a per-ACL result: the
// error name and the message the broker attached, or "" and "" on success.
func errorCells(code int16, msg *string) (string, string) {
	if code == 0 {
		return "", ""
	}
	var m string
	if msg != nil {
		m = *msg
	}
	return kerr.TypedErrorForCode(code).Message, m
}

// filterFlags installs the filter flags list and delete share. The generic
// --type, --name, --principal, and --host flags are the filter form of an
// ACL: each defaults to matching everything, unlike create, where a
// resource flag names the one resource an ACL is created for.
func filterFlags(cmd *cobra.Command, f *aclFilter) {
	cmd.Flags().StringVar(&f.resourceType, "type", "any", "resource type to match; any matches every type")
	cmd.Flags().StringVar(&f.resourceName, "name", "", "resource name to match; empty matches every name")
	cmd.Flags().StringVar(&f.resourcePattern, "pattern", "match", "resource pattern type to match; match means all (Kafka 2.0.0+)")
	cmd.Flags().StringVar(&f.principal, "principal", "", "principal to match; empty matches every principal")
	cmd.Flags().StringVar(&f.host, "host", "", "host to match; empty matches every host")
	cmd.Flags().StringVar(&f.operation, "operation", "any", "operation to match; any matches every operation (alias: --op)")
	cmd.Flags().StringVar(&f.operation, "op", "any", "")
	cmd.Flags().StringVar(&f.permission, "permission", "any", "permission to match; any matches allow and deny (alias: --perm)")
	cmd.Flags().StringVar(&f.permission, "perm", "any", "")
	cmd.Flags().MarkHidden("op")
	cmd.Flags().MarkHidden("perm")
	registerCompletions(cmd, map[string][]string{
		"type":       resourceTypeValues,
		"pattern":    patternValues,
		"operation":  operationValues,
		"permission": permissionValues,
	})

	// The resource flags are shortcuts for --type and --name.
	var topicFlag, groupFlag, txnIDFlag, dtokenFlag string
	cmd.Flags().StringVarP(&topicFlag, "topic", "t", "", "match ACLs for this topic (--type topic --name TOPIC)")
	cmd.Flags().StringVarP(&groupFlag, "group", "g", "", "match ACLs for this group (--type group --name GROUP)")
	cmd.Flags().BoolVar(new(bool), "cluster", false, "match cluster ACLs (--type cluster)")
	cmd.Flags().StringVar(&txnIDFlag, "transactional-id", "", "match ACLs for this transactional ID")
	cmd.Flags().StringVar(&dtokenFlag, "delegation-token", "", "match ACLs for this delegation token")
	cmd.PreRunE = func(_ *cobra.Command, _ []string) error {
		switch {
		case topicFlag != "":
			f.resourceType, f.resourceName = "topic", topicFlag
		case groupFlag != "":
			f.resourceType, f.resourceName = "group", groupFlag
		case cmd.Flags().Changed("cluster"):
			f.resourceType = "cluster"
		case txnIDFlag != "":
			f.resourceType, f.resourceName = "transactional_id", txnIDFlag
		case dtokenFlag != "":
			f.resourceType, f.resourceName = "delegation_token", dtokenFlag
		}
		return nil
	}
}

func describeCommand(cl *client.Client) *cobra.Command {
	var f aclFilter

	cmd := &cobra.Command{
		Use:     "list",
		Aliases: []string{"ls", "describe", "d"},
		Short:   "List ACLs.",
		Long: `List ACLs.

List ACLs on a filter basis (Kafka 0.11.0+).

Listing works on a filter: every ACL matching the filter is returned. The
--type, --name, --pattern, --principal, --host, --operation, and --permission
flags are the filter form, where each one left at its default matches
everything; they are not the resource flags "kcl acl create" takes, which
name the one resource an ACL is created for. The --topic, --group, --cluster,
--transactional-id, and --delegation-token flags are shortcuts for --type and
--name. For resource names, principals, and hosts, a wildcard matches only
ACLs with wildcards; to match everything, leave the filter empty.

Rows are sorted by principal and then by resource.

EXAMPLES:
  kcl acl list                                           # list all ACLs
  kcl acl list --topic foo                               # ACLs for topic foo
  kcl acl list --cluster                                 # cluster-level ACLs
  kcl acl list --principal User:alice                    # ACLs for a principal
  kcl acl list --type any --pattern match --op any       # explicit match-all

SEE ALSO:
  kcl acl create    create ACLs
  kcl acl delete    delete ACLs
  kcl acl --help    detailed ACL documentation
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := f.validate(); err != nil {
				return err
			}
			rows, err := f.describe(cl)
			if err != nil {
				return err
			}
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "acls", aclHeaders...)
			for _, r := range rows {
				table.Row(r.identity()...)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, aclHeaders...)
	filterFlags(cmd, &f)
	return cmd
}

func createCommand(cl *client.Client) *cobra.Command {
	var (
		allowPrincipals []string
		denyPrincipals  []string
		allowHosts      []string
		denyHosts       []string
		topics          []string
		groups          []string
		txnIDs          []string
		dtokens         []string
		cluster         bool
		operations      []string
		pattern         string
		dryRun          bool
	)

	// Deprecated flags kept for backwards compatibility.
	var (
		oldTypes      []string
		oldNames      []string
		oldPrincipals []string
		oldHosts      []string
		oldPermission string
	)

	cmd := &cobra.Command{
		Use:     "create",
		Aliases: []string{"c"},
		Short:   "Create ACLs.",
		Long: `Create ACLs.

Create ACLs on a combinatorial basis (Kafka 0.11.0+).

ACL creation is combinatorial: all principals x all hosts x all resources x
all operations. Requires at least one principal, one resource, and one
operation. Hosts default to "*" (all) if not specified.

Every ACL prints with its result. --dry-run prints the same rows without
asking the cluster for anything, so ERROR and MESSAGE are unknown.

EXAMPLES:
  kcl acl create --topic foo --allow-principal User:alice --operation read
  kcl acl create --group '*' --allow-principal User:bob --operation read --operation describe
  kcl acl create --cluster --allow-principal User:admin --operation all
  kcl acl create --topic foo --topic bar --deny-principal User:eve --operation write
  kcl acl create --transactional-id myapp --allow-principal User:producer --operation write --operation describe
  kcl acl create --topic logs --allow-principal User:alice --allow-host 10.0.0.1 --operation read --pattern literal

SEE ALSO:
  kcl acl list      list ACLs
  kcl acl delete    delete ACLs
  kcl acl --help    detailed ACL documentation
`,

		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			// Merge deprecated flags into new ones.
			for _, p := range oldPrincipals {
				if client.Strnorm(oldPermission) == "deny" {
					denyPrincipals = append(denyPrincipals, p)
				} else {
					allowPrincipals = append(allowPrincipals, p)
				}
			}
			allowHosts = append(allowHosts, oldHosts...)
			for i, t := range oldTypes {
				if i < len(oldNames) {
					switch client.Strnorm(t) {
					case "topic":
						topics = append(topics, oldNames[i])
					case "group":
						groups = append(groups, oldNames[i])
					case "transactionalid":
						txnIDs = append(txnIDs, oldNames[i])
					case "delegationtoken":
						dtokens = append(dtokens, oldNames[i])
					case "cluster":
						cluster = true
					}
				}
			}

			// Build resource list: each (type, name) pair.
			type resource struct {
				typ  string
				name string
			}
			var resources []resource
			for _, t := range topics {
				resources = append(resources, resource{"topic", t})
			}
			for _, g := range groups {
				resources = append(resources, resource{"group", g})
			}
			for _, t := range txnIDs {
				resources = append(resources, resource{"transactional_id", t})
			}
			for _, d := range dtokens {
				resources = append(resources, resource{"delegation_token", d})
			}
			if cluster {
				resources = append(resources, resource{"cluster", "kafka-cluster"})
			}

			if len(resources) == 0 {
				return out.Errf(out.ExitUsage, "at least one resource is required (--topic, --group, --cluster, --transactional-id, or --delegation-token)")
			}
			if len(operations) == 0 {
				return out.Errf(out.ExitUsage, "at least one --operation is required")
			}
			if err := validateCreate(pattern, operations); err != nil {
				return err
			}

			// Build principal/permission pairs.
			type principalPerm struct {
				principal  string
				permission string
			}
			var principals []principalPerm
			for _, p := range allowPrincipals {
				principals = append(principals, principalPerm{p, "allow"})
			}
			for _, p := range denyPrincipals {
				principals = append(principals, principalPerm{p, "deny"})
			}
			if len(principals) == 0 {
				return out.Errf(out.ExitUsage, "at least one principal is required (--allow-principal or --deny-principal)")
			}

			// Default hosts to wildcard.
			hosts := append(allowHosts, denyHosts...)
			if len(hosts) == 0 {
				hosts = []string{"*"}
			}

			// Build combinatorial request.
			req := new(kmsg.CreateACLsRequest)
			for _, res := range resources {
				for _, pp := range principals {
					for _, host := range hosts {
						for _, op := range operations {
							req.Creations = append(req.Creations, kmsg.CreateACLsRequestCreation{
								ResourceType:        atoiResourceType(res.typ),
								ResourceName:        res.name,
								ResourcePatternType: atoiResourcePattern(pattern),
								Principal:           pp.principal,
								Host:                host,
								Operation:           atoiOperation(op),
								PermissionType:      atoiPermission(pp.permission),
							})
						}
					}
				}
			}

			rows := make([]aclRow, len(req.Creations))
			for i, c := range req.Creations {
				rows[i] = aclRow{
					typ:        c.ResourceType.String(),
					name:       c.ResourceName,
					pattern:    c.ResourcePatternType.String(),
					principal:  c.Principal,
					host:       c.Host,
					operation:  c.Operation.String(),
					permission: c.PermissionType.String(),
					err:        out.Unknown,
					msg:        out.Unknown,
				}
			}

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results", aclResultHeaders...).ResultColumns()
			if dryRun {
				table.SetDryRun(true)
				for _, r := range rows {
					table.Row(r.result()...)
				}
				return table.Flush()
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to create acls: %v", err)
			}
			resp := kresp.(*kmsg.CreateACLsResponse)
			if len(resp.Results) != len(req.Creations) {
				return fmt.Errorf("Kafka answered %d results for %d creations", len(resp.Results), len(req.Creations))
			}
			for i, result := range resp.Results {
				rows[i].err, rows[i].msg = errorCells(result.ErrorCode, result.ErrorMessage)
				table.Row(rows[i].result()...)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, aclResultHeaders...)

	// Primary flags: the ergonomic interface.
	cmd.Flags().StringArrayVar(&allowPrincipals, "allow-principal", nil, "principal to allow (repeatable)")
	cmd.Flags().StringArrayVar(&denyPrincipals, "deny-principal", nil, "principal to deny (repeatable)")
	cmd.Flags().StringArrayVar(&allowHosts, "allow-host", nil, "host to allow from (repeatable; default '*')")
	cmd.Flags().StringArrayVar(&denyHosts, "deny-host", nil, "host to deny from (repeatable)")
	cmd.Flags().StringArrayVarP(&topics, "topic", "t", nil, "topic resource (repeatable)")
	cmd.Flags().StringArrayVarP(&groups, "group", "g", nil, "group resource (repeatable)")
	cmd.Flags().StringArrayVar(&txnIDs, "transactional-id", nil, "transactional ID resource (repeatable)")
	cmd.Flags().StringArrayVar(&dtokens, "delegation-token", nil, "delegation token resource (repeatable)")
	cmd.Flags().BoolVar(&cluster, "cluster", false, "cluster resource")
	cmd.Flags().StringArrayVar(&operations, "operation", nil, "operation to allow or deny (repeatable)")
	cmd.Flags().StringVar(&pattern, "pattern", "literal", "resource pattern type: literal or prefixed (Kafka 2.0.0+)")
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "print the ACLs that would be created without creating them")

	// Deprecated flags: hidden, and they still work.
	cmd.Flags().StringArrayVar(&oldTypes, "type", nil, "")
	cmd.Flags().StringArrayVar(&oldNames, "name", nil, "")
	cmd.Flags().StringArrayVar(&oldPrincipals, "principal", nil, "")
	cmd.Flags().StringArrayVar(&oldHosts, "host", nil, "")
	cmd.Flags().StringVar(&oldPermission, "perm", "allow", "")
	cmd.Flags().StringArrayVar(&operations, "op", nil, "")
	cmd.Flags().MarkDeprecated("type", "use --topic, --group, --cluster, or --transactional-id")
	cmd.Flags().MarkDeprecated("name", "use --topic, --group, --cluster, or --transactional-id")
	cmd.Flags().MarkDeprecated("principal", "use --allow-principal or --deny-principal")
	cmd.Flags().MarkDeprecated("host", "use --allow-host or --deny-host")
	cmd.Flags().MarkDeprecated("perm", "use --allow-principal or --deny-principal")
	cmd.Flags().MarkDeprecated("op", "use --operation")

	registerCompletions(cmd, map[string][]string{
		"pattern":   createPatternValues,
		"operation": createOperationValues,
	})

	return cmd
}

func deleteCommand(cl *client.Client) *cobra.Command {
	var (
		f         aclFilter
		dryRun    bool
		noConfirm bool
	)

	cmd := &cobra.Command{
		Use:   "delete",
		Short: "Delete ACLs.",
		Long: `Delete ACLs.

Delete ACLs on a filter basis (Kafka 0.11.0+).

Like listing, deleting works on a filter: every ACL matching the filter is
deleted. The --type, --name, --pattern, --principal, --host, --operation, and
--permission flags are the filter form, where each one left at its default
matches everything; they are not the resource flags "kcl acl create" takes.
The --topic, --group, --cluster, --transactional-id, and --delegation-token
flags are shortcuts for --type and --name. For resource names, principals,
and hosts, a wildcard matches only ACLs with wildcards; to match everything,
leave the filter empty.

The delete request allows many filters at once, but that is hard to express
from a CLI, so kcl sends one filter per command.

Every unspecified filter matches everything, so a bare "kcl acl delete"
matches every ACL in the cluster. What protects you is the confirmation: the
command first prints every ACL the filter matched and asks before deleting,
the same as rpk and kafka-acls.sh. Declining, or a stdin that is not a
terminal, prints the matches as a dry run and exits 0. Use --dry-run to see
the matches and stop there, or --yes/-y to skip the prompt entirely.

EXAMPLES:
  kcl acl delete                                 # every ACL, after confirming
  kcl acl delete --topic foo                     # all ACLs for topic foo
  kcl acl delete --cluster --principal User:old  # all cluster ACLs for a principal
  kcl acl delete --topic foo --dry-run           # show the matches, delete nothing

SEE ALSO:
  kcl acl list      list ACLs
  kcl acl create    create ACLs
  kcl acl --help    detailed ACL documentation
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			if err := f.validate(); err != nil {
				return err
			}

			// plan prints the matched ACLs in the result shape, with
			// no result: a dry run.
			plan := func(matched []aclRow) error {
				table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "deleted", aclResultHeaders...).ResultColumns()
				table.SetDryRun(true)
				for _, r := range matched {
					r.err, r.msg = out.Unknown, out.Unknown
					table.Row(r.result()...)
				}
				return table.Flush()
			}

			if dryRun || !noConfirm {
				matched, err := f.describe(cl)
				if err != nil {
					return err
				}
				if dryRun {
					return plan(matched)
				}
				if len(matched) == 0 {
					// The same empty table -y prints when its
					// filter matches nothing.
					fmt.Fprintln(os.Stderr, "No ACLs match the filter; nothing to delete.")
					return out.NewFormattedTable(cl.Format(), cl.Command(), 1, "deleted", aclResultHeaders...).ResultColumns().Flush()
				}
				text := cl.Format() == out.FormatText
				if text {
					fmt.Fprintln(os.Stderr, "The following ACLs will be deleted:")
					table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "acls", aclHeaders...)
					for _, r := range matched {
						table.Row(r.identity()...)
					}
					table.Flush()
				}
				if out.Confirm(fmt.Sprintf("Proceed with deletion of %s?", plural(len(matched), "ACL"))) != out.Yes {
					if text {
						return nil
					}
					return plan(matched)
				}
			}

			kresp, err := cl.Client().Request(context.Background(), f.deleteRequest())
			if err != nil {
				return fmt.Errorf("unable to delete acls: %v", err)
			}
			resp := kresp.(*kmsg.DeleteACLsResponse)
			if len(resp.Results) != 1 {
				return fmt.Errorf("we requested one filter, but got %d responses", len(resp.Results))
			}
			result := resp.Results[0]
			if err := kerr.ErrorForCode(result.ErrorCode); err != nil {
				return fmt.Errorf("%s%s", err, brokerMessage(result.ErrorMessage))
			}

			rows := make([]aclRow, 0, len(result.MatchingACLs))
			for _, acl := range result.MatchingACLs {
				r := aclRow{
					typ:        acl.ResourceType.String(),
					name:       acl.ResourceName,
					pattern:    acl.ResourcePatternType.String(),
					principal:  acl.Principal,
					host:       acl.Host,
					operation:  acl.Operation.String(),
					permission: acl.PermissionType.String(),
				}
				r.err, r.msg = errorCells(acl.ErrorCode, acl.ErrorMessage)
				rows = append(rows, r)
			}
			sortACLs(rows)
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "deleted", aclResultHeaders...).ResultColumns()
			for _, r := range rows {
				table.Row(r.result()...)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, aclResultHeaders...)
	filterFlags(cmd, &f)
	cmd.Flags().BoolVarP(&dryRun, "dry-run", "d", false, "print the ACLs that would be deleted without deleting them")
	cmd.Flags().BoolVarP(&noConfirm, "yes", "y", false, "skip the confirmation prompt before deleting")

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
