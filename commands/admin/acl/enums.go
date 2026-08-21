package acl

import (
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/twmb/kcl/out"
)

// The accepted values for each enum-valued ACL flag, declared once and used
// three ways: to build the error message when input is not recognized, to drive
// shell completion, and (via the atoi* functions) to validate. Validation goes
// through the same atoi* conversion the request builders use rather than
// re-matching these strings, so the check and the conversion cannot disagree
// about what counts as a match -- the atoi* functions normalize casing,
// underscores, and dashes, and duplicating that here would be a second place
// for it to drift.
var (
	resourceTypeValues = []string{"any", "topic", "group", "cluster", "transactional_id", "delegation_token"}
	patternValues      = []string{"any", "match", "literal", "prefixed"}
	operationValues    = []string{
		"any", "all", "read", "write", "create", "delete", "alter", "describe",
		"cluster_action", "describe_configs", "alter_configs", "idempotent_write",
	}
	permissionValues = []string{"any", "allow", "deny"}

	// Creating an ACL cannot use the filter-only match-anything values: there
	// is no such thing as an ACL whose operation is ANY, and a created ACL's
	// pattern must name a concrete scheme.
	createPatternValues   = []string{"literal", "prefixed"}
	createOperationValues = operationValues[1:] // everything but "any"
)

// invalidValue builds a usage error naming the flag and what it accepts. It
// deliberately says nothing about the Kafka request: an unrecognized value is a
// problem with the flag, and the user does not need to know that it would have
// become an UNKNOWN element in a DescribeACLs filter.
func invalidValue(flag, got string, valid []string) error {
	return out.Errf(out.ExitUsage, "invalid --%s %q: valid values are %s",
		flag, got, strings.Join(valid, ", "))
}

// validateFilters checks the four enum-valued filter flags shared by list and
// delete. Unrecognized input maps to UNKNOWN, which is not a legal filter
// element -- brokers reject the whole request while parsing it, in the worst
// case by closing the connection (see #56). kcl can tell locally, so it does,
// and never sends a request it already knows is malformed.
//
// Empty values are the caller's business: list defaults every filter to a
// match-anything value, while delete requires each one explicitly and reports
// the missing ones itself.
func validateFilters(resourceType, pattern, operation, permission string) error {
	if atoiResourceType(resourceType) == 0 {
		return invalidValue("type", resourceType, resourceTypeValues)
	}
	if atoiResourcePattern(pattern) == 0 {
		return invalidValue("pattern", pattern, patternValues)
	}
	if atoiOperation(operation) == 0 {
		return invalidValue("operation", operation, operationValues)
	}
	if atoiPermission(permission) == 0 {
		return invalidValue("permission", permission, permissionValues)
	}
	return nil
}

// validateCreate checks the enum-valued flags on create, which accepts a
// narrower set than a filter does.
func validateCreate(pattern string, operations []string) error {
	if p := atoiResourcePattern(pattern); p != 3 && p != 4 { // literal, prefixed
		return invalidValue("pattern", pattern, createPatternValues)
	}
	for _, op := range operations {
		if o := atoiOperation(op); o == 0 || o == 1 { // unknown, any
			return invalidValue("operation", op, createOperationValues)
		}
	}
	return nil
}

// registerCompletions wires value completion for the enum-valued flags. Cobra
// omits hidden flags from suggestions, so the short aliases (--op, --perm) are
// not offered while still working when typed.
func registerCompletions(cmd *cobra.Command, flags map[string][]string) {
	for flag, values := range flags {
		vals := values
		cmd.RegisterFlagCompletionFunc(flag, func(*cobra.Command, []string, string) ([]string, cobra.ShellCompDirective) {
			return vals, cobra.ShellCompDirectiveNoFileComp
		})
	}
}

// confirm prints prompt and reports whether the answer was yes. Anything else,
// including a bare enter or EOF, declines.
func confirm(prompt string) bool {
	fmt.Fprint(os.Stderr, prompt)
	var answer string
	fmt.Scanln(&answer)
	switch strings.ToLower(strings.TrimSpace(answer)) {
	case "y", "yes":
		return true
	}
	fmt.Fprintln(os.Stderr, "Aborting.")
	return false
}

// plural renders "1 ACL" / "3 ACLs".
func plural(n int, noun string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, noun)
	}
	return fmt.Sprintf("%d %ss", n, noun)
}
