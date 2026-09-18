package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"github.com/twmb/kcl/out"
)

// The walkthrough runs every read-only command against one kfake cluster and
// checks what a user sees: the exit code, and the shape of stdout in each of
// the three formats. It runs each command in a child process because a
// command may exit on its own, and because the exit code is half of what we
// are checking.

// walkthroughEnv carries the arguments of the kcl a child runs, as JSON. A
// test binary with this set in its environment is a kcl, not a test.
const walkthroughEnv = "KCL_TEST_WALKTHROUGH_ARGS"

func TestMain(m *testing.M) {
	if s, ok := os.LookupEnv(walkthroughEnv); ok {
		var args []string
		if err := json.Unmarshal([]byte(s), &args); err != nil {
			fmt.Fprintf(os.Stderr, "unable to read %s: %v\n", walkthroughEnv, err)
			os.Exit(99)
		}
		os.Args = append([]string{"kcl"}, args...)
		main() // exits on its own on failure
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// Names the walkthrough seeds and then reads back.
const (
	walkTopic     = "walk-topic" // two partitions, three replicas
	walkOther     = "walk-other" // one partition, one replica, a retention.ms override
	walkGroup     = "walk-group" // committed by an admin client, no member
	walkMember    = "walk-member"
	walkInstance  = "walk-instance" // the static member of walkMember, live for the run
	walk848       = "walk-848"      // a consumer protocol group with a live member
	walkShare     = "walk-share"
	walkTxn       = "walk-txn" // a transaction left open on walkOther
	walkSubject   = walkTopic + "-value"
	walkRefer     = "walk-refer-value" // a schema referencing walkSubject
	walkProfile   = "walkthrough"
	walkPrincipal = "User:alice"
	walkUser      = "alice"

	// walkRecords is how many records each seeded topic carries, spread
	// over its partitions in turn.
	walkRecords = 5
)

// schemaFileArg stands in for the schema file a command reads. We write the
// file per run, so the table cannot name its path.
const schemaFileArg = "<schema-file>"

// walkthroughLeaves are the leaf commands the walkthrough runs, by the dotted
// path out.CommandName gives them. Args are what follows the command name.
// Variant is the flags that choose which table the command prints, so that
// a command with a --section, --by, or --aggregate-into is run once per
// table; a leaf's key in walkthroughHeaders and walkthroughEmpty is its path
// followed by its variant. Exit is what a user sees: zero, unless kfake
// cannot answer the request, and then why says what it answers instead.
var walkthroughLeaves = []struct {
	path    string
	args    []string
	variant []string
	stdin   string
	exit    int
	why     string
}{
	{path: "acl.list"},
	{path: "client-metrics.describe", args: []string{"walk-metrics"}},
	{path: "client-metrics.list"},
	{path: "cluster.describe"},
	{path: "cluster.describe", variant: []string{"--section", "cluster"}},
	{path: "cluster.describe", variant: []string{"--section", "brokers"}},
	{path: "cluster.describe-quorum", exit: 1, why: "kfake does not implement DescribeQuorum"},
	{path: "cluster.describe-quorum", variant: []string{"--section", "voters"}, exit: 1, why: "kfake does not implement DescribeQuorum"},
	{path: "cluster.describe-quorum", variant: []string{"--section", "observers"}, exit: 1, why: "kfake does not implement DescribeQuorum"},
	{path: "cluster.features.describe"},
	{path: "cluster.metadata"},
	{path: "cluster.metadata", variant: []string{"--section", "cluster"}},
	{path: "cluster.metadata", variant: []string{"--section", "brokers"}},
	{path: "cluster.metadata", variant: []string{"--section", "topics"}},
	{path: "config.describe", args: []string{walkTopic}},
	{path: "dtoken.describe", exit: 1, why: "kfake does not implement DescribeDelegationToken"},
	{path: "group.describe", args: []string{walkGroup}},
	{path: "group.describe", args: []string{walkGroup}, variant: []string{"--section", "summary"}},
	{path: "group.describe", args: []string{walkGroup}, variant: []string{"--section", "lag"}},
	{path: "group.describe", args: []string{walkMember}, variant: []string{"--section", "members"}},
	{path: "group.describe", args: []string{walkGroup}, variant: []string{"--by", "topic"}},
	{path: "group.describe", args: []string{walkGroup}, variant: []string{"--by", "member"}},
	{path: "group.describe", args: []string{walkGroup}, variant: []string{"--by", "group"}},
	{path: "group.describe", args: []string{walk848}, variant: []string{"--consumer-protocol"}},
	{path: "group.describe", args: []string{walkMember}, variant: []string{"--lag", ">0"}},
	{path: "group.describe", args: []string{walkMember}, variant: []string{"--instance-ids"}},
	{path: "group.list"},
	{path: "group.list", variant: []string{"--state", "empty"}},
	{path: "logdirs.describe"},
	{path: "logdirs.describe", variant: []string{"--aggregate-into", "broker"}},
	{path: "logdirs.describe", variant: []string{"--aggregate-into", "dir"}},
	{path: "logdirs.describe", variant: []string{"--aggregate-into", "topic"}},
	{path: "misc.api-versions"},
	{path: "misc.errcode", args: []string{"3"}},
	{path: "misc.errtext", args: []string{"UNKNOWN_TOPIC_OR_PARTITION"}},
	{path: "misc.offset-for-leader-epoch", args: []string{walkTopic, "-e", "0"}},
	{path: "misc.probe-version"},
	{path: "misc.raw-req", args: []string{"-k", "18"}, stdin: "{}"},
	{path: "profile.current"},
	{path: "profile.dump"},
	{path: "profile.list"},
	{path: "quota.describe"},
	{path: "reassign.list"},
	{path: "registry.compatibility.get", args: []string{walkSubject}},
	{path: "registry.context.list"},
	{path: "registry.mode.get"},
	{path: "registry.schema.check-compatibility", args: []string{walkSubject, "-s", schemaFileArg}},
	{path: "registry.schema.get", args: []string{"-S", walkSubject}},
	{path: "registry.schema.list"},
	{path: "registry.schema.references", args: []string{walkSubject}},
	{path: "registry.subject.list"},
	{path: "share-group.describe", args: []string{walkShare}},
	{path: "share-group.describe", args: []string{walkShare}, variant: []string{"--section", "summary"}},
	{path: "share-group.describe", args: []string{walkShare}, variant: []string{"--section", "members"}},
	{path: "share-group.describe", args: []string{walkShare}, variant: []string{"--section", "offsets"}},
	{path: "share-group.list"},
	{path: "topic.describe", args: []string{walkTopic, walkOther}},
	{path: "topic.describe", args: []string{walkTopic, walkOther}, variant: []string{"--section", "summary"}},
	{path: "topic.describe", args: []string{walkTopic, walkOther}, variant: []string{"--section", "partitions"}},
	{path: "topic.describe", args: []string{walkTopic, walkOther}, variant: []string{"--section", "configs"}},
	{path: "topic.describe", args: []string{walkTopic, walkOther}, variant: []string{"--stable"}},
	{path: "topic.describe", args: []string{walkTopic, walkOther}, variant: []string{"--under-replicated"}},
	{path: "topic.describe", args: []string{walkTopic, walkOther}, variant: []string{"--with-overrides"}},
	{path: "topic.list"},
	{path: "topic.list", args: []string{"walk-.*"}, variant: []string{"-r"}},
	{path: "topic.list-offsets", args: []string{walkTopic}},
	{path: "txn.describe", args: []string{walkTxn}},
	{path: "txn.describe-producers", args: []string{walkOther}},
	{path: "txn.list"},
	{path: "user.list"},
	{path: "version"},
}

// walkthroughSkips are the leaves the walkthrough does not run, and why. A
// leaf in neither list fails the run, so a new command cannot arrive without
// someone deciding which of the two it is.
var walkthroughSkips = []struct{ path, why string }{
	{"client-metrics.alter", "alters a metrics subscription"},
	{"client-metrics.delete", "deletes a metrics subscription"},
	{"cluster.add-controller", "changes the quorum"},
	{"cluster.features.update", "changes finalized feature versions"},
	{"cluster.remove-controller", "changes the quorum"},
	{"consume", "runs until it is interrupted; the consume package tests it"},
	{"dtoken.create", "creates a delegation token"},
	{"dtoken.expire", "expires a delegation token"},
	{"dtoken.renew", "renews a delegation token"},
	{"fake.control.call", "drives a running kcl fake cluster"},
	{"fake.control.fault.add", "drives a running kcl fake cluster"},
	{"fake.control.fault.list", "needs a kcl fake control endpoint, which a cluster in this process does not serve"},
	{"fake.control.fault.rm", "drives a running kcl fake cluster"},
	{"fake.control.fault.wait", "blocks until a fault is hit"},
	{"fake.control.group.wait", "blocks until a group reaches a state"},
	{"fake.control.methods", "needs a kcl fake control endpoint, which a cluster in this process does not serve"},
	{"group.offset-delete", "deletes committed offsets"},
	{"logdirs.alter", "moves partitions between log dirs"},
	{"misc.gen-autocomplete", "writes a shell script for you to source, so there is no document for --format to shape"},
	{"produce", "reads records from stdin and writes them to the cluster"},
	{"reassign.alter", "reassigns partitions"},
	{"reassign.cancel", "cancels a reassignment"},
	{"registry.compatibility.set", "sets a compatibility level"},
	{"registry.context.delete", "deletes a context"},
	{"registry.schema.delete", "deletes a schema version, and has no dry run"},
	{"registry.subject.delete", "deletes a subject, and has no dry run"},
	{"registry.mode.set", "sets a mode"},
	{"registry.schema.create", "registers a schema"},
	{"share-group.offset-delete", "deletes committed offsets"},
	{"topic.add-partitions", "adds partitions"},
	{"user.alter", "alters SCRAM credentials"},
}

// walkthroughMutations are the mutating leaves the walkthrough runs against
// the cluster in the way that changes nothing: a dry run, or a [y/N] prompt
// whose stdin is not a terminal, which prints the plan. Each prints one
// document in every format, marked as a dry run, and exits 0, so that a
// script can preview what a command would do. The state check after them
// pins that nothing changed.
var walkthroughMutations = []struct {
	path string
	args []string
}{
	{path: "acl.create", args: []string{"--topic", walkOther, "--allow-principal", walkPrincipal, "--operation", "read", "--dry-run"}},
	{path: "acl.delete", args: []string{"--topic", walkTopic, "--dry-run"}},
	{path: "cluster.elect-leaders", args: []string{walkTopic + ":0", "--dry-run"}},
	{path: "config.alter", args: []string{walkTopic, "-s", "retention.ms=1000", "--dry-run"}},
	{path: "group.delete", args: []string{walkGroup, "--dry-run"}},
	{path: "group.seek", args: []string{walkGroup, "--to", "start", "--dry-run"}},
	{path: "quota.alter", args: []string{"--name", "user=bob", "--add", "producer_byte_rate=1048576", "--dry-run"}},
	{path: "share-group.delete", args: []string{walkShare, "--dry-run"}},
	{path: "share-group.seek", args: []string{walkShare, "--to", "start", "-t", walkTopic, "--dry-run"}},
	{path: "topic.create", args: []string{"walk-new", "--dry-run"}},
	{path: "topic.delete", args: []string{walkOther, "--dry-run"}},
	{path: "topic.trim-prefix", args: []string{walkTopic, "-o", "1"}},
}

// walkthroughProfileSteps are the profile mutators, run in this order against
// a config file of their own, since each depends on what the one before
// wrote. Each prints a {profile, path, current} document and exits 0.
var walkthroughProfileSteps = []struct {
	path string
	args []string
}{
	{path: "profile.create", args: []string{"walk2", "-B", "localhost:9"}},
	{path: "profile.use", args: []string{"walk2"}},
	{path: "profile.set", args: []string{"-X", "dial_timeout=2s"}},
	{path: "profile.rename", args: []string{"walk2", "walk3"}},
	{path: "profile.delete", args: []string{"walk3"}},
}

// walkthroughHeaders is the awk header of every leaf the walkthrough runs,
// exactly as --format awk-header prints it for the leaf's variant. It is the
// scripting contract: a column renamed, added, moved, or dropped fails here,
// and is a BREAKING entry in the release notes. A leaf with no table, whose
// awk output is a document, has an empty header.
var walkthroughHeaders = map[string]string{
	"acl.create":                                  "TYPE\tNAME\tPATTERN\tPRINCIPAL\tHOST\tOPERATION\tPERMISSION\tERROR\tMESSAGE",
	"acl.delete":                                  "TYPE\tNAME\tPATTERN\tPRINCIPAL\tHOST\tOPERATION\tPERMISSION\tERROR\tMESSAGE",
	"acl.list":                                    "TYPE\tNAME\tPATTERN\tPRINCIPAL\tHOST\tOPERATION\tPERMISSION",
	"client-metrics.describe":                     "KEY\tVALUE\tSOURCE",
	"client-metrics.list":                         "NAME",
	"cluster.describe":                            "ID\tHOST\tPORT\tRACK",
	"cluster.describe --section brokers":          "ID\tHOST\tPORT\tRACK",
	"cluster.describe --section cluster":          "CLUSTER-ID\tCONTROLLER\tAUTHORIZED-OPERATIONS",
	"cluster.describe-quorum":                     "TOPIC\tPARTITION\tLEADER\tLEADER-EPOCH\tHIGH-WATERMARK\tROLE\tREPLICA\tLOG-END-OFFSET\tLAST-FETCH-TIMESTAMP\tLAST-CAUGHT-UP-TIMESTAMP\tERROR\tMESSAGE",
	"cluster.describe-quorum --section observers": "TOPIC\tPARTITION\tLEADER\tLEADER-EPOCH\tHIGH-WATERMARK\tROLE\tREPLICA\tLOG-END-OFFSET\tLAST-FETCH-TIMESTAMP\tLAST-CAUGHT-UP-TIMESTAMP\tERROR\tMESSAGE",
	"cluster.describe-quorum --section voters":    "TOPIC\tPARTITION\tLEADER\tLEADER-EPOCH\tHIGH-WATERMARK\tROLE\tREPLICA\tLOG-END-OFFSET\tLAST-FETCH-TIMESTAMP\tLAST-CAUGHT-UP-TIMESTAMP\tERROR\tMESSAGE",
	"cluster.elect-leaders":                       "TOPIC\tPARTITION\tERROR\tMESSAGE",
	"cluster.features.describe":                   "FEATURE\tSUPPORTED-MIN\tSUPPORTED-MAX\tFINALIZED\tEPOCH\tDESCRIPTION",
	"cluster.metadata":                            "TOPIC\tTOPIC-ID\tPARTITIONS\tREPLICATION\tERROR",
	"cluster.metadata --section brokers":          "ID\tHOST\tPORT\tRACK",
	"cluster.metadata --section cluster":          "CLUSTER-ID\tCONTROLLER",
	"cluster.metadata --section topics":           "TOPIC\tTOPIC-ID\tPARTITIONS\tREPLICATION\tERROR",
	"config.alter":                                "RESOURCE\tERROR\tMESSAGE",
	"config.describe":                             "RESOURCE\tKEY\tTYPE\tVALUE\tSOURCE\tREAD-ONLY",
	"dtoken.describe":                             "PRINCIPAL\tISSUED\tEXPIRY\tMAX-AGE\tTOKEN-ID\tHMAC\tRENEWERS",
	"group.delete":                                "BROKER\tGROUP\tERROR\tMESSAGE",
	"group.describe":                              "GROUP\tTOPIC\tPARTITION\tCURRENT-OFFSET\tLOG-START-OFFSET\tLOG-END-OFFSET\tLAG\tMEMBER-ID\tCLIENT-ID\tHOST\tRACK\tINSTANCE-ID\tERROR\tMESSAGE",
	"group.describe --by group":                   "GROUP\tSTATE\tMEMBERS\tPARTITIONS\tLAG",
	"group.describe --by member":                  "GROUP\tMEMBER-ID\tPARTITIONS\tLAG\tCLIENT-ID\tHOST\tRACK\tINSTANCE-ID",
	"group.describe --by topic":                   "GROUP\tTOPIC\tPARTITIONS\tLAG",
	"group.describe --consumer-protocol":          "GROUP\tTOPIC\tPARTITION\tCURRENT-OFFSET\tLOG-START-OFFSET\tLOG-END-OFFSET\tLAG\tMEMBER-ID\tCLIENT-ID\tHOST\tRACK\tINSTANCE-ID\tERROR\tMESSAGE",
	"group.describe --instance-ids":               "GROUP\tTOPIC\tPARTITION\tCURRENT-OFFSET\tLOG-START-OFFSET\tLOG-END-OFFSET\tLAG\tMEMBER-ID\tCLIENT-ID\tHOST\tRACK\tINSTANCE-ID\tERROR\tMESSAGE",
	"group.describe --lag >0":                     "GROUP\tTOPIC\tPARTITION\tCURRENT-OFFSET\tLOG-START-OFFSET\tLOG-END-OFFSET\tLAG\tMEMBER-ID\tCLIENT-ID\tHOST\tRACK\tINSTANCE-ID\tERROR\tMESSAGE",
	"group.describe --section lag":                "GROUP\tTOPIC\tPARTITION\tCURRENT-OFFSET\tLOG-START-OFFSET\tLOG-END-OFFSET\tLAG\tMEMBER-ID\tCLIENT-ID\tHOST\tRACK\tINSTANCE-ID\tERROR\tMESSAGE",
	"group.describe --section members":            "GROUP\tMEMBER-ID\tCLIENT-ID\tHOST\tRACK\tINSTANCE-ID\tMEMBER-EPOCH\tSUBSCRIBED-TOPICS\tASSIGNMENT\tTARGET-ASSIGNMENT",
	"group.describe --section summary":            "GROUP\tCOORDINATOR\tSTATE\tBALANCER\tMEMBERS\tTOTAL-LAG\tERROR\tMESSAGE",
	"group.list":                                  "BROKER\tGROUP\tPROTO-TYPE\tGROUP-TYPE\tSTATE\tERROR",
	"group.list --state empty":                    "BROKER\tGROUP\tPROTO-TYPE\tGROUP-TYPE\tSTATE\tERROR",
	"group.seek":                                  "TOPIC\tPARTITION\tPRIOR-OFFSET\tNEW-OFFSET\tERROR\tMESSAGE",
	"logdirs.describe":                            "BROKER\tDIR\tTOPIC\tPARTITION\tSIZE\tOFFSET-LAG\tIS-FUTURE\tTOTAL\tUSABLE\tCORDONED\tERROR",
	"logdirs.describe --aggregate-into broker":    "BROKER\tSIZE",
	"logdirs.describe --aggregate-into dir":       "DIR\tSIZE",
	"logdirs.describe --aggregate-into topic":     "TOPIC\tSIZE",
	"misc.api-versions":                           "NAME\tKEY\tMAX",
	"misc.errcode":                                "NAME\tCODE\tDESCRIPTION",
	"misc.errtext":                                "NAME\tCODE\tDESCRIPTION",
	"misc.offset-for-leader-epoch":                "BROKER\tTOPIC\tPARTITION\tLEADER-EPOCH\tEND-OFFSET\tERROR",
	"misc.probe-version":                          "MIN\tMAX",
	"misc.raw-req":                                "", // no table: a raw response is a JSON document in every format
	"profile.create":                              "KEY\tVALUE",
	"profile.current":                             "PROFILE",
	"profile.delete":                              "KEY\tVALUE",
	"profile.dump":                                "KEY\tVALUE",
	"profile.list":                                "NAME\tCURRENT",
	"profile.rename":                              "KEY\tVALUE",
	"profile.set":                                 "KEY\tVALUE",
	"profile.use":                                 "KEY\tVALUE",
	"quota.alter":                                 "ENTITY\tERROR\tMESSAGE",
	"quota.describe":                              "ENTITY\tKEY\tVALUE",
	"reassign.list":                               "TOPIC\tPARTITION\tCURRENT-REPLICAS\tADDING\tREMOVING",
	"registry.compatibility.get":                  "SUBJECT\tLEVEL\tERROR",
	"registry.context.list":                       "CONTEXT",
	"registry.mode.get":                           "SUBJECT\tMODE\tERROR",
	"registry.schema.check-compatibility":         "SUBJECT\tVERSION\tCOMPATIBLE",
	"registry.schema.get":                         "SUBJECT\tVERSION\tID\tTYPE\tSCHEMA",
	"registry.schema.list":                        "SUBJECT\tVERSION\tID\tTYPE",
	"registry.schema.references":                  "SUBJECT\tVERSION\tID",
	"registry.subject.list":                       "SUBJECT",
	"share-group.delete":                          "BROKER\tGROUP\tERROR\tMESSAGE",
	"share-group.describe":                        "GROUP\tTOPIC\tPARTITION\tSTART-OFFSET\tLEADER-EPOCH\tLAG\tERROR\tMESSAGE",
	"share-group.describe --section members":      "GROUP\tMEMBER-ID\tCLIENT-ID\tHOST\tRACK\tMEMBER-EPOCH\tSUBSCRIBED-TOPICS\tASSIGNMENT",
	"share-group.describe --section offsets":      "GROUP\tTOPIC\tPARTITION\tSTART-OFFSET\tLEADER-EPOCH\tLAG\tERROR\tMESSAGE",
	"share-group.describe --section summary":      "GROUP\tCOORDINATOR\tSTATE\tEPOCH\tASSIGNMENT-EPOCH\tASSIGNOR\tMEMBERS\tTOTAL-LAG\tERROR\tMESSAGE",
	"share-group.list":                            "BROKER\tGROUP\tSTATE\tERROR",
	"share-group.seek":                            "TOPIC\tPARTITION\tPRIOR-OFFSET\tNEW-OFFSET\tERROR\tMESSAGE",
	"topic.create":                                "TOPIC\tTOPIC-ID\tERROR\tMESSAGE",
	"topic.delete":                                "TOPIC\tERROR\tMESSAGE",
	"topic.describe":                              "TOPIC\tPARTITION\tLEADER\tLEADER-EPOCH\tREPLICAS\tISR\tOFFLINE-REPLICAS\tSTART-OFFSET\tEND-OFFSET\tSTABLE-OFFSET\tERROR",
	"topic.describe --section configs":            "TOPIC\tKEY\tVALUE\tSOURCE\tSENSITIVE\tERROR",
	"topic.describe --section partitions":         "TOPIC\tPARTITION\tLEADER\tLEADER-EPOCH\tREPLICAS\tISR\tOFFLINE-REPLICAS\tSTART-OFFSET\tEND-OFFSET\tSTABLE-OFFSET\tERROR",
	"topic.describe --section summary":            "TOPIC\tTOPIC-ID\tPARTITIONS\tREPLICATION\tERROR",
	"topic.describe --stable":                     "TOPIC\tPARTITION\tLEADER\tLEADER-EPOCH\tREPLICAS\tISR\tOFFLINE-REPLICAS\tSTART-OFFSET\tEND-OFFSET\tSTABLE-OFFSET\tERROR",
	"topic.describe --under-replicated":           "TOPIC\tPARTITION\tLEADER\tLEADER-EPOCH\tREPLICAS\tISR\tOFFLINE-REPLICAS\tSTART-OFFSET\tEND-OFFSET\tSTABLE-OFFSET\tERROR",
	"topic.describe --with-overrides":             "TOPIC\tPARTITION\tLEADER\tLEADER-EPOCH\tREPLICAS\tISR\tOFFLINE-REPLICAS\tSTART-OFFSET\tEND-OFFSET\tSTABLE-OFFSET\tERROR",
	"topic.list":                                  "TOPIC\tTOPIC-ID\tPARTITIONS\tREPLICATION\tERROR",
	"topic.list -r":                               "TOPIC\tTOPIC-ID\tPARTITIONS\tREPLICATION\tERROR",
	"topic.list-offsets":                          "BROKER\tTOPIC\tPARTITION\tSTART\tSTABLE\tEND\tSTART-EPOCH\tSTABLE-EPOCH\tEND-EPOCH\tAT\tERROR",
	"topic.trim-prefix":                           "TOPIC\tPARTITION\tPRIOR-OFFSET\tNEW-OFFSET\tERROR\tMESSAGE",
	"txn.describe":                                "TRANSACTIONAL-ID\tSTATE\tPRODUCER-ID\tPRODUCER-EPOCH\tTIMEOUT-MS\tSTART-TIMESTAMP\tTOPICS\tERROR",
	"txn.describe-producers":                      "TOPIC\tPARTITION\tPRODUCER-ID\tPRODUCER-EPOCH\tLAST-SEQUENCE\tLAST-TIMESTAMP\tCOORDINATOR-EPOCH\tTXN-START-OFFSET\tERROR\tMESSAGE",
	"txn.list":                                    "BROKER\tTRANSACTIONAL-ID\tPRODUCER-ID\tSTATE\tERROR",
	"user.list":                                   "USER\tMECHANISM\tITERATIONS\tERROR\tMESSAGE",
	"version":                                     "KEY\tVALUE",
}

// walkthroughKeys is the JSON key set of every leaf the walkthrough runs, as
// keySet renders the document: the sorted keys of the top level and of every
// object below it, the first element standing for an array. It is the other
// half of the scripting contract: a key renamed, added, or dropped fails
// here, and a rename or removal is a BREAKING entry in the release notes.
var walkthroughKeys = map[string]string{
	"acl.create":                                  ".: _command _version dry_run results; results[]: error host message name operation pattern permission principal type",
	"acl.delete":                                  ".: _command _version deleted dry_run; deleted[]: error host message name operation pattern permission principal type",
	"acl.list":                                    ".: _command _version acls; acls[]: host name operation pattern permission principal type",
	"client-metrics.describe":                     ".: _command _version configs; configs[]: key source value",
	"client-metrics.list":                         ".: _command _version subscriptions; subscriptions[]: name",
	"cluster.describe --section brokers":          ".: _command _version authorized_operations brokers cluster_id controller_id; brokers[]: host id port rack",
	"cluster.describe --section cluster":          ".: _command _version authorized_operations brokers cluster_id controller_id; brokers[]: host id port rack",
	"cluster.describe":                            ".: _command _version authorized_operations brokers cluster_id controller_id; brokers[]: host id port rack",
	"cluster.describe-quorum --section observers": ".: _command _version code error",
	"cluster.describe-quorum --section voters":    ".: _command _version code error",
	"cluster.describe-quorum":                     ".: _command _version code error",
	"cluster.elect-leaders":                       ".: _command _version dry_run results; results[]: error message partition topic",
	"cluster.features.describe":                   ".: _command _version features; features[]: description epoch feature finalized supported_max supported_min",
	"cluster.metadata --section brokers":          ".: _command _version brokers; brokers[]: host id port rack",
	"cluster.metadata --section cluster":          ".: _command _version cluster_id controller_id",
	"cluster.metadata --section topics":           ".: _command _version topics; topics[]: error internal partition_count replication_factor topic topic_id",
	"cluster.metadata":                            ".: _command _version brokers cluster_id controller_id topics; brokers[]: host id port rack; topics[]: error internal partition_count replication_factor topic topic_id",
	"config.alter":                                ".: _command _version dry_run results; results[]: error message resource",
	"config.describe":                             ".: _command _version configs; configs[]: key read_only resource source type value",
	"dtoken.describe":                             ".: _command _version code error",
	"group.delete":                                ".: _command _version dry_run results; results[]: broker error group message",
	"group.describe --by group":                   ".: _command _version groups; groups[]: group member_count partition_count state total_lag",
	"group.describe --by member":                  ".: _command _version groups; groups[]: balancer coordinator error group lag members message state total_lag; groups[].lag[]: client_id host instance_id lag member_id partitions rack",
	"group.describe --by topic":                   ".: _command _version groups; groups[]: balancer coordinator error group lag members message state total_lag; groups[].lag[]: lag partitions topic",
	"group.describe --consumer-protocol":          ".: _command _version groups; groups[]: balancer coordinator error group lag members message state total_lag; groups[].lag[]: client_id current_offset error host instance_id lag log_end_offset log_start_offset member_id message partition rack topic; groups[].members[]: assignment client_id host instance_id member_epoch member_id rack subscribed_topics target_assignment",
	"group.describe --instance-ids":               ".: _command _version groups; groups[]: balancer coordinator error group lag members message state total_lag; groups[].lag[]: client_id current_offset error host instance_id lag log_end_offset log_start_offset member_id message partition rack topic; groups[].members[]: assignment client_id host instance_id member_epoch member_id rack subscribed_topics target_assignment",
	"group.describe --lag >0":                     ".: _command _version groups; groups[]: balancer coordinator error group lag members message state total_lag; groups[].lag[]: client_id current_offset error host instance_id lag log_end_offset log_start_offset member_id message partition rack topic; groups[].members[]: assignment client_id host instance_id member_epoch member_id rack subscribed_topics target_assignment",
	"group.describe --section lag":                ".: _command _version groups; groups[]: balancer coordinator error group lag members message state total_lag; groups[].lag[]: client_id current_offset error host instance_id lag log_end_offset log_start_offset member_id message partition rack topic",
	"group.describe --section members":            ".: _command _version groups; groups[]: balancer coordinator error group lag members message state total_lag; groups[].lag[]: client_id current_offset error host instance_id lag log_end_offset log_start_offset member_id message partition rack topic; groups[].members[]: assignment client_id host instance_id member_epoch member_id rack subscribed_topics target_assignment",
	"group.describe --section summary":            ".: _command _version groups; groups[]: balancer coordinator error group lag members message state total_lag; groups[].lag[]: client_id current_offset error host instance_id lag log_end_offset log_start_offset member_id message partition rack topic",
	"group.describe":                              ".: _command _version groups; groups[]: balancer coordinator error group lag members message state total_lag; groups[].lag[]: client_id current_offset error host instance_id lag log_end_offset log_start_offset member_id message partition rack topic",
	"group.list --state empty":                    ".: _command _version groups; groups[]: broker error group group_type proto_type state",
	"group.list":                                  ".: _command _version groups; groups[]: broker error group group_type proto_type state",
	"group.seek":                                  ".: _command _version dry_run group plan results; plan[]: error message new_offset partition prior_offset topic",
	"logdirs.describe --aggregate-into broker":    ".: _command _version dirs; dirs[]: broker size",
	"logdirs.describe --aggregate-into dir":       ".: _command _version dirs; dirs[]: dir size",
	"logdirs.describe --aggregate-into topic":     ".: _command _version dirs; dirs[]: size topic",
	"logdirs.describe":                            ".: _command _version dirs; dirs[]: broker cordoned dir error is_future offset_lag partition size topic total usable",
	"misc.api-versions":                           ".: _command _version api_versions; api_versions[]: key max name",
	"misc.errcode":                                ".: _command _version description error_code name",
	"misc.errtext":                                ".: _command _version description error_code name",
	"misc.offset-for-leader-epoch":                ".: _command _version epochs; epochs[]: broker end_offset error leader_epoch partition topic",
	"misc.probe-version":                          ".: _command _version guess max min",
	"misc.raw-req":                                ".: _command _version response; response: ApiKeys ErrorCode FinalizedFeatures FinalizedFeaturesEpoch SupportedFeatures ThrottleMillis UnknownTags Version ZkMigrationReady; response.ApiKeys[]: ApiKey MaxVersion MinVersion UnknownTags; response.ApiKeys[].UnknownTags: ; response.FinalizedFeatures[]: MaxVersionLevel MinVersionLevel Name UnknownTags; response.FinalizedFeatures[].UnknownTags: ; response.SupportedFeatures[]: MaxVersion MinVersion Name UnknownTags; response.SupportedFeatures[].UnknownTags: ; response.UnknownTags: ",
	"profile.create":                              ".: _command _version current path profile",
	"profile.current":                             ".: _command _version profile",
	"profile.delete":                              ".: _command _version current path profile",
	"profile.dump":                                ".: _command _version config; config: broker_timeout registry seed_brokers; config.registry: urls",
	"profile.list":                                ".: _command _version profiles; profiles[]: current name",
	"profile.rename":                              ".: _command _version current path profile",
	"profile.set":                                 ".: _command _version current path profile",
	"profile.use":                                 ".: _command _version current path profile",
	"quota.alter":                                 ".: _command _version dry_run results; results[]: entity error message",
	"quota.describe":                              ".: _command _version quotas; quotas[]: entity key value",
	"reassign.list":                               ".: _command _version reassignments",
	"registry.compatibility.get":                  ".: _command _version compatibility; compatibility[]: error level subject",
	"registry.context.list":                       ".: _command _version contexts; contexts[]: context",
	"registry.mode.get":                           ".: _command _version modes; modes[]: error mode subject",
	"registry.schema.check-compatibility":         ".: _command _version compatible messages subject version",
	"registry.schema.get":                         ".: _command _version id references schema subject type version",
	"registry.schema.list":                        ".: _command _version schemas; schemas[]: id subject type version",
	"registry.schema.references":                  ".: _command _version references; references[]: id subject version",
	"registry.subject.list":                       ".: _command _version subjects; subjects[]: subject",
	"share-group.delete":                          ".: _command _version dry_run results; results[]: broker error group message",
	"share-group.describe --section members":      ".: _command _version groups; groups[]: assignment_epoch assignor coordinator epoch error group members message offsets state total_lag; groups[].members[]: assignment client_id host member_epoch member_id rack subscribed_topics; groups[].offsets[]: error lag leader_epoch message partition start_offset topic",
	"share-group.describe --section offsets":      ".: _command _version groups; groups[]: assignment_epoch assignor coordinator epoch error group members message offsets state total_lag; groups[].members[]: assignment client_id host member_epoch member_id rack subscribed_topics; groups[].offsets[]: error lag leader_epoch message partition start_offset topic",
	"share-group.describe --section summary":      ".: _command _version groups; groups[]: assignment_epoch assignor coordinator epoch error group members message offsets state total_lag; groups[].members[]: assignment client_id host member_epoch member_id rack subscribed_topics; groups[].offsets[]: error lag leader_epoch message partition start_offset topic",
	"share-group.describe":                        ".: _command _version groups; groups[]: assignment_epoch assignor coordinator epoch error group members message offsets state total_lag; groups[].members[]: assignment client_id host member_epoch member_id rack subscribed_topics; groups[].offsets[]: error lag leader_epoch message partition start_offset topic",
	"share-group.list":                            ".: _command _version groups; groups[]: broker error group state",
	"share-group.seek":                            ".: _command _version dry_run group plan results; plan[]: error message new_offset partition prior_offset topic",
	"topic.create":                                ".: _command _version dry_run topics; topics[]: error message topic topic_id",
	"topic.delete":                                ".: _command _version dry_run topics; topics[]: error message topic",
	"topic.describe --section configs":            ".: _command _version topics; topics[]: configs error internal partition_count replication_factor topic topic_id; topics[].configs[]: key sensitive source value",
	"topic.describe --section partitions":         ".: _command _version topics; topics[]: error internal partition_count partitions replication_factor topic topic_id; topics[].partitions[]: end_offset error isr leader leader_epoch offline_replicas partition replicas stable_offset start_offset",
	"topic.describe --section summary":            ".: _command _version topics; topics[]: error internal partition_count replication_factor topic topic_id",
	"topic.describe --stable":                     ".: _command _version topics; topics[]: configs error internal partition_count partitions replication_factor topic topic_id; topics[].configs[]: key sensitive source value; topics[].partitions[]: end_offset error isr leader leader_epoch offline_replicas partition replicas stable_offset start_offset",
	"topic.describe --under-replicated":           ".: _command _version topics; topics[]: configs error internal partition_count partitions replication_factor topic topic_id; topics[].configs[]: key sensitive source value",
	"topic.describe --with-overrides":             ".: _command _version topics; topics[]: configs error internal partition_count partitions replication_factor topic topic_id; topics[].configs[]: key sensitive source value; topics[].partitions[]: end_offset error isr leader leader_epoch offline_replicas partition replicas stable_offset start_offset",
	"topic.describe":                              ".: _command _version topics; topics[]: configs error internal partition_count partitions replication_factor topic topic_id; topics[].configs[]: key sensitive source value; topics[].partitions[]: end_offset error isr leader leader_epoch offline_replicas partition replicas stable_offset start_offset",
	"topic.list -r":                               ".: _command _version topics; topics[]: error internal partition_count replication_factor topic topic_id",
	"topic.list":                                  ".: _command _version topics; topics[]: error internal partition_count replication_factor topic topic_id",
	"topic.list-offsets":                          ".: _command _version offsets; offsets[]: at broker end end_epoch error partition stable stable_epoch start start_epoch topic",
	"topic.trim-prefix":                           ".: _command _version dry_run plan results; plan[]: error message new_offset partition prior_offset topic",
	"txn.describe":                                ".: _command _version transactions; transactions[]: error producer_epoch producer_id start_timestamp state timeout_ms topics transactional_id; transactions[].topics[]: partitions topic",
	"txn.describe-producers":                      ".: _command _version producers; producers[]: coordinator_epoch error last_sequence last_timestamp message partition producer_epoch producer_id topic txn_start_offset",
	"txn.list":                                    ".: _command _version transactions; transactions[]: broker error producer_id state transactional_id",
	"user.list":                                   ".: _command _version credentials; credentials[]: error iterations mechanism message user",
	"version":                                     ".: _command _version build_date git_ref go_version os_arch version",
}

// walkthroughEmpty are the leaves that exit 0 with no awk row against the
// seeded fake, and why no row can be seeded. Every other leaf that exits 0
// must print a row, so that a check cannot pass on nothing: the fake is
// seeded with an ACL, a quota, a SCRAM user, a group, and a share group for
// the lists that would otherwise be empty.
var walkthroughEmpty = map[string]string{
	"reassign.list":                     "kfake has no reassignment in flight, and cannot start one",
	"topic.describe --under-replicated": "every replica of every seeded partition is in sync, and kfake cannot take one out",
}

func TestWalkthrough(t *testing.T) {
	t.Run("every leaf is listed or skipped", testEveryLeafClassified)

	w := newWalkthrough(t)
	w.shareOffsets = w.readShareOffsets(t)

	// Each group runs its leaves in parallel and returns when they are
	// all done, so the state check below sees every mutation's effect.
	t.Run("leaves", func(t *testing.T) {
		for _, leaf := range walkthroughLeaves {
			key := leafKey(leaf.path, leaf.variant)
			t.Run(key, func(t *testing.T) {
				t.Parallel()
				args := slices.Concat(strings.Split(leaf.path, "."), w.fillArgs(leaf.args), leaf.variant)

				text := w.run(t, leaf.stdin, slices.Concat(args, []string{"--format", "text"}))
				w.check(t, key, "text", leaf.exit, leaf.why, text)
				if text.code == out.ExitOK && strings.TrimSpace(text.stdout) == "" {
					w.errf(t, key, "text", text, "exit 0 with nothing on stdout")
				}

				js := w.run(t, leaf.stdin, slices.Concat(args, []string{"--format", "json"}))
				if w.check(t, key, "json", leaf.exit, leaf.why, js) {
					if doc, ok := w.checkJSON(t, key, leaf.path, js, false); ok {
						w.checkKeys(t, key, js, doc)
					}
				}

				awk := w.run(t, leaf.stdin, slices.Concat(args, []string{"--format", "awk"}))
				header := w.checkHeader(t, key, args)
				if w.check(t, key, "awk", leaf.exit, leaf.why, awk) {
					w.checkAWK(t, key, text.stdout, awk, header)
					if leaf.exit == out.ExitOK {
						w.checkNotEmpty(t, key, awk)
					}
				}
			})
		}
	})

	t.Run("mutations", func(t *testing.T) {
		for _, m := range walkthroughMutations {
			t.Run(m.path, func(t *testing.T) {
				t.Parallel()
				args := slices.Concat(strings.Split(m.path, "."), m.args)

				text := w.run(t, "", slices.Concat(args, []string{"--format", "text"}))
				if w.check(t, m.path, "text", out.ExitOK, "", text) && strings.TrimSpace(text.stdout) == "" {
					w.errf(t, m.path, "text", text, "exit 0 with nothing on stdout")
				}

				js := w.run(t, "", slices.Concat(args, []string{"--format", "json"}))
				if w.check(t, m.path, "json", out.ExitOK, "", js) {
					if doc, ok := w.checkJSON(t, m.path, m.path, js, true); ok {
						w.checkKeys(t, m.path, js, doc)
					}
				}

				awk := w.run(t, "", slices.Concat(args, []string{"--format", "awk"}))
				header := w.checkHeader(t, m.path, args)
				if w.check(t, m.path, "awk", out.ExitOK, "", awk) {
					w.checkAWK(t, m.path, text.stdout, awk, header)
					w.checkNotEmpty(t, m.path, awk)
				}
			})
		}
	})

	t.Run("nothing changed", w.testNothingChanged)

	t.Run("profile", func(t *testing.T) {
		for _, format := range []string{"text", "json", "awk"} {
			t.Run(format, func(t *testing.T) {
				t.Parallel()
				w.runProfileSteps(t, format, "", "")
			})
		}
	})

	t.Run("aliases", w.testAliases)

	// The aliases ran the dry-run forms under their old flags; a hidden
	// flag that skipped the dry run would show here.
	t.Run("nothing changed after the aliases", w.testNothingChanged)
}

// leafKey names a leaf and its variant: "group.describe --section summary".
func leafKey(path string, variant []string) string {
	return strings.TrimSpace(path + " " + strings.Join(variant, " "))
}

// runProfileSteps runs the profile mutators in order against a fresh config
// file. With alias set, the step whose path is target runs under the alias
// path instead, and its document must still name target.
func (w *walkthrough) runProfileSteps(t *testing.T, format, alias, target string) {
	t.Helper()
	cfgPath := filepath.Join(t.TempDir(), "config.toml")
	for _, step := range walkthroughProfileSteps {
		path := step.path
		if alias != "" && path == target {
			path = alias
		}
		args := slices.Concat([]string{"--config-path", cfgPath}, strings.Split(path, "."), step.args, []string{"--format", format})
		r := w.runArgs(t, "", args)
		if !w.check(t, path, format, out.ExitOK, "", r) {
			return
		}
		switch format {
		case "json":
			doc, ok := w.checkJSON(t, path, step.path, r, false)
			if !ok {
				return
			}
			w.checkKeys(t, step.path, r, doc)
		case "awk":
			header := w.checkHeader(t, step.path, args)
			w.checkAWK(t, path, "", r, header)
			w.checkNotEmpty(t, step.path, r)
		}
	}
}

// testEveryLeafClassified names any command the lists above miss, and any
// entry of walkthroughHeaders or walkthroughEmpty that no leaf runs. Hidden
// and deprecated commands are walked by testAliases: they exist so that an
// old script keeps working, and their document must name the command they
// forward to.
func testEveryLeafClassified(t *testing.T) {
	classified := make(map[string]bool)
	keys := make(map[string]bool)
	for _, leaf := range walkthroughLeaves {
		classified[leaf.path] = true
		keys[leafKey(leaf.path, leaf.variant)] = true
	}
	for _, m := range walkthroughMutations {
		if classified[m.path] {
			t.Errorf("%s is both walked and mutated", m.path)
		}
		classified[m.path] = true
		keys[m.path] = true
	}
	for _, step := range walkthroughProfileSteps {
		classified[step.path] = true
		keys[step.path] = true
	}
	for _, skip := range walkthroughSkips {
		if skip.why == "" {
			t.Errorf("%s is skipped with no reason", skip.path)
		}
		if classified[skip.path] {
			t.Errorf("%s is both walked and skipped", skip.path)
		}
		classified[skip.path] = true
	}
	for key := range walkthroughHeaders {
		if !keys[key] {
			t.Errorf("walkthroughHeaders has %q, which no leaf runs", key)
		}
	}
	for key := range walkthroughKeys {
		if !keys[key] {
			t.Errorf("walkthroughKeys has %q, which no leaf runs", key)
		}
	}
	for key := range keys {
		if _, ok := walkthroughHeaders[key]; !ok {
			t.Errorf("%s has no entry in walkthroughHeaders; add its --format awk-header line", key)
		}
		if _, ok := walkthroughKeys[key]; !ok {
			t.Errorf("%s has no entry in walkthroughKeys; add its JSON key set", key)
		}
	}
	for key, columns := range walkthroughFreeText {
		if !keys[key] {
			t.Errorf("walkthroughFreeText has %q, which no leaf runs", key)
		}
		if len(columns) == 0 {
			t.Errorf("walkthroughFreeText has %q with no column", key)
		}
	}
	for key, why := range walkthroughEmpty {
		if !keys[key] {
			t.Errorf("walkthroughEmpty has %q, which no leaf runs", key)
		}
		if why == "" {
			t.Errorf("%s is expected empty with no reason", key)
		}
	}

	var hidden func(*cobra.Command) bool
	hidden = func(cmd *cobra.Command) bool {
		return cmd.Hidden || cmd.Deprecated != "" || cmd.HasParent() && hidden(cmd.Parent())
	}

	root, _ := buildRoot()
	var leaves int
	allCommands(root, func(cmd *cobra.Command) {
		if cmd.HasSubCommands() || hidden(cmd) {
			return
		}
		leaves++
		path := out.CommandName(cmd.CommandPath())
		if !classified[path] {
			t.Errorf("%s is in neither list; add it to walkthroughLeaves, or to walkthroughSkips with a reason", path)
		}
		delete(classified, path)
	})
	for path := range classified {
		t.Errorf("%s is listed but is not a leaf of the tree", path)
	}
	if leaves == 0 {
		t.Error("no leaves found; the walk is not reaching them")
	}
}

type walkthrough struct {
	exe        string
	cfgPath    string
	schemaFile string

	// shareOffsets is the share group's offsets section as seeded, read
	// back before anything ran, for the state check to compare against.
	shareOffsets string
}

// newWalkthrough seeds one kfake cluster and one srfake registry with enough
// to read back: topics carrying records, a group with committed offsets, a
// group with a live static member and a consumer protocol group with a live
// member, a share group that fetched the records with a member still in it,
// a transaction left open, an ACL, a quota, a SCRAM user, and a registered
// schema with one referencing it. It writes a config file naming both, so
// that every command reaches them the way a profile does.
func newWalkthrough(t *testing.T) *walkthrough {
	t.Helper()

	c, err := kfake.NewCluster()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	reg := srfake.New()
	t.Cleanup(reg.Close)

	kcl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(kcl.Close)

	ctx := t.Context()
	adm := kadm.NewClient(kcl)
	// walkTopic has two partitions on every broker, so that a replica
	// list has more than one element and a partition more than one row;
	// walkOther has one of each.
	if _, err := adm.CreateTopics(ctx, 2, -1, nil, walkTopic); err != nil {
		t.Fatal(err)
	}
	// walkOther carries a config override, so that topic describe
	// --with-overrides has a topic to keep.
	retention := "60000"
	if _, err := adm.CreateTopics(ctx, 1, 1, map[string]*string{"retention.ms": &retention}, walkOther); err != nil {
		t.Fatal(err)
	}
	partitions := map[string]int32{walkTopic: 2, walkOther: 1}
	ends := make(map[string]map[int32]int64)
	for topic, n := range partitions {
		ends[topic] = make(map[int32]int64)
		for i := range walkRecords {
			r := &kgo.Record{
				Topic:     topic,
				Partition: int32(i) % n,
				Key:       fmt.Appendf(nil, "id-%d", i),
				Value:     fmt.Appendf(nil, `{"id":"id-%d","count":%d}`, i, i),
			}
			if res := kcl.ProduceSync(ctx, r); res.FirstErr() != nil {
				t.Fatal(res.FirstErr())
			}
			ends[topic][r.Partition]++
		}
	}

	// The group has every partition of walkTopic committed at its end.
	var offsets kadm.Offsets
	for p, end := range ends[walkTopic] {
		offsets.Add(kadm.Offset{Topic: walkTopic, Partition: p, At: end, LeaderEpoch: -1})
	}
	resp, err := adm.CommitOffsets(ctx, walkGroup, offsets)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.Error(); err != nil {
		t.Fatal(err)
	}

	// walkMember has one static member, and walk848 one member on the
	// consumer protocol; both stay for the run, commit nothing, and read
	// from the end, so the lag rows are the whole log. The members
	// section has a row to print, and seek --dry-run, which needs an
	// Empty group, runs against walkGroup.
	joinWalkGroup(t, c, walkMember, walkInstance, false)
	joinWalkGroup(t, c, walk848, "", true)

	// The share group fetched every record of walkTopic and left, so its
	// start offsets are settled; a second member then joins and stays,
	// so the members section has a row. Share groups start at the end of
	// the log unless told otherwise.
	c.SetGroupConfigs(walkShare, map[string]string{"share.auto.offset.reset": "earliest"})
	share, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ShareGroup(walkShare), kgo.ConsumeTopics(walkTopic))
	if err != nil {
		t.Fatal(err)
	}
	shareCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	for got := 0; got < walkRecords; {
		fs := share.PollFetches(shareCtx)
		if err := fs.Err(); err != nil {
			t.Fatalf("share group %s: %v", walkShare, err)
		}
		got += fs.NumRecords()
	}
	cancel()
	share.Close()
	member, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ShareGroup(walkShare), kgo.ConsumeTopics(walkTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(member.Close)
	shareCtx, cancel = context.WithTimeout(ctx, 10*time.Second)
	if fs := member.PollFetches(shareCtx); fs.Err() != nil && !errors.Is(fs.Err(), context.DeadlineExceeded) {
		t.Fatalf("share group %s member: %v", walkShare, fs.Err())
	}
	cancel()

	// A transaction stays open on walkOther for the run, so that the txn
	// commands have a transaction and a producer to describe. walkTopic
	// is left alone: its end offsets are what the group committed.
	txn, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.TransactionalID(walkTxn), kgo.TransactionTimeout(5*time.Minute), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(txn.Close)
	if err := txn.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if res := txn.ProduceSync(ctx, &kgo.Record{Topic: walkOther, Partition: 0, Value: []byte("in flight")}); res.FirstErr() != nil {
		t.Fatal(res.FirstErr())
	}

	acls, err := adm.CreateACLs(ctx, kadm.NewACLs().Topics(walkTopic).Allow(walkPrincipal).AllowHosts().Operations(kadm.OpRead).ResourcePatternType(kadm.ACLPatternLiteral))
	if err != nil {
		t.Fatal(err)
	}
	for _, acl := range acls {
		if acl.Err != nil {
			t.Fatal(acl.Err)
		}
	}
	user := walkUser
	quotas, err := adm.AlterClientQuotas(ctx, []kadm.AlterClientQuotaEntry{{
		Entity: kadm.ClientQuotaEntity{{Type: "user", Name: &user}},
		Ops:    []kadm.AlterClientQuotaOp{{Key: "producer_byte_rate", Value: 1048576}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, quota := range quotas {
		if quota.Err != nil {
			t.Fatal(quota.Err)
		}
	}
	// kadm has no client metrics helper; one IncrementalAlterConfigs on the
	// CLIENT_METRICS resource creates the subscription.
	metricsReq := kmsg.NewPtrIncrementalAlterConfigsRequest()
	metricsRes := kmsg.NewIncrementalAlterConfigsRequestResource()
	metricsRes.ResourceType = kmsg.ConfigResourceTypeClientMetrics
	metricsRes.ResourceName = "walk-metrics"
	metricsCfg := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	metricsCfg.Name = "interval.ms"
	metricsCfg.Op = kmsg.IncrementalAlterConfigOpSet
	metricsCfg.Value = kmsg.StringPtr("30000")
	metricsRes.Configs = append(metricsRes.Configs, metricsCfg)
	metricsReq.Resources = append(metricsReq.Resources, metricsRes)
	metricsResp, err := metricsReq.RequestWith(ctx, kcl)
	if err != nil {
		t.Fatal(err)
	}
	if ec := metricsResp.Resources[0].ErrorCode; ec != 0 {
		t.Fatalf("client metrics seed: error code %d", ec)
	}
	scrams, err := adm.AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{User: walkUser, Mechanism: kadm.ScramSha256, Iterations: 4096, Password: "secret"}})
	if err != nil {
		t.Fatal(err)
	}
	if err := scrams.Error(); err != nil {
		t.Fatal(err)
	}

	const schema = `{"type":"record","name":"Demo","fields":[{"name":"id","type":"string"},{"name":"count","type":"int"}]}`
	scl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := scl.CreateSchema(ctx, walkSubject, sr.Schema{Schema: schema, Type: sr.TypeAvro}); err != nil {
		t.Fatal(err)
	}
	const refer = `{"type":"record","name":"Wrap","fields":[{"name":"demo","type":"Demo"}]}`
	if _, err := scl.CreateSchema(ctx, walkRefer, sr.Schema{Schema: refer, Type: sr.TypeAvro, References: []sr.SchemaReference{{Name: "Demo", Subject: walkSubject, Version: 1}}}); err != nil {
		t.Fatal(err)
	}

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	w := &walkthrough{
		exe:        exe,
		cfgPath:    filepath.Join(dir, "config.toml"),
		schemaFile: filepath.Join(dir, "schema.avsc"),
	}
	cfg := fmt.Sprintf(`current_profile = %q

[profiles.%s]
seed_brokers = [%q]

[profiles.%s.registry]
urls = [%q]
`, walkProfile, walkProfile, c.ListenAddrs()[0], walkProfile, reg.URL())
	if err := os.WriteFile(w.cfgPath, []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(w.schemaFile, []byte(schema), 0o600); err != nil {
		t.Fatal(err)
	}
	return w
}

// joinWalkGroup joins group as a member of walkTopic and returns once the
// member holds an assignment. The member stays until the test ends, commits
// nothing, and reads from the end, so nothing is fetched. With instance set
// it is a static member; with consumer set it speaks the KIP-848 protocol.
func joinWalkGroup(t *testing.T, c *kfake.Cluster, group, instance string, consumer bool) {
	t.Helper()
	assigned := make(chan struct{})
	var once sync.Once
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(walkTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) {
			once.Do(func() { close(assigned) })
		}),
	}
	if instance != "" {
		opts = append(opts, kgo.InstanceID(instance))
	}
	if consumer {
		opts = append(opts, kgo.WithContext(context.WithValue(context.Background(), "opt_in_kafka_next_gen_balancer_beta", true)))
	}
	m, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)

	// The first poll joins the group.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.PollFetches(ctx)
	}()
	select {
	case <-assigned:
	case <-ctx.Done():
		t.Fatalf("group %s: no assignment within 10s", group)
	}
	cancel()
	<-done
}

// fillArgs replaces the placeholders a table cannot know at run time.
func (w *walkthrough) fillArgs(args []string) []string {
	out := slices.Clone(args)
	for i, a := range out {
		if a == schemaFileArg {
			out[i] = w.schemaFile
		}
	}
	return out
}

type runResult struct {
	stdout string
	stderr string
	code   int
}

// run runs one kcl in a child process, pointed at the seeded cluster.
func (w *walkthrough) run(t *testing.T, stdin string, args []string) runResult {
	t.Helper()
	return w.runArgs(t, stdin, slices.Concat([]string{"--config-path", w.cfgPath}, args))
}

// runArgs runs one kcl in a child process with exactly these arguments.
func (w *walkthrough) runArgs(t *testing.T, stdin string, full []string) runResult {
	t.Helper()

	enc, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, w.exe)
	// A KCL_ variable in the environment this test was run from would
	// override the config we just wrote, so the child gets none of them.
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "KCL_") {
			cmd.Env = append(cmd.Env, kv)
		}
	}
	cmd.Env = append(cmd.Env, walkthroughEnv+"="+string(enc))
	cmd.Stdin = strings.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	r := runResult{}
	err = cmd.Run()
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		r.code = exit.ExitCode()
	default:
		t.Fatalf("unable to run kcl %s: %v", strings.Join(full, " "), err)
	}
	r.stdout, r.stderr = stdout.String(), stderr.String()
	return r
}

// awkRows splits awk stdout into its rows and fields.
func awkRows(stdout string) [][]string {
	var rows [][]string
	for line := range strings.SplitSeq(strings.TrimSuffix(stdout, "\n"), "\n") {
		if line != "" {
			rows = append(rows, strings.Split(line, "\t"))
		}
	}
	return rows
}

// checkHeader runs --format awk-header for the command args name and pins
// it against walkthroughHeaders under key: the exact line, or nothing for a
// leaf with no table. It runs in a child like everything else, so it is what
// a script would see, and with no config file, since the header depends on
// no config and the developer's real one must not fail the test. It returns
// the header's fields, nil when there are none.
func (w *walkthrough) checkHeader(t *testing.T, key string, args []string) []string {
	t.Helper()
	got, ok := w.awkHeader(t, key, args)
	if !ok {
		return nil
	}
	if want, ok := walkthroughHeaders[key]; !ok {
		w.errf(t, key, "awk-header", runResult{stdout: got}, "has no entry in walkthroughHeaders")
	} else if got != want {
		w.errf(t, key, "awk-header", runResult{stdout: got}, "prints %q, want %q; a column change is BREAKING and updates walkthroughHeaders", got, want)
	}
	if got == "" {
		return nil
	}
	return strings.Split(got, "\t")
}

// awkHeader is what --format awk-header prints for args, without a trailing
// newline, and false when the run failed or wrote anything to stderr but a
// deprecation notice.
func (w *walkthrough) awkHeader(t *testing.T, key string, args []string) (string, bool) {
	t.Helper()
	r := w.runArgs(t, "", slices.Concat([]string{"--no-config-file"}, args, []string{"--format", "awk-header"}))
	// cobra notes a deprecated command or flag on stderr; an alias is
	// expected to.
	var stderr []string
	for line := range strings.SplitSeq(strings.TrimSuffix(r.stderr, "\n"), "\n") {
		if line != "" && !strings.Contains(line, " is deprecated, ") && !strings.Contains(line, " has been deprecated, ") {
			stderr = append(stderr, line)
		}
	}
	if r.code != out.ExitOK || len(stderr) > 0 {
		w.errf(t, key, "awk-header", r, "exit %d, want 0 and nothing on stderr", r.code)
		return "", false
	}
	return strings.TrimSuffix(r.stdout, "\n"), true
}

// checkNotEmpty pins that a leaf that exits 0 prints a row under awk, unless
// walkthroughEmpty says why it cannot, and that one listed there is still
// empty, so the list does not go stale.
func (w *walkthrough) checkNotEmpty(t *testing.T, key string, r runResult) {
	t.Helper()
	_, expectEmpty := walkthroughEmpty[key]
	empty := strings.TrimSpace(r.stdout) == ""
	switch {
	case empty && !expectEmpty:
		w.errf(t, key, "awk", r, "exit 0 with no row; seed the fake so it prints one, or add it to walkthroughEmpty with the reason it cannot")
	case !empty && expectEmpty:
		w.errf(t, key, "awk", r, "prints a row now; drop it from walkthroughEmpty")
	}
}

// check reports the exit code, returning whether it is the one we expect.
func (w *walkthrough) check(t *testing.T, path, format string, want int, why string, r runResult) bool {
	t.Helper()
	if r.code == want {
		return true
	}
	if why != "" {
		w.errf(t, path, format, r, "exit %d, want %d (%s)", r.code, want, why)
	} else {
		w.errf(t, path, format, r, "exit %d, want %d", r.code, want)
	}
	return false
}

// checkJSON pins the contract every JSON document carries: one line, the
// command that printed it, which is the new path when it ran under an old
// name, and "dry_run":true when the run was one. It returns the document.
func (w *walkthrough) checkJSON(t *testing.T, key, command string, r runResult, dryRun bool) (map[string]any, bool) {
	t.Helper()
	body := strings.TrimSuffix(r.stdout, "\n")
	if body == "" {
		w.errf(t, key, "json", r, "nothing on stdout")
		return nil, false
	}
	if strings.Contains(body, "\n") {
		w.errf(t, key, "json", r, "stdout is %d lines, want one", strings.Count(body, "\n")+1)
		return nil, false
	}
	var doc map[string]any
	if err := json.Unmarshal([]byte(body), &doc); err != nil {
		w.errf(t, key, "json", r, "stdout is not JSON: %v", err)
		return nil, false
	}
	if _, ok := doc["_version"]; !ok {
		w.errf(t, key, "json", r, "document has no _version")
	}
	if got, _ := doc["_command"].(string); got != command {
		w.errf(t, key, "json", r, "_command is %q, want %q", got, command)
	}
	if dryRun && doc["dry_run"] != true {
		w.errf(t, key, "json", r, "dry_run is %v, want true", doc["dry_run"])
	}
	return doc, true
}

// checkAWK pins the scripting contract: every row carries the same fields,
// as many as the header --format awk-header prints for the command, and the
// header the text format prints is not one of them. A command that prints
// rows must have registered its columns, so that a script can learn them.
// No field is empty, since awk's default splitting would lose it, and none
// holds a space, a bracket, or a "<nil>", which are a Go value printed raw:
// a list is comma joined and an unknown is "-". A column that is free text
// in walkthroughFreeText may hold spaces and brackets.
func (w *walkthrough) checkAWK(t *testing.T, path, text string, r runResult, header []string) {
	t.Helper()
	body := strings.TrimSuffix(r.stdout, "\n")
	if body == "" {
		return
	}
	rows := strings.Split(body, "\n")
	want := strings.Count(rows[0], "\t") + 1
	if header == nil && strings.HasPrefix(body, "{") {
		// misc raw-req has no table: a raw response is a JSON document in
		// every format.
		return
	}
	if header == nil {
		w.errf(t, path, "awk", r, "prints rows but --format awk-header prints nothing; register the columns with out.Columns")
	} else if len(header) != want {
		w.errf(t, path, "awk", r, "--format awk-header has %d fields, row 0 has %d: %q vs %q", len(header), want, header, rows[0])
	}
	headers := headerLines(text)
	for i, row := range rows {
		fields := strings.Split(row, "\t")
		if len(fields) != want {
			w.errf(t, path, "awk", r, "row %d has %d fields, row 0 has %d: %q", i, len(fields), want, row)
		}
		if slices.Contains(headers, strings.Join(strings.Fields(row), " ")) {
			w.errf(t, path, "awk", r, "row %d is a header the text format prints: %q", i, row)
		}
		for j, field := range fields {
			column := ""
			if j < len(header) {
				column = header[j]
			}
			freeText := slices.Contains(walkthroughFreeText[path], column)
			switch {
			case field == "":
				w.errf(t, path, "awk", r, "row %d field %d (%s) is empty; an empty or unknown cell is \"-\": %q", i, j, column, row)
			case strings.Contains(field, "<nil>"):
				w.errf(t, path, "awk", r, "row %d field %d (%s) is a nil printed raw; an unknown cell is \"-\": %q", i, j, column, row)
			case strings.ContainsAny(field, "[]") && !freeText:
				w.errf(t, path, "awk", r, "row %d field %d (%s) holds a bracket; a list is comma joined: %q", i, j, column, row)
			case strings.Contains(field, " ") && !freeText:
				w.errf(t, path, "awk", r, "row %d field %d (%s) holds a space, which awk's default splitting breaks on; name the column in walkthroughFreeText if it is free text: %q", i, j, column, row)
			}
		}
	}
}

// walkthroughFreeText names, per leaf, the awk columns that are free text
// and may hold a space or a bracket: an error's description, a schema. Every
// other field is one word with no bracket, so that a script splitting on
// whitespace reads the columns it counted and a list is comma joined.
var walkthroughFreeText = map[string][]string{
	"cluster.features.describe": {"DESCRIPTION"},
	"misc.errcode":              {"DESCRIPTION"},
	"misc.errtext":              {"DESCRIPTION"},
	"registry.schema.get":       {"SCHEMA"},
}

// checkKeys pins the JSON key set of a document against walkthroughKeys
// under key: the keys of the top level and of every object below it, as
// keySet renders them. A key renamed, added, or dropped fails here and is
// listed in the release notes.
func (w *walkthrough) checkKeys(t *testing.T, key string, r runResult, doc map[string]any) {
	t.Helper()
	got := keySet(doc)
	if want, ok := walkthroughKeys[key]; !ok {
		w.errf(t, key, "json", r, "has no entry in walkthroughKeys; add:\n\t%q: %q,", key, got)
	} else if got != want {
		w.errf(t, key, "json", r, "keys are\n\t%q\nwant\n\t%q\na key change is BREAKING and updates walkthroughKeys", got, want)
	}
}

// keySet renders the keys of a document: the sorted keys of the top level,
// then of every object below it, the first element standing for an array,
// each object as "path: key key ...", the paths joined by "; ". An empty
// array has no element to name, so its keys are not listed.
func keySet(doc map[string]any) string {
	var sets []string
	var walk func(path string, m map[string]any)
	walk = func(path string, m map[string]any) {
		keys := slices.Sorted(maps.Keys(m))
		sets = append(sets, path+": "+strings.Join(keys, " "))
		for _, k := range keys {
			sub := path + "." + k
			if path == "." {
				sub = k
			}
			switch v := m[k].(type) {
			case map[string]any:
				walk(sub, v)
			case []any:
				if len(v) > 0 {
					if first, ok := v[0].(map[string]any); ok {
						walk(sub+"[]", first)
					}
				}
			}
		}
	}
	walk(".", doc)
	return strings.Join(sets, "; ")
}

// headerLines are the table headers the text format printed, as their words:
// the lines with every letter capitalized, which is what a header is and what
// a row of data is not. A command that prints sections has one per section.
func headerLines(text string) []string {
	var headers []string
	for line := range strings.SplitSeq(text, "\n") {
		if line == "" || line != strings.ToUpper(line) || !strings.ContainsFunc(line, unicode.IsLetter) {
			continue
		}
		headers = append(headers, strings.Join(strings.Fields(line), " "))
	}
	return headers
}

// readShareOffsets is the share group's offsets section as one line of
// JSON, the document's _command and _version aside, so that two reads
// compare as strings.
func (w *walkthrough) readShareOffsets(t *testing.T) string {
	t.Helper()
	r := w.run(t, "", []string{"share-group", "describe", walkShare, "--section", "offsets", "--format", "json"})
	if r.code != out.ExitOK {
		t.Fatalf("share-group describe %s: exit %d\n  stderr: %s", walkShare, r.code, r.stderr)
	}
	var doc struct {
		Groups []struct {
			Offsets json.RawMessage `json:"offsets"`
		} `json:"groups"`
	}
	if err := json.Unmarshal([]byte(r.stdout), &doc); err != nil {
		t.Fatalf("share-group describe %s: stdout is not JSON: %v\n%s", walkShare, err, r.stdout)
	}
	if len(doc.Groups) != 1 || len(doc.Groups[0].Offsets) == 0 {
		t.Fatalf("share-group describe %s: want one group with offsets: %s", walkShare, r.stdout)
	}
	return string(doc.Groups[0].Offsets)
}

// testNothingChanged reads the cluster back after the mutations ran as dry
// runs and declined prompts: what they would have created is absent, what
// they would have deleted or changed is as it was seeded. A mutation that
// changed something fails here, next to its cause, rather than as another
// leaf's flaky row.
func (w *walkthrough) testNothingChanged(t *testing.T) {
	// The share group's start offsets are where the seed left them.
	if got := w.readShareOffsets(t); got != w.shareOffsets {
		t.Errorf("share group %s offsets are %s, were %s; share-group seek --dry-run moved them", walkShare, got, w.shareOffsets)
	}

	rows := func(t *testing.T, args ...string) [][]string {
		t.Helper()
		r := w.run(t, "", append(args, "--format", "awk"))
		if r.code != out.ExitOK {
			t.Fatalf("kcl %s: exit %d\n  stderr: %s", strings.Join(args, " "), r.code, r.stderr)
		}
		return awkRows(r.stdout)
	}
	column := func(rows [][]string, i int) []string {
		var vals []string
		for _, row := range rows {
			vals = append(vals, row[i])
		}
		return vals
	}

	topics := column(rows(t, "topic", "list"), 0)
	for _, topic := range []string{walkTopic, walkOther} {
		if !slices.Contains(topics, topic) {
			t.Errorf("topic %s is gone: topic delete --dry-run deleted it", topic)
		}
	}
	if slices.Contains(topics, "walk-new") {
		t.Error("topic walk-new exists: topic create --dry-run created it")
	}

	if groups := column(rows(t, "group", "list"), 1); !slices.Contains(groups, walkGroup) {
		t.Errorf("group %s is gone: group delete --dry-run deleted it", walkGroup)
	}
	if groups := column(rows(t, "share-group", "list"), 1); !slices.Contains(groups, walkShare) {
		t.Errorf("share group %s is gone: share-group delete --dry-run deleted it", walkShare)
	}

	// The group's committed offsets are where they were seeded, at each
	// partition's end: CURRENT-OFFSET equals LOG-END-OFFSET.
	lag := rows(t, "group", "describe", walkGroup, "--section", "lag")
	if len(lag) != 2 {
		t.Errorf("group %s has %d lag rows, want one per partition of %s", walkGroup, len(lag), walkTopic)
	}
	for _, row := range lag {
		if row[3] != row[5] || row[6] != "0" {
			t.Errorf("group %s row %v: CURRENT-OFFSET %s, LOG-END-OFFSET %s, LAG %s; group seek --dry-run moved it", walkGroup, row, row[3], row[5], row[6])
		}
	}

	// Nothing was trimmed: every start offset is still 0.
	for _, row := range rows(t, "topic", "list-offsets", walkTopic) {
		if row[3] != "0" {
			t.Errorf("%s partition %s starts at %s, want 0; topic trim-prefix trimmed it", walkTopic, row[2], row[3])
		}
	}

	// The config alter dry run set nothing.
	for _, row := range rows(t, "config", "describe", walkTopic) {
		if row[1] == "retention.ms" && row[3] == "1000" {
			t.Errorf("%s retention.ms is 1000: config alter --dry-run set it", walkTopic)
		}
	}

	acls := rows(t, "acl", "list")
	var seeded, other bool
	for _, row := range acls {
		seeded = seeded || row[1] == walkTopic && row[3] == walkPrincipal
		other = other || row[1] == walkOther
	}
	if !seeded {
		t.Errorf("the ACL on %s for %s is gone: acl delete --dry-run deleted it: %q", walkTopic, walkPrincipal, acls)
	}
	if other {
		t.Errorf("an ACL on %s exists: acl create --dry-run created it: %q", walkOther, acls)
	}

	quotas := column(rows(t, "quota", "describe"), 0)
	if !slices.Contains(quotas, "{user="+walkUser+"}") {
		t.Errorf("the quota for %s is gone: %q", walkUser, quotas)
	}
	if slices.Contains(quotas, "{user=bob}") {
		t.Errorf("a quota for bob exists: quota alter --dry-run created it: %q", quotas)
	}

	if users := column(rows(t, "user", "list"), 0); !slices.Contains(users, walkUser) {
		t.Errorf("the SCRAM credential for %s is gone: %q", walkUser, users)
	}
}

// walkthroughAliasTargets names the leaf a hidden or deprecated leaf forwards
// to, for the ones --help-json cannot say. The rest are derived: a
// deprecation reading "use 'kcl X' instead" names X, and a hidden subtree is
// its top-level twin, admin.topic.list for topic.list and myconfig.use for
// profile.use.
var walkthroughAliasTargets = map[string]string{
	"admin.describe":          "cluster.describe",
	"admin.describe-quorum":   "cluster.describe-quorum",
	"admin.elect-leaders":     "cluster.elect-leaders",
	"admin.features.describe": "cluster.features.describe",
	"admin.features.update":   "cluster.features.update",
	"myconfig.setup":          "profile.create",
	"profile.setup":           "profile.create",
}

// walkthroughAliasArgs are the arguments an alias runs with when its target's
// own arguments do not fit it.
var walkthroughAliasArgs = map[string][]string{
	"registry.versions": {walkSubject}, // schema list lists every subject without one; versions needs its subject
}

// walkthroughAliasSkips are the hidden leaves the walkthrough does not run,
// and why. A hidden leaf whose target is in walkthroughSkips is skipped for
// the target's reason and needs no entry here.
var walkthroughAliasSkips = map[string]string{
	"myconfig.link":   "changes the config symlink in the home directory, and is not the profile use its deprecation names",
	"myconfig.unlink": "removes the config symlink in the home directory, and is not the profile use its deprecation names",
	"topic.consume":   "consume runs until it is interrupted; the consume package tests it",
	"topic.produce":   "produce reads records from stdin, and its --format is the record format",
}

// walkthroughAliasFlags run a leaf with a hidden or deprecated flag standing
// in for the flag or argument it was renamed to. The document names target,
// which is the leaf itself unless the flag forwards to another command, as
// --detailed does to topic describe.
var walkthroughAliasFlags = []struct {
	path   string
	args   []string
	target string
}{
	{"acl.create", []string{"--type", "topic", "--name", walkOther, "--principal", walkPrincipal, "--host", "*", "--op", "read", "--perm", "allow", "--dry-run"}, "acl.create"},
	{"acl.delete", []string{"--topic", walkTopic, "--op", "any", "--perm", "any", "--dry-run"}, "acl.delete"},
	{"acl.list", []string{"--op", "any", "--perm", "any"}, "acl.list"},
	{"cluster.metadata", []string{"--detailed"}, "topic.describe"},
	{"config.alter", []string{walkTopic, "--kv", "retention.ms=1000", "--dry-run"}, "config.alter"},
	{"group.list", []string{"--filter", "empty", "--type-filter", "classic"}, "group.list"},
	{"group.seek", []string{walkGroup, "--to", "start", "--topics", walkTopic, "--dry-run"}, "group.seek"},
	{"registry.schema.get", []string{"--subject", walkSubject}, "registry.schema.get"},
	{"share-group.list", []string{"--filter", "stable"}, "share-group.list"},
	{"share-group.seek", []string{walkShare, "--to", "start", "--topics", walkTopic, "--dry-run"}, "share-group.seek"},
	{"topic.create", []string{"walk-new", "--kv", "retention.ms=1000", "--dry-run"}, "topic.create"},
	{"topic.list", []string{"--detailed"}, "topic.describe"},
}

// helpLeaf is one leaf of --help-json.
type helpLeaf struct {
	path string
	cmd  commandJSON
}

// helpLeaves are the leaves of --help-json, run in a child, by their dotted
// path.
func (w *walkthrough) helpLeaves(t *testing.T) []helpLeaf {
	t.Helper()
	r := w.runArgs(t, "", []string{"--help-json"})
	if r.code != out.ExitOK {
		t.Fatalf("--help-json: exit %d\n  stderr: %s", r.code, r.stderr)
	}
	var tree helpJSON
	if err := json.Unmarshal([]byte(r.stdout), &tree); err != nil {
		t.Fatalf("--help-json is not JSON: %v", err)
	}
	var leaves []helpLeaf
	var walk func(path string, c commandJSON)
	walk = func(path string, c commandJSON) {
		if len(c.Commands) == 0 {
			leaves = append(leaves, helpLeaf{path, c})
			return
		}
		for name, sub := range c.Commands {
			walk(strings.TrimPrefix(path+"."+name, "."), sub)
		}
	}
	walk("", tree.commandJSON)
	slices.SortFunc(leaves, func(a, b helpLeaf) int { return strings.Compare(a.path, b.path) })
	return leaves
}

// aliasTarget is the leaf a hidden or deprecated leaf forwards to.
func aliasTarget(path string, c commandJSON, visible map[string]bool) (string, bool) {
	if target, ok := walkthroughAliasTargets[path]; ok {
		return target, true
	}
	if _, after, ok := strings.Cut(c.Deprecated, "'kcl "); ok {
		name, _, _ := strings.Cut(after, "'")
		var words []string
		for _, word := range strings.Fields(name) {
			if !strings.HasPrefix(word, "-") {
				words = append(words, word)
			}
		}
		if target := strings.Join(words, "."); visible[target] {
			return target, true
		}
	}
	if rest, ok := strings.CutPrefix(path, "admin."); ok && visible[rest] {
		return rest, true
	}
	if rest, ok := strings.CutPrefix(path, "myconfig."); ok && visible["profile."+rest] {
		return "profile." + rest, true
	}
	return "", false
}

// testAliases runs every hidden or deprecated leaf of --help-json under
// --format json with the arguments its target runs with, and pins that the
// document parses, exits as the target does, and names the target in
// _command, so that an old script keeps working and reads the new name; and
// that --format awk-header prints the target's header, so that the alias
// registered its columns. Then it runs every hidden or deprecated flag the
// same way, under awk too. A hidden leaf or flag that no list here accounts
// for fails, so a rename cannot arrive without its alias being walked.
func (w *walkthrough) testAliases(t *testing.T) {
	leaves := w.helpLeaves(t)
	visible := make(map[string]bool)
	hiddenLeaves := make(map[string]bool)
	for _, leaf := range leaves {
		if leaf.cmd.Hidden || leaf.cmd.Deprecated != "" {
			hiddenLeaves[leaf.path] = true
		} else {
			visible[leaf.path] = true
		}
	}
	if len(hiddenLeaves) == 0 {
		t.Fatal("--help-json has no hidden leaf; the walk is not reaching them")
	}
	for path := range walkthroughAliasSkips {
		if !hiddenLeaves[path] {
			t.Errorf("walkthroughAliasSkips has %s, which is not a hidden leaf", path)
		}
	}
	for path := range walkthroughAliasTargets {
		if !hiddenLeaves[path] {
			t.Errorf("walkthroughAliasTargets has %s, which is not a hidden leaf", path)
		}
	}
	for path := range walkthroughAliasArgs {
		if !hiddenLeaves[path] {
			t.Errorf("walkthroughAliasArgs has %s, which is not a hidden leaf", path)
		}
	}

	skips := make(map[string]string)
	for _, skip := range walkthroughSkips {
		skips[skip.path] = skip.why
	}
	// A leaf's run: its arguments and exit code, from whichever list runs
	// the target.
	type run struct {
		args  []string
		stdin string
		exit  int
	}
	runs := make(map[string]run)
	for _, leaf := range walkthroughLeaves {
		if len(leaf.variant) == 0 {
			runs[leaf.path] = run{w.fillArgs(leaf.args), leaf.stdin, leaf.exit}
		}
	}
	for _, m := range walkthroughMutations {
		runs[m.path] = run{args: m.args}
	}
	steps := make(map[string]bool)
	for _, step := range walkthroughProfileSteps {
		steps[step.path] = true
	}

	for _, leaf := range leaves {
		if !hiddenLeaves[leaf.path] {
			continue
		}
		if _, ok := walkthroughAliasSkips[leaf.path]; ok {
			continue
		}
		target, ok := aliasTarget(leaf.path, leaf.cmd, visible)
		if !ok {
			t.Errorf("%s is hidden and forwards to no command we know; add it to walkthroughAliasTargets or walkthroughAliasSkips", leaf.path)
			continue
		}
		if _, ok := skips[target]; ok {
			continue
		}
		t.Run(leaf.path, func(t *testing.T) {
			t.Parallel()
			w.checkAliasHeader(t, leaf.path, target, strings.Split(leaf.path, "."))
			if steps[target] {
				w.runProfileSteps(t, "json", leaf.path, target)
				return
			}
			r, ok := runs[target]
			if !ok {
				t.Fatalf("%s forwards to %s, which no list runs", leaf.path, target)
			}
			args := r.args
			if a, ok := walkthroughAliasArgs[leaf.path]; ok {
				args = a
			}
			res := w.run(t, r.stdin, slices.Concat(strings.Split(leaf.path, "."), args, []string{"--format", "json"}))
			if w.check(t, leaf.path, "json", r.exit, "the exit code "+target+" has", res) {
				w.checkJSON(t, leaf.path, target, res, false)
			}
		})
	}

	// Every hidden flag of a visible leaf is walked, unless the leaf is
	// skipped as a whole.
	walked := make(map[string]bool)
	for _, f := range walkthroughAliasFlags {
		for _, arg := range f.args {
			if name, ok := strings.CutPrefix(arg, "--"); ok {
				name, _, _ = strings.Cut(name, "=")
				walked[f.path+" --"+name] = true
			}
		}
	}
	for _, leaf := range leaves {
		if !visible[leaf.path] {
			continue
		}
		if _, ok := skips[leaf.path]; ok {
			continue
		}
		for name, flag := range leaf.cmd.Flags {
			if (flag.Hidden || flag.Deprecated != "") && !walked[leaf.path+" --"+name] {
				t.Errorf("%s --%s is hidden and no walkthroughAliasFlags entry runs it", leaf.path, name)
			}
		}
	}
	for _, f := range walkthroughAliasFlags {
		if !visible[f.path] {
			t.Errorf("walkthroughAliasFlags has %s, which is not a visible leaf", f.path)
			continue
		}
		t.Run(f.path+" "+strings.Join(f.args, " "), func(t *testing.T) {
			t.Parallel()
			args := slices.Concat(strings.Split(f.path, "."), f.args)
			res := w.run(t, "", slices.Concat(args, []string{"--format", "json"}))
			if w.check(t, f.path, "json", out.ExitOK, "", res) {
				w.checkJSON(t, f.path, f.target, res, false)
			}
			header := w.checkAliasHeader(t, f.path, f.target, args)
			awk := w.run(t, "", slices.Concat(args, []string{"--format", "awk"}))
			if w.check(t, f.path, "awk", out.ExitOK, "", awk) {
				w.checkAWK(t, f.target, "", awk, header)
				w.checkNotEmpty(t, f.target, awk)
			}
		})
	}
}

// checkAliasHeader pins that --format awk-header under an alias, a hidden
// leaf or a leaf under a hidden flag, prints the header its target does, so
// that an alias built without its registration cannot print rows a script
// cannot learn the columns of. It returns the header's fields.
func (w *walkthrough) checkAliasHeader(t *testing.T, path, target string, args []string) []string {
	t.Helper()
	got, ok := w.awkHeader(t, path, args)
	if !ok {
		return nil
	}
	want, ok := walkthroughHeaders[target]
	if !ok {
		t.Errorf("%s forwards to %s, which has no entry in walkthroughHeaders", path, target)
		return nil
	}
	if got != want {
		w.errf(t, path, "awk-header", runResult{stdout: got}, "prints %q, but its target %s prints %q", got, target, want)
	}
	if got == "" {
		return nil
	}
	return strings.Split(got, "\t")
}

// errf reports one failure and keeps going, so that one run lists everything
// that drifted rather than the first thing.
func (w *walkthrough) errf(t *testing.T, path, format string, r runResult, msg string, args ...any) {
	t.Helper()
	stdout, _, _ := strings.Cut(strings.TrimSuffix(r.stdout, "\n"), "\n")
	stderr, _, _ := strings.Cut(strings.TrimSuffix(r.stderr, "\n"), "\n")
	t.Errorf("kcl %s --format %s: %s\n  stdout: %s\n  stderr: %s",
		strings.ReplaceAll(path, ".", " "), format, fmt.Sprintf(msg, args...), stdout, stderr)
}
