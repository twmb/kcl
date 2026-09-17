// Package dtoken contains delegation token commands.
package dtoken

import (
	"cmp"
	"context"
	"encoding/base64"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "dtoken",
		Short: "Delegation token commands.",
		Long: `Delegation token commands.

Delegation tokens allow for (ideally) a quicker and easier method of enabling
authorization for a wide array of clients. Rather than having to manage many
accounts external to Kafka, you only need to manage a few accounts and then use
those accounts to create delegation tokens per client.

Delegation tokens inherit the same ACLs as the user creating the token. Thus,
if you want to properly scope ACLs, you should not use admin accounts to create
delegation tokens.

Delegation tokens have an inherent max lifetime timestamp. In clients, you will
need to program the ability to receive new tokens and use them before their
current tokens expire. Lastly, delegation tokens are usually granted with an
even shorter expiry timestamp. Tokens can be renewed to bump their expiry
timestamp until the max lifetime is reached; only token renewers can bump the
expiry timestamp.

As a client, you can use delegation tokens in SCRAM-SHA-256 or SCRAM-SHA-512
sasl authentication, and you must specify "tokenauth=true" with a scram
extension (kcl does this automatically with the sasl_scram_is_token option).

To enable delegation tokens in Kafka, use the delegation.token.master.key
setting. All brokers must use the same token master key.
`,
	}

	cmd.AddCommand(createTokenCommand(cl))
	cmd.AddCommand(renewTokenCommand(cl))
	cmd.AddCommand(expireTokenCommand(cl))
	cmd.AddCommand(describeTokensCommand(cl))

	return cmd
}

// The columns of a token: who it is for, when it was issued, when it
// expires, when it can no longer be renewed, and the credentials. describe
// adds who may renew it; create has no renewer list in its response.
var (
	tokenHeaders    = []string{"PRINCIPAL", "ISSUED", "EXPIRY", "MAX-AGE", "TOKEN-ID", "HMAC"}
	describeHeaders = append(slices.Clone(tokenHeaders), "RENEWERS")
	expiryHeaders   = []string{"EXPIRY", "ERROR", "MESSAGE"}
)

// principal prints a principal as Type:name.
func principal(typ, name string) string {
	return typ + ":" + name
}

// parsePrincipal splits Type:name, with User the type when none is given,
// which is the only type Kafka's SimpleAuthorizer has.
func parsePrincipal(s string) (typ, name string) {
	if delim := strings.IndexByte(s, ':'); delim != -1 {
		return s[:delim], s[delim+1:]
	}
	return "User", s
}

// createRow is the one row a create prints.
func createRow(resp *kmsg.CreateDelegationTokenResponse) []any {
	return []any{
		principal(resp.PrincipalType, resp.PrincipalName),
		millisToStr(resp.IssueTimestamp),
		millisToStr(resp.ExpiryTimestamp),
		millisToStr(resp.MaxTimestamp),
		resp.TokenID,
		base64.StdEncoding.EncodeToString(resp.HMAC),
	}
}

// describeRow is one row of a describe. RENEWERS is a list, comma joined in
// text and awk and an array in JSON; a token with no renewers of its own is
// renewed by its owner, so that is who is listed.
func describeRow(detail *kmsg.DescribeDelegationTokenResponseTokenDetail) []any {
	rs := make([]string, 0, len(detail.Renewers))
	for _, renewer := range detail.Renewers {
		rs = append(rs, principal(renewer.PrincipalType, renewer.PrincipalName))
	}
	if len(rs) == 0 {
		rs = append(rs, principal(detail.PrincipalType, detail.PrincipalName))
	}
	return []any{
		principal(detail.PrincipalType, detail.PrincipalName),
		millisToStr(detail.IssueTimestamp),
		millisToStr(detail.ExpiryTimestamp),
		millisToStr(detail.MaxTimestamp),
		detail.TokenID,
		base64.StdEncoding.EncodeToString(detail.HMAC),
		rs,
	}
}

// expiryRow is the one row a renew or expire prints: the new expiry, or the
// error that kept it where it was. Neither response carries a message.
func expiryRow(code int16, expiry int64) []any {
	if code != 0 {
		return []any{out.Unknown, out.ErrName(code), ""}
	}
	return []any{millisToStr(expiry), "", ""}
}

func createTokenCommand(cl *client.Client) *cobra.Command {
	var renewers []string
	var maxLifetimeMillis int64

	cmd := &cobra.Command{
		Use:     "create",
		Aliases: []string{"c"},
		Short:   "Create a delegation token.",
		Long: `Create a delegation token.

Requires Kafka 1.1.0+.

A delegation token inherits all ACLs from the creator. Without any manual
extra renewers, only the creator can renew.

Renewers can be specified either as "Type:name" or just "name". If eliding the
type, the client uses "User", which is the only type that exists in Kafka's
SimpleAuthorizer.

The token prints as one row: PRINCIPAL ISSUED EXPIRY MAX-AGE TOKEN-ID HMAC,
the times in UTC and the HMAC in base64. The token ID and HMAC are the
username and password for SCRAM authentication with the token.

EXAMPLES:
  kcl dtoken create -r admin1 -r User:admin2

SEE ALSO:
  kcl dtoken describe    describe tokens
  kcl dtoken renew       renew a token
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			req := &kmsg.CreateDelegationTokenRequest{
				MaxLifetimeMillis: maxLifetimeMillis,
			}
			for _, renewer := range renewers {
				ptyp, pname := parsePrincipal(renewer)
				req.Renewers = append(req.Renewers, kmsg.CreateDelegationTokenRequestRenewer{
					PrincipalType: ptyp,
					PrincipalName: pname,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to create delegation token: %v", err)
			}
			resp := kresp.(*kmsg.CreateDelegationTokenResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				return fmt.Errorf("%v", err)
			}

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "tokens", tokenHeaders...)
			table.Row(createRow(resp)...)
			return table.Flush()
		},
	}
	out.Columns(cmd, tokenHeaders...)

	cmd.Flags().Int64VarP(&maxLifetimeMillis, "max-lifetime-millis", "l", -1, "the maximum lifetime of this token, or -1 for the broker's delegation.token.max.lifetime.ms default")
	cmd.Flags().StringArrayVarP(&renewers, "renewer", "r", nil, "optional list of users allowed to renew this token, or empty to default to the token creator; repeatable")

	return cmd
}

func renewTokenCommand(cl *client.Client) *cobra.Command {
	var renewTimeMillis int64

	cmd := &cobra.Command{
		Use:   "renew HMAC",
		Short: "Renew a delegation token (Kafka 1.1.0+).",
		Long: `Renew a delegation token (Kafka 1.1.0+).

Renewing bumps a token's expiry timestamp, up to its max age. The token is
named by its base64 HMAC, as describe prints it. The result is one row: the
new EXPIRY, with ERROR and MESSAGE.

EXAMPLES:
  kcl dtoken renew 'base64 hmac' -t 3600000

SEE ALSO:
  kcl dtoken describe    describe tokens
  kcl dtoken expire      expire a token
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			decoded, err := base64.StdEncoding.DecodeString(args[0])
			if err != nil {
				return out.Errf(out.ExitUsage, "unable to base64 decode hmac: %v", err)
			}
			req := &kmsg.RenewDelegationTokenRequest{
				HMAC:            decoded,
				RenewTimeMillis: renewTimeMillis,
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to renew delegation token: %v", err)
			}
			resp := kresp.(*kmsg.RenewDelegationTokenResponse)

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results", expiryHeaders...).ResultColumns()
			table.Row(expiryRow(resp.ErrorCode, resp.ExpiryTimestamp)...)
			return table.Flush()
		},
	}
	out.Columns(cmd, expiryHeaders...)

	cmd.Flags().Int64VarP(&renewTimeMillis, "renew-time-millis", "t", -1, "how long to renew the token's expiry time for, or -1 for the broker's delegation.token.expiry.time.ms default")

	return cmd
}

func expireTokenCommand(cl *client.Client) *cobra.Command {
	var expiryPeriodMillis int64

	cmd := &cobra.Command{
		Use:   "expire HMAC",
		Short: "Change a delegation token expiry time (Kafka 1.1.0+).",
		Long: `Change a delegation token expiry time (Kafka 1.1.0+).

Expiring sets a token's expiry to now plus --expire-period-millis, so that
the default of 0 expires it now. The token is named by its base64 HMAC, as
describe prints it. The result is one row: the new EXPIRY, with ERROR and
MESSAGE.

EXAMPLES:
  kcl dtoken expire 'base64 hmac'

SEE ALSO:
  kcl dtoken describe    describe tokens
  kcl dtoken renew       renew a token
`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			decoded, err := base64.StdEncoding.DecodeString(args[0])
			if err != nil {
				return out.Errf(out.ExitUsage, "unable to base64 decode hmac: %v", err)
			}
			req := &kmsg.ExpireDelegationTokenRequest{
				HMAC:               decoded,
				ExpiryPeriodMillis: expiryPeriodMillis,
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to expire delegation token: %v", err)
			}
			resp := kresp.(*kmsg.ExpireDelegationTokenResponse)

			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "results", expiryHeaders...).ResultColumns()
			table.Row(expiryRow(resp.ErrorCode, resp.ExpiryTimestamp)...)
			return table.Flush()
		},
	}
	out.Columns(cmd, expiryHeaders...)

	cmd.Flags().Int64VarP(&expiryPeriodMillis, "expire-period-millis", "p", 0, "how long from now to allow for expiry")

	return cmd
}

func describeTokensCommand(cl *client.Client) *cobra.Command {
	var owners []string

	cmd := &cobra.Command{
		Use:     "describe",
		Aliases: []string{"d"},
		Short:   "Describe delegation tokens (Kafka 1.1.0+).",
		Long: `Describe delegation tokens (Kafka 1.1.0+).

This prints every token, or with --owner the tokens those owners created.
Each row is PRINCIPAL ISSUED EXPIRY MAX-AGE TOKEN-ID HMAC RENEWERS, the times
in UTC, the HMAC in base64, and the renewers comma joined (an array in
JSON). Rows are sorted by principal and then token ID.

EXAMPLES:
  kcl dtoken describe                  # every token
  kcl dtoken describe -o User:admin    # tokens the admin user owns

SEE ALSO:
  kcl dtoken create    create a token
  kcl dtoken renew     renew a token
  kcl dtoken expire    expire a token
`,
		Args: cobra.ExactArgs(0),
		RunE: func(_ *cobra.Command, _ []string) error {
			req := new(kmsg.DescribeDelegationTokenRequest)
			for _, owner := range owners {
				ptyp, pname := parsePrincipal(owner)
				req.Owners = append(req.Owners, kmsg.DescribeDelegationTokenRequestOwner{
					PrincipalType: ptyp,
					PrincipalName: pname,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			if err != nil {
				return fmt.Errorf("unable to describe delegation tokens: %v", err)
			}
			resp := kresp.(*kmsg.DescribeDelegationTokenResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				return fmt.Errorf("%v", err)
			}

			details := slices.Clone(resp.TokenDetails)
			slices.SortFunc(details, func(a, b kmsg.DescribeDelegationTokenResponseTokenDetail) int {
				return cmp.Or(
					strings.Compare(principal(a.PrincipalType, a.PrincipalName), principal(b.PrincipalType, b.PrincipalName)),
					strings.Compare(a.TokenID, b.TokenID),
				)
			})
			table := out.NewFormattedTable(cl.Format(), cl.Command(), 1, "tokens", describeHeaders...)
			for i := range details {
				table.Row(describeRow(&details[i])...)
			}
			return table.Flush()
		},
	}
	out.Columns(cmd, describeHeaders...)

	cmd.Flags().StringArrayVarP(&owners, "owner", "o", nil, "optional list of tokens by created by these owners to filter for; repeatable")

	return cmd
}

// millisToStr is a unix millisecond timestamp as the UTC time in RFC 3339,
// one word, so that it is one awk field.
func millisToStr(millis int64) string {
	return time.UnixMilli(millis).UTC().Format("2006-01-02T15:04:05.000Z")
}
