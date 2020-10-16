// Package dtoken contains delegation token commands.
package dtoken

import (
	"context"
	"encoding/base64"
	"fmt"
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
		Short: "Delegation token commands",
		Long: `Delegation tokens allow for (ideally) a quicker and easier method of enabling
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

func createTokenCommand(cl *client.Client) *cobra.Command {
	var renewers []string
	var maxLifetimeMillis int64

	cmd := &cobra.Command{
		Use:     "create",
		Aliases: []string{"c"},
		Short:   "Create a delegation token",
		Long: `Create a delegation token (Kafka 1.1.0+).

A delegation token inherits all ACLs from the creator. Without any manual
extra renewers, only the creator can renew.

Renewers can be specified either as "Type:name" or just "name". If eliding the
type, the client uses "User", which is the only type that exists in Kafka's
SimpleAuthorizer.
`,

		Example: "create -r admin1 -r User:admin2",
		Args:    cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			req := &kmsg.CreateDelegationTokenRequest{
				MaxLifetimeMillis: maxLifetimeMillis,
			}
			for _, renewer := range renewers {
				ptyp, pname := "User", renewer
				if delim := strings.IndexByte(renewer, ':'); delim != -1 {
					ptyp = renewer[:delim]
					pname = renewer[delim+1:]
				}
				req.Renewers = append(req.Renewers, kmsg.CreateDelegationTokenRequestRenewer{
					PrincipalType: ptyp,
					PrincipalName: pname,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to create delegation token: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}
			resp := kresp.(*kmsg.CreateDelegationTokenResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.Die("%v", err)
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()

			fmt.Fprintf(tw, "PRINCIPAL\t%s:%s\n", resp.PrincipalType, resp.PrincipalName)
			fmt.Fprintf(tw, "ISSUED\t%s\n", millisToStr(resp.IssueTimestamp))
			fmt.Fprintf(tw, "EXPIRY\t%s\n", millisToStr(resp.ExpiryTimestamp))
			fmt.Fprintf(tw, "MAX AGE\t%s\n", millisToStr(resp.MaxTimestamp))
			fmt.Fprintf(tw, "TOKEN ID\t%s\n", resp.TokenID)
			fmt.Fprintf(tw, "base64(HMAC)\t%s\n", base64.StdEncoding.EncodeToString(resp.HMAC))
		},
	}

	cmd.Flags().Int64VarP(&maxLifetimeMillis, "max-lifetime-millis", "l", -1, "the maximum lifetime of this token, or -1 for the broker's delegation.token.max.lifetime.ms default")
	cmd.Flags().StringArrayVarP(&renewers, "renewer", "r", nil, "optional list of users allowed to renew this token, or empty to default to the token creator; repeatable")

	return cmd
}

func renewTokenCommand(cl *client.Client) *cobra.Command {
	var renewTimeMillis int64

	cmd := &cobra.Command{
		Use:     "renew",
		Short:   "Renew a delegation token (Kafka 1.1.0+)",
		Example: "renew [base64 hmac here]",
		Args:    cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			decoded, err := base64.StdEncoding.DecodeString(args[0])
			out.MaybeDie(err, "unable to base64 decode hmac: %v", err)
			req := &kmsg.RenewDelegationTokenRequest{
				HMAC:            decoded,
				RenewTimeMillis: renewTimeMillis,
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to renew delegation token: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}
			resp := kresp.(*kmsg.RenewDelegationTokenResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.Die("%v", err)
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()

			fmt.Fprintf(tw, "EXPIRY\t%s\n", millisToStr(resp.ExpiryTimestamp))
		},
	}

	cmd.Flags().Int64VarP(&renewTimeMillis, "renew-time-millis", "t", -1, "how long to renew the token's expiry time for, or -1 for the broker's delegation.token.expiry.time.ms default")

	return cmd
}

func expireTokenCommand(cl *client.Client) *cobra.Command {
	var expiryPeriodMillis int64

	cmd := &cobra.Command{
		Use:     "expire",
		Short:   "Change a delegation token expiry time (Kafka 1.1.0+)",
		Example: "expire [base64 hmac here]",
		Args:    cobra.ExactArgs(1),
		Run: func(_ *cobra.Command, args []string) {
			decoded, err := base64.StdEncoding.DecodeString(args[0])
			out.MaybeDie(err, "unable to base64 decode hmac: %v", err)
			req := &kmsg.ExpireDelegationTokenRequest{
				HMAC:               decoded,
				ExpiryPeriodMillis: expiryPeriodMillis,
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to expire delegation token: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}
			resp := kresp.(*kmsg.ExpireDelegationTokenResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.Die("%v", err)
			}

			tw := out.BeginTabWrite()
			defer tw.Flush()

			fmt.Fprintf(tw, "EXPIRY\t%s\n", millisToStr(resp.ExpiryTimestamp))
		},
	}

	cmd.Flags().Int64VarP(&expiryPeriodMillis, "expire-period-millis", "p", 0, "how long from now to allow for expiry")

	return cmd
}

func describeTokensCommand(cl *client.Client) *cobra.Command {
	var owners []string

	cmd := &cobra.Command{
		Use:     "describe",
		Aliases: []string{"d"},
		Short:   "Describe delegation tokens (Kafka 1.1.0+)",
		Example: ` describe // to display all tokens

describe -o User:admin // to display tokens owned by the admin user`,
		Args: cobra.ExactArgs(0),
		Run: func(_ *cobra.Command, _ []string) {
			req := new(kmsg.DescribeDelegationTokenRequest)
			for _, owner := range owners {
				ptyp, pname := "User", owner
				if delim := strings.IndexByte(owner, ':'); delim != -1 {
					ptyp = owner[:delim]
					pname = owner[delim+1:]
				}
				req.Owners = append(req.Owners, kmsg.DescribeDelegationTokenRequestOwner{
					PrincipalType: ptyp,
					PrincipalName: pname,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), req)
			out.MaybeDie(err, "unable to describe delegation tokens: %v", err)
			if cl.AsJSON() {
				out.ExitJSON(kresp)
			}
			resp := kresp.(*kmsg.DescribeDelegationTokenResponse)
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				out.Die("%v", err)
			}

			for _, detail := range resp.TokenDetails {
				tw := out.BeginTabWrite()
				fmt.Fprintf(tw, "PRINCIPAL\t%s:%s\n", detail.PrincipalType, detail.PrincipalName)
				fmt.Fprintf(tw, "ISSUED\t%s\n", millisToStr(detail.IssueTimestamp))
				fmt.Fprintf(tw, "EXPIRY\t%s\n", millisToStr(detail.ExpiryTimestamp))
				fmt.Fprintf(tw, "MAX AGE\t%s\n", millisToStr(detail.MaxTimestamp))
				fmt.Fprintf(tw, "TOKEN ID\t%s\n", detail.TokenID)
				fmt.Fprintf(tw, "base64(HMAC)\t%s\n", base64.StdEncoding.EncodeToString(detail.HMAC))
				var renewers []string
				for _, renewer := range detail.Renewers {
					renewers = append(renewers, fmt.Sprintf("%s:%s", renewer.PrincipalType, renewer.PrincipalName))
				}
				if len(renewers) == 0 {
					renewers = append(renewers, fmt.Sprintf("%s:%s", detail.PrincipalType, detail.PrincipalName))
				}
				fmt.Fprintf(tw, "RENEWERS\t%s\n", "["+strings.Join(renewers, ", ")+"]")
				tw.Flush()
				fmt.Println()
			}
		},
	}

	cmd.Flags().StringArrayVarP(&owners, "owner", "o", nil, "optional list of tokens by created by these owners to filter for; repeatable")

	return cmd
}

func millisToStr(millis int64) string {
	return time.Unix(millis/1000, 0).
		Add(time.Duration(millis%1000) * time.Millisecond).
		Format("2006-01-02 15:04:05.999")
}
