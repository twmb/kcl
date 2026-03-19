package userscram

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"hash"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
	"golang.org/x/crypto/pbkdf2"
)

func Command(cl *client.Client) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "user",
		Aliases: []string{"user-scram"},
		Short:   "Alter or describe user scram configs (2.7.0+).",
	}
	list := describeUserSCRAM(cl)
	list.Use = "list"
	list.Aliases = []string{"ls", "describe", "d"}
	list.Short = "List user SCRAM credentials."

	cmd.AddCommand(alterUserSCRAM(cl))
	cmd.AddCommand(list)
	return cmd
}

func mech2str(mech int8) string {
	switch mech {
	default:
		return "UNKNOWN"
	case 1:
		return "SCRAM-SHA-256"
	case 2:
		return "SCRAM-SHA-512"
	}
}

func str2mech(str string) (int8, error) {
	switch client.Strnorm(str) {
	default:
		return 0, out.Errf(out.ExitUsage, "unknown mechanism %s", str)
	case "scramsha256":
		return 1, nil
	case "scramsha512":
		return 2, nil
	}
}

func describeUserSCRAM(cl *client.Client) *cobra.Command {
	var users []string

	cmd := &cobra.Command{
		Use:     "describe",
		Aliases: []string{"d"},
		Short:   "Describe user scram credentials.",
		Long:    `Describe user scram credentials (Kafka 2.7.0+)`,
		Args:    cobra.ExactArgs(0),

		RunE: func(_ *cobra.Command, _ []string) error {
			var req kmsg.DescribeUserSCRAMCredentialsRequest
			for _, user := range users {
				req.Users = append(req.Users, kmsg.DescribeUserSCRAMCredentialsRequestUser{
					Name: user,
				})
			}

			kresp, err := cl.Client().Request(context.Background(), &req)
			if err != nil {
				return fmt.Errorf("unable to describe user scram credentials: %v", err)
			}
			resp := kresp.(*kmsg.DescribeUserSCRAMCredentialsResponse)

			if resp.ErrorCode != 0 {
				additional := ""
				if resp.ErrorMessage != nil {
					additional = ": " + *resp.ErrorMessage
				}
				return fmt.Errorf("%s%s", kerr.ErrorForCode(resp.ErrorCode), additional)
			}

			table := out.NewFormattedTable(cl.Format(), "user.list", 1, "credentials",
				"USER", "MECHANISM", "ITERATIONS", "ERROR")
			for _, res := range resp.Results {
				if res.ErrorCode != 0 {
					msg := ""
					if res.ErrorMessage != nil {
						msg = *res.ErrorMessage
					}
					table.Row(res.User, "", "", fmt.Sprintf("%v, %s", kerr.ErrorForCode(res.ErrorCode), msg))
					continue
				}
				for _, info := range res.CredentialInfos {
					table.Row(res.User, mech2str(info.Mechanism), info.Iterations, "")
				}
			}
			table.Flush()
			return nil
		},
	}

	cmd.Flags().StringArrayVar(&users, "user", nil, "user to describe; if nil, describes all")

	return cmd
}

func alterUserSCRAM(cl *client.Client) *cobra.Command {
	var sets []string
	var dels []string

	cmd := &cobra.Command{
		Use:   "alter",
		Short: "Alter user scram credentials.",
		Long: `Alter user scram credentials (Kafka 2.7.0+)

Both deleting and setting have the same input format, with setting requiring
more keys.

To delete, you must pass a user and scram mechanism to match for deletion:

    --del user=alice,mechanism=scram-sha-512

Both user and mechanism are required.

To insert or update, you must pass a user, scram mechanism, and password.
Optional arguments are iterations and salt; if these are empty, they will
be 4096 and 20 cryptographically random bytes. To pass a halt, use hex.

For example,

    --set user=alice,mechanism=scram-sha-512,password=foo
    --set user=bob,mechanism=scram-sha-256,iterations=16384,salt=ab01cd3f
    --set user=george,mechanism=scram-sha-512,iterations=8192
    --set user=sally,mechanism=scram-sha-256,salt=01020304badcef

Both --set and --del can be specified many times.
`,
		Args: cobra.ExactArgs(0),

		RunE: func(_ *cobra.Command, _ []string) error {
			var req kmsg.AlterUserSCRAMCredentialsRequest
			for _, del := range dels {
				allowed := map[string]bool{
					"user":      true,
					"mechanism": true,
				}

				var d kmsg.AlterUserSCRAMCredentialsRequestDeletion

				for _, kv := range strings.Split(del, ",") {
					split := strings.SplitN(kv, "=", 2)
					if len(split) != 2 {
						return out.Errf(out.ExitUsage, "delete kv %q missing value", kv)
					}
					k, v := split[0], split[1]
					k = strings.ToLower(k)
					if !allowed[k] {
						return out.Errf(out.ExitUsage, "delete key %q is invalid (allowed: name, mechanism)", split[0])
					}
					delete(allowed, k)
					switch k {
					case "user":
						d.Name = v
					case "mechanism":
						mech, err := str2mech(v)
						if err != nil {
							return err
						}
						d.Mechanism = mech
					}
				}

				if len(allowed) > 0 {
					return out.Errf(out.ExitUsage, "deletions require both user and mechanism specified")
				}

				req.Deletions = append(req.Deletions, d)
			}

			for _, set := range sets {
				allowed := map[string]bool{
					"user":       true,
					"mechanism":  true,
					"password":   true,
					"iterations": true,
					"salt":       true,
				}

				u := kmsg.AlterUserSCRAMCredentialsRequestUpsertion{
					Iterations: 4 << 10,
				}
				var password string

				for _, kv := range strings.Split(set, ",") {
					split := strings.SplitN(kv, "=", 2)
					if len(split) != 2 {
						return out.Errf(out.ExitUsage, "set kv %q missing value", kv)
					}
					k, v := split[0], split[1]
					k = strings.ToLower(k)
					if !allowed[k] {
						return out.Errf(out.ExitUsage, "set key %q is invalid (allowed: user, mechanism, password, iterations, salt)", split[0])
					}
					delete(allowed, k)
					switch k {
					case "user":
						u.Name = v
					case "mechanism":
						mech, err := str2mech(v)
						if err != nil {
							return err
						}
						u.Mechanism = mech
					case "password":
						password = v
					case "iterations":
						i, err := strconv.ParseInt(v, 10, 32)
						if err != nil {
							return fmt.Errorf("set iterations is not a number: %v", err)
						}
						if i < 4092 || i > 16<<10 {
							return out.Errf(out.ExitUsage, "invalid iterations %d: min allowed 4k, max 16k", i)
						}
						u.Iterations = int32(i)
					case "salt":
						var err error
						u.Salt, err = hex.DecodeString(v)
						if err != nil {
							return fmt.Errorf("salt is not hex: %v", err)
						}
					}
				}

				delete(allowed, "iterations") // optional
				delete(allowed, "salt")       // optional

				if len(allowed) > 0 {
					return out.Errf(out.ExitUsage, "sets require user, mechanism, and password")
				}

				if len(u.Salt) == 0 {
					u.Salt = make([]byte, 20)
					_, err := rand.Read(u.Salt)
					if err != nil {
						return fmt.Errorf("unable to read 20 random bytes: %v", err)
					}
				}

				var h func() hash.Hash
				switch u.Mechanism {
				case 1:
					h = sha256.New
				case 2:
					h = sha512.New
				default:
					return out.Errf(out.ExitUsage, "unknown mechanism %d", u.Mechanism)
				}

				u.SaltedPassword = pbkdf2.Key([]byte(password), u.Salt, int(u.Iterations), h().Size(), h) // SaltedPassword := Hi(Normalize(password), salt, i)

				req.Upsertions = append(req.Upsertions, u)
			}

			kresp, err := cl.Client().Request(context.Background(), &req)
			if err != nil {
				return fmt.Errorf("unable to alter user scram credentials: %v", err)
			}
			resp := kresp.(*kmsg.AlterUserSCRAMCredentialsResponse)

			table := out.NewFormattedTable(cl.Format(), "user.alter", 1, "results",
				"USER", "ERROR")
			for _, res := range resp.Results {
				table.Row(res.User, kerr.ErrorForCode(res.ErrorCode))
			}
			table.Flush()
			return nil
		},
	}

	cmd.Flags().StringArrayVar(&dels, "del", nil, "user and mechanism pairing to delete, repeatable")
	cmd.Flags().StringArrayVar(&sets, "set", nil, "user and mechanism pairing to insert or update, repeatable")

	return cmd
}
