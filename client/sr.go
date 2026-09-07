package client

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/sr"
)

// DefaultRegistryURL is the conventional local Schema Registry address, used
// when no registry URL is otherwise configured. It mirrors the default
// localhost:9092 seed broker.
const DefaultRegistryURL = "http://localhost:8081"

// SchemaRegistryClient builds a Schema Registry client from the loaded
// configuration. Any extra opts are applied last, so callers can override
// defaults (e.g. a custom *http.Client in tests).
//
// Unlike Client, this does not construct a kgo.Client, so "kcl registry"
// commands and schema-aware produce/consume only talk to the registry HTTP
// service when they actually need it.
func (c *Client) SchemaRegistryClient(opts ...sr.ClientOpt) (*sr.Client, error) {
	c.loadCfg()
	if c.expandErr != nil {
		return nil, c.expandErr
	}

	cfg := c.cfg.SR
	if cfg == nil {
		cfg = new(CfgSR)
	}
	urls := cfg.URLs
	if len(urls) == 0 {
		// Mirror the localhost:9092 broker default: with nothing
		// configured, talk to a registry on the conventional local port.
		urls = []string{DefaultRegistryURL}
	}

	srOpts := []sr.ClientOpt{sr.URLs(urls...)}

	switch {
	case cfg.BearerToken != "":
		if cfg.User != "" || cfg.Pass != "" {
			return nil, errors.New("schema registry bearer token and basic auth (user/pass) are mutually exclusive")
		}
		srOpts = append(srOpts, sr.BearerToken(cfg.BearerToken))
	case cfg.User != "" || cfg.Pass != "":
		srOpts = append(srOpts, sr.BasicAuth(cfg.User, cfg.Pass))
	}

	// A --context flag wins over a configured registry.context.
	ctxName := cfg.Context
	if c.registryContext != "" {
		ctxName = c.registryContext
	}
	if ctxName != "" {
		srOpts = append(srOpts, sr.DefaultSchemaContext(ctxName))
	}

	tc, err := buildTLS(cfg.TLS)
	if err != nil {
		return nil, fmt.Errorf("schema registry tls: %w", err)
	}
	if tc != nil {
		srOpts = append(srOpts, sr.DialTLSConfig(tc))
	}

	srOpts = append(srOpts, opts...)
	return sr.NewClient(srOpts...)
}

// SetRegistryContext sets the schema registry context (namespace) that
// subsequent SchemaRegistryClient calls scope requests to. It takes priority
// over a configured registry.context.
func (c *Client) SetRegistryContext(name string) { c.registryContext = name }
