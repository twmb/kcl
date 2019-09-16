// Package client is a simple package to build a kgo.Client.
package client

import (
	"sync"

	"github.com/twmb/kgo"

	"github.com/twmb/kcl/internal/out"
)

// Client contains kgo client options and a kgo client.
type Client struct {
	opts   []kgo.Opt
	once   sync.Once
	client *kgo.Client
}

// New returns a new Client.
func New() *Client {
	return new(Client)
}

// AddOpt adds an option to be passed to the eventual new kgo.Client.
func (c *Client) AddOpt(opt kgo.Opt) {
	c.opts = append(c.opts, opt)
}

// Client returns a new kgo.Client using all buffered options.
//
// This can only be used once.
func (c *Client) Client() *kgo.Client {
	c.once.Do(func() {
		var err error
		c.client, err = kgo.NewClient(c.opts...)
		out.MaybeDie(err, "unable to load client: %v", err)
	})
	return c.client
}
