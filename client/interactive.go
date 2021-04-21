package client

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
)

func p(noHelp bool, msg string, args ...interface{}) {
	if !noHelp {
		if !strings.HasSuffix(msg, "\n") {
			msg += "\n"
		}
		fmt.Printf(msg, args...)
	}
}

func exit(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

const intro = `
    Welcome to kcl!

    This short interactive prompt will guide through setting up a configuration.

    If you are in this interactive prompt but do not want to be, you can either set
    the KCL_NO_CONFIG_FILE env var to be non-empty or use the --no-config-file flag.

    kcl configurations are located by default in your user configuration directory,
    but this can be changed with the KCL_CONFIG_PATH environment variable. See the
    help text in "kcl myconfig help" for more information, as well as for info on
    specifying configuration directly through flags or env variables.

###

    First, specify some seed brokers that kcl can connect to. These can be entered
    over multiple lines or comma delimited on one line. An empty line moves to the
    next prompt.

`

const promptTLS = `
###

    Does your broker require TLS? If so, enter "y" or "yes", and then for any
    prompt, if the field is required, specify it.

`

const promptSASL = `
###

    Does connecting to your cluster require SASL?

`

type scanner struct {
	s *bufio.Scanner

	mu    sync.Mutex
	cond  *sync.Cond
	lines []string
}

func newScanner() *scanner {
	s := &scanner{
		s: bufio.NewScanner(os.Stdin),
	}
	s.cond = sync.NewCond(&s.mu)
	go s.scan()
	return s
}

func (s *scanner) scan() {
	last := time.Now()
	for s.s.Scan() {
		line := s.s.Text()
		if len(line) == 0 && time.Since(last) < 50*time.Millisecond {
			last = time.Now()
			continue
		}
		last = time.Now()

		s.mu.Lock()
		s.lines = append(s.lines, line)
		if len(s.lines) > 10 {
			exit("too much unhandled input, exiting")
		}
		s.mu.Unlock()
		s.cond.Broadcast()
	}
	if s.s.Err() != nil {
		exit("scanner error: %v, exiting", s.s.Err())
	}
	exit("scanner received EOF, exiting")
}

func (s *scanner) line(prompt string) string {
	fmt.Print(prompt + " ")

	done := make(chan struct{})
	var line string
	go func() {
		defer close(done)
		s.mu.Lock()
		defer s.mu.Unlock()

		for len(s.lines) == 0 {
			s.cond.Wait()
		}
		line = s.lines[0]
		s.lines = s.lines[1:]
	}()

	<-done
	return line
}

// Wizard walks through an interactive prompt to create a configuration.
func Wizard(noHelp bool) {
	p(noHelp, intro)

	s := newScanner()
	cfg := Cfg{
		TimeoutMillis: 10000,
	}

	for {
		l := s.line("broker addr?")
		if len(l) == 0 {
			break
		}

		for _, field := range strings.Split(l, ",") {
			field = strings.TrimSpace(field)
			if len(field) == 0 {
				continue
			}
			cfg.SeedBrokers = append(cfg.SeedBrokers, field)
		}
	}

	p(noHelp, promptTLS)
	i := s.line("tls yes/no?")
	l := strings.ToLower(i)
	switch {
	case strings.HasPrefix("yes", l):
		parseTLS(&cfg, s, noHelp)
	case strings.HasPrefix("no", l):
	default:
		exit("unrecognized input %q, exiting", i)
	}

	p(noHelp, promptSASL)
	i = s.line("sasl yes/no?")
	l = strings.ToLower(i)
	switch {
	case strings.HasPrefix("yes", l):
		parseSASL(&cfg, s, noHelp)
	case strings.HasPrefix("no", l):
	default:
		exit("unrecognized input %q, exiting", i)
	}

	write(&cfg, s, noHelp)
}

func parseTLS(cfg *Cfg, s *scanner, noHelp bool) {
	cfg.TLS = new(CfgTLS)

	p(noHelp, "\n    If connecting via tls requires a custom CA cert, specify the path to your CA.\n\n")
	cfg.TLS.CACert = s.line("ca path?")

	p(noHelp, "\n    If connecting via tls requires a client cert & key, specify the path to each.\n\n")
	if cert := s.line("client cert path?"); cert != "" {
		cfg.TLS.ClientCertPath = cert
		cfg.TLS.ClientKeyPath = s.line("client key path? ")
	}

	p(noHelp, `
    If you need a distinct server name to use when connecting over TLS, rather than
    just the broker name, specify it. By default, the client will use the hostname
    or ip address of whatever broker it is connecting to as the ServerName.

`)
	cfg.TLS.ServerName = s.line("tls server name?")
}

func parseSASL(cfg *Cfg, s *scanner, noHelp bool) {
	cfg.SASL = new(CfgSASL)

	p(noHelp, `
    Which SASL method is required, and what is the user/pass?

`)
	switch method := s.line("method (plain, scram-sha-256, scram-sha-512)?"); Strnorm(method) {
	case "plain",
		"scramsha256",
		"scramsha512":
		cfg.SASL.Method = method
	default:
		exit("unrecognized sasl method %q, exiting", method)
	}

	cfg.SASL.User = s.line("user?")
	cfg.SASL.Pass = s.line("pass?")

	p(noHelp, `
    Is this SASL from a delegation token?

`)

	l := strings.ToLower(s.line("is token?"))
	switch {
	case strings.HasPrefix("yes", l):
		cfg.SASL.IsToken = true
	}
}

func write(cfg *Cfg, s *scanner, noHelp bool) {
	p(noHelp, `
###

    Configuration complete, please specify the filename to save this under.

`)

	// Create our file.
	var raw bytes.Buffer
	toml.NewEncoder(&raw).Encode(cfg)

	// Write our file...
	cfgDir, err := os.UserConfigDir()
	if err == nil {
		cfgDir = filepath.Join(cfgDir, "kcl")
	}
	if envDir, ok := os.LookupEnv("KCL_CONFIG_DIR"); ok {
		cfgDir = envDir
	}
	fname := s.line("filename?")
	if !strings.HasSuffix(fname, ".toml") {
		fname += ".toml"
	}
	cfgPath := filepath.Join(cfgDir, fname)
	if err := os.MkdirAll(cfgDir, 0755); err != nil {
		exit("unable to create configuration directory at %s: %v", cfgDir, err)
	}

	if err := writeFile(cfgPath, raw.Bytes(), 0666); err != nil {
		exit("unable to create configuration at %s: %v", cfgPath, err)
	}

	// Maybe link to our new file.
	linkFile := "config.toml"
	if envLink, ok := os.LookupEnv("KCL_CONFIG_FILE"); ok {
		linkFile = envLink
	}

	fmt.Printf("\n    Successfully created configuration at %s!\n", cfgPath)

	if fname == linkFile {
		p(noHelp, `
    Multiple configurations can be managed in this directory with a symlink and
    the "kcl myconfig link" command. Since this file is named %s, which is the
    default name of the link file, we will not link to this file, and instead
    kcl will just use this file directly.
`, linkFile)
		return
	}

	quit := func(msg string, args ...interface{}) {
		fmt.Printf(msg+"\n", args...)
		fmt.Println()
		fmt.Println("\n    To use this new configuration, set KCL_CONFIG_PATH to %s.\n\n", cfgPath)
		os.Exit(0)
	}

	linkPath := filepath.Join(cfgDir, linkFile)

	existing, err := os.Lstat(linkPath)
	if err != nil {
		if !os.IsNotExist(err) {
			quit("stat err for existing config path %q: %v", linkPath, err)
		}
		// not exists: we can create symlink
	} else {
		if existing.Mode()&os.ModeSymlink == 0 {
			quit("stat shows that existing config at %q is not a symlink, avoiding linking", linkPath)
			return
		}
	}

	if existing != nil {
		if err := os.Remove(linkPath); err != nil {
			quit("unable to remove old symlink at %q: %v, avoiding linking", linkPath, err)
		}
	}

	if err := os.Symlink(cfgPath, linkPath); err != nil {
		quit("unable to create symlink from %q to %q: %v", cfgPath, linkPath, err)
	}

	fmt.Printf("    Successfully linked %s to %s\n", cfgPath, linkPath)
}

// This is os.WriteFile, but with O_EXCL and not O_TRUNC.
func writeFile(name string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, perm)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err1 := f.Close(); err1 != nil && err == nil {
		err = err1
	}
	return err
}
