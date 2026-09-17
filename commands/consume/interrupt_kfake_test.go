package consume

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/twmb/kcl/client"
	"github.com/twmb/kcl/out"
)

// consumeEnv carries the arguments of the kcl a child runs, as JSON. A test
// binary with this set in its environment is a kcl, not a test. Consuming
// stops on a signal, and a signal needs a process of its own to send one to.
const consumeEnv = "KCL_TEST_CONSUME_ARGS"

func TestMain(m *testing.M) {
	if s, ok := os.LookupEnv(consumeEnv); ok {
		var args []string
		if err := json.Unmarshal([]byte(s), &args); err != nil {
			fmt.Fprintf(os.Stderr, "unable to read %s: %v\n", consumeEnv, err)
			os.Exit(99)
		}
		root := &cobra.Command{Use: "kcl", SilenceUsage: true, SilenceErrors: true}
		cl := client.New(root)
		root.AddCommand(Command(cl))
		root.SetArgs(args)
		if err := root.Execute(); err != nil {
			out.HandleError(err, cl.Format(), out.CommandName("kcl consume"))
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// TestConsumeInterrupt pins what an interrupted consume writes. Stopping one
// with ctrl-c canceled the context the poll in flight was using, and the
// cancellation came back as a fetch error, so every interrupt ended with
// "fetch error [-1]: context canceled" on stderr before exit 0. A fetch that
// really failed still says so.
func TestConsumeInterrupt(t *testing.T) {
	for _, test := range []struct {
		name          string
		faultFetch    bool
		wantFetchErrs bool
	}{
		{name: "clean", wantFetchErrs: false},
		{name: "faulted fetch", faultFetch: true, wantFetchErrs: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			const topic = "consume-interrupt"
			const records = 3

			c, err := kfake.NewCluster(kfake.NumBrokers(1))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.Close)

			kcl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(kcl.Close)
			if _, err := kadm.NewClient(kcl).CreateTopic(t.Context(), 1, 1, nil, topic); err != nil {
				t.Fatal(err)
			}
			for i := range records {
				r := &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "record-%d", i)}
				if res := kcl.ProduceSync(t.Context(), r); res.FirstErr() != nil {
					t.Fatal(res.FirstErr())
				}
			}

			// UNKNOWN_SERVER_ERROR is not retriable, so kgo hands it
			// to the consume loop rather than fetching again.
			if test.faultFetch {
				c.Fault(kfake.Fault{
					Keys:  []kmsg.Key{kmsg.Fetch},
					Topic: topic,
					Err:   kerr.UnknownServerError,
					Count: -1,
				})
			}

			child := startConsume(t, "consume", topic, "-o", "start", "--no-config-file", "-B", c.ListenAddrs()[0])

			// Interrupt only once the child is consuming, so that the
			// signal lands on a poll in flight rather than on startup.
			if test.faultFetch {
				child.waitFor(t, &child.stderr, "fetch error")
			} else {
				child.waitFor(t, &child.stdout, fmt.Sprintf("record-%d", records-1))
			}

			code := child.interrupt(t)
			if code != out.ExitOK {
				t.Errorf("exit %d, want %d; stderr:\n%s", code, out.ExitOK, strings.Join(child.stderr.all(), "\n"))
			}

			var fetchErrs []string
			for _, line := range child.stderr.all() {
				if strings.Contains(line, "fetch error") {
					fetchErrs = append(fetchErrs, line)
				}
			}
			switch {
			case test.wantFetchErrs && len(fetchErrs) == 0:
				t.Errorf("a faulted fetch printed no fetch error; stderr:\n%s", strings.Join(child.stderr.all(), "\n"))
			case !test.wantFetchErrs && len(fetchErrs) > 0:
				t.Errorf("an interrupt printed %d fetch errors:\n%s", len(fetchErrs), strings.Join(fetchErrs, "\n"))
			}
		})
	}
}

// lines are the lines one of a child's outputs has written so far.
type lines struct {
	mu sync.Mutex
	l  []string
}

func (ls *lines) add(line string) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.l = append(ls.l, line)
}

func (ls *lines) all() []string {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return append([]string(nil), ls.l...)
}

type consumeChild struct {
	cmd    *exec.Cmd
	scans  sync.WaitGroup
	stdout lines
	stderr lines
}

// startConsume runs "kcl consume" in a child process, reading both of its
// outputs as they arrive.
func startConsume(t *testing.T, args ...string) *consumeChild {
	t.Helper()

	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	enc, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}

	child := &consumeChild{cmd: exec.Command(exe)}
	// A KCL_ variable in the environment this test was run from would
	// reach the child as config, so it gets none of them.
	for _, kv := range os.Environ() {
		if !strings.HasPrefix(kv, "KCL_") {
			child.cmd.Env = append(child.cmd.Env, kv)
		}
	}
	child.cmd.Env = append(child.cmd.Env, consumeEnv+"="+string(enc))

	stdout, err := child.cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	stderr, err := child.cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if child.cmd.ProcessState == nil {
			child.cmd.Process.Kill()
			child.cmd.Wait()
		}
	})
	child.scan(stdout, &child.stdout)
	child.scan(stderr, &child.stderr)
	return child
}

func (c *consumeChild) scan(r io.Reader, into *lines) {
	c.scans.Add(1)
	go func() {
		defer c.scans.Done()
		scanner := bufio.NewScanner(r)
		for scanner.Scan() {
			into.add(scanner.Text())
		}
	}()
}

// waitFor blocks until the child writes a line containing want.
func (c *consumeChild) waitFor(t *testing.T, ls *lines, want string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	for {
		for _, line := range ls.all() {
			if strings.Contains(line, want) {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("waiting for %q: %v\nstdout:\n%s\nstderr:\n%s", want, ctx.Err(),
				strings.Join(c.stdout.all(), "\n"), strings.Join(c.stderr.all(), "\n"))
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// interrupt signals the child the way ctrl-c does and returns the code it
// exits with.
func (c *consumeChild) interrupt(t *testing.T) int {
	t.Helper()
	c.signal(t)
	return c.wait(t)
}

// signal sends the child one ctrl-c and returns without waiting.
func (c *consumeChild) signal(t *testing.T) {
	t.Helper()
	if err := c.cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
}

// wait returns the code the child exits with.
func (c *consumeChild) wait(t *testing.T) int {
	t.Helper()
	// Both outputs must be read to EOF before Wait, which closes them.
	c.scans.Wait()
	if err := c.cmd.Wait(); err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatalf("unable to wait on kcl consume: %v", err)
		}
		return exit.ExitCode()
	}
	return out.ExitOK
}

// freezeListener wraps a net.Listener so that, once frozen, every connection
// to it accepts but delivers no more bytes to the server. The server then
// never sees a request and never answers, which is a broker that stopped
// responding -- not the connection-refused a closed cluster gives, which
// kgo fails fast. kfake serves on this via ListenFn, so it advertises this
// address and the client cannot route around it.
type freezeListener struct {
	net.Listener
	frozen *atomic.Bool
}

func (l freezeListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	return &freezeConn{Conn: c, frozen: l.frozen, done: make(chan struct{})}, nil
}

type freezeConn struct {
	net.Conn
	frozen   *atomic.Bool
	done     chan struct{}
	doneOnce sync.Once
}

func (c *freezeConn) Read(b []byte) (int, error) {
	for c.frozen.Load() {
		select {
		case <-c.done:
			return 0, io.EOF
		case <-time.After(20 * time.Millisecond):
		}
	}
	return c.Conn.Read(b)
}

func (c *freezeConn) Close() error {
	c.doneOnce.Do(func() { close(c.done) })
	return c.Conn.Close()
}

// TestConsumeInterruptBrokerGone pins what one interrupt does when the broker
// will not answer the group leave. Close leaves the group, and a leave to a
// dead broker retries for retry_timeout, so the first ctrl-c cannot finish;
// after a second we must say what we are waiting on, and a second ctrl-c
// must quit at once. An earlier share-group smoke run reached for SIGKILL
// here because nothing said the process was still alive.
func TestConsumeInterruptBrokerGone(t *testing.T) {
	for _, shared := range []bool{false, true} {
		name := "group"
		if shared {
			name = "share-group"
		}
		t.Run(name, func(t *testing.T) {
			const topic = "interrupt-gone"
			var frozen atomic.Bool
			c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.ListenFn(func(network, address string) (net.Listener, error) {
				ln, err := net.Listen(network, address)
				if err != nil {
					return nil, err
				}
				return freezeListener{Listener: ln, frozen: &frozen}, nil
			}))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.Close)

			kcl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(kcl.Close)
			if _, err := kadm.NewClient(kcl).CreateTopic(t.Context(), 1, 1, nil, topic); err != nil {
				t.Fatal(err)
			}
			if res := kcl.ProduceSync(t.Context(), &kgo.Record{Topic: topic, Value: []byte("r")}); res.FirstErr() != nil {
				t.Fatal(res.FirstErr())
			}

			args := []string{"consume", topic, "-o", "start", "--no-config-file", "-B", c.ListenAddrs()[0], "-X", "retry_timeout=45s"}
			if shared {
				c.SetGroupConfigs("sg", map[string]string{"share.auto.offset.reset": "earliest"})
				args = append(args, "--share-group", "sg")
			} else {
				args = append(args, "-g", "g")
			}
			child := startConsume(t, args...)
			child.waitFor(t, &child.stdout, "r")

			// From here the broker answers nothing; the leave retries
			// for retry_timeout (45s).
			frozen.Store(true)

			child.signal(t)
			child.waitFor(t, &child.stderr, "ctrl+c again")
			start := time.Now()
			child.signal(t)
			code := child.wait(t)
			if took := time.Since(start); took > 10*time.Second {
				t.Errorf("second interrupt took %v, want an immediate exit", took)
			}
			if code != out.ExitOK {
				t.Errorf("exit %d, want %d; stderr:\n%s", code, out.ExitOK, strings.Join(child.stderr.all(), "\n"))
			}
		})
	}
}
