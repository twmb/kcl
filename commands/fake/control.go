package fake

import (
	"context"
	"encoding/hex"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"uuid"

	"github.com/twmb/franz-go/pkg/kfake"
)

// defaultControlAddr is where --control listens when given no address.
const defaultControlAddr = "127.0.0.1:9099"

// controlSkip are methods we never expose. Close would kill the cluster out
// from under the process. The other three are documented as valid only from
// within a control function, so called over HTTP they do nothing or return
// -1, which is worse than not offering them.
var controlSkip = map[string]bool{
	"Close":       true,
	"KeepControl": true,
	"DropControl": true,
	"CurrentNode": true,
}

// controlMethod is one *kfake.Cluster method we can call remotely.
type controlMethod struct {
	Name      string `json:"name"`
	Signature string `json:"signature"`

	fn  reflect.Value
	ctx bool // the method takes a leading context, which we supply
}

// controlMethods returns the cluster methods we can call with arguments built
// from strings, sorted by name. We discover them rather than list them so
// that a method kfake grows later is reachable without a change here; the
// tradeoff is that a method kfake *renames* fails at request time instead of
// at build time, which is what TestControlMethods is for.
func controlMethods(c *kfake.Cluster) []controlMethod {
	var ms []controlMethod
	v := reflect.ValueOf(c)
	t := v.Type()
	for i := range t.NumMethod() {
		name := t.Method(i).Name
		if controlSkip[name] {
			continue
		}
		fn := v.Method(i)
		if !callable(fn.Type()) {
			continue
		}
		ms = append(ms, controlMethod{
			Name:      name,
			Signature: signature(fn.Type()),
			fn:        fn,
			ctx:       takesCtx(fn.Type()),
		})
	}
	slices.SortFunc(ms, func(a, b controlMethod) int { return strings.Compare(a.Name, b.Name) })
	return ms
}

// callable reports whether we can build every argument of fn from a string.
// Funcs are the interesting exclusion: they are how Control, ControlKey and
// SleepControl take their callbacks, and there is no sending one of those
// over a wire. A leading context is the one interface we allow, because we
// supply it rather than you. We skip variadic methods too; kfake has none
// today, and one that appears later is better absent than half working.
func callable(fn reflect.Type) bool {
	if fn.IsVariadic() {
		return false
	}
	for i := range fn.NumIn() {
		if i == 0 && takesCtx(fn) {
			continue
		}
		switch fn.In(i).Kind() {
		case reflect.Func, reflect.Chan, reflect.Interface, reflect.UnsafePointer:
			return false
		}
	}
	return true
}

// takesCtx reports whether fn's first parameter is a context. A method that
// blocks takes one, WaitGroupStable being the first; we hand it the
// request's context so that a caller hanging up ends the wait.
func takesCtx(fn reflect.Type) bool {
	return fn.NumIn() > 0 && fn.In(0) == reflect.TypeFor[context.Context]()
}

// signature is what you type: the method's parameters and results, without
// the context we supply ourselves.
func signature(fn reflect.Type) string {
	if takesCtx(fn) {
		in := make([]reflect.Type, 0, fn.NumIn()-1)
		for i := 1; i < fn.NumIn(); i++ {
			in = append(in, fn.In(i))
		}
		out := make([]reflect.Type, 0, fn.NumOut())
		for i := range fn.NumOut() {
			out = append(out, fn.Out(i))
		}
		fn = reflect.FuncOf(in, out, false)
	}
	return strings.TrimPrefix(fn.String(), "func")
}

// isID reports whether t is a 16 byte array, i.e. a topic ID.
func isID(t reflect.Type) bool {
	return t.Kind() == reflect.Array && t.Len() == 16 && t.Elem().Kind() == reflect.Uint8
}

// buildArg converts one argument into a method's parameter type. Strings are
// taken as they are so you can write foo rather than "foo", a topic ID is a
// uuid in any form uuid.Parse takes, and everything else is JSON.
func buildArg(t reflect.Type, arg string) (reflect.Value, error) {
	p := reflect.New(t)
	switch {
	case t.Kind() == reflect.String:
		p.Elem().SetString(arg)
	case isID(t):
		id, err := uuid.Parse(arg)
		if err != nil {
			return reflect.Value{}, err
		}
		p.Elem().Set(reflect.ValueOf(id).Convert(t))
	default:
		if err := json.Unmarshal([]byte(arg), p.Interface(), json.RejectUnknownMembers(true)); err != nil {
			return reflect.Value{}, err
		}
	}
	return p.Elem(), nil
}

// call invokes m with ctx, which a method that takes a context gets as its
// first argument and you do not pass. A trailing error return comes back as
// an error rather than as a result; what is left is the result, alone if
// there is one and as a list if there are more.
func (m controlMethod) call(ctx context.Context, args []string) (any, error) {
	ft := m.fn.Type()
	var in []reflect.Value
	var off int
	if m.ctx {
		in = append(in, reflect.ValueOf(ctx))
		off = 1
	}
	if len(args) != ft.NumIn()-off {
		return nil, usagef("%s takes %d argument(s), got %d", m.Name, ft.NumIn()-off, len(args))
	}
	for i, arg := range args {
		v, err := buildArg(ft.In(i+off), arg)
		if err != nil {
			return nil, usagef("argument %d (%s): %v", i+1, ft.In(i+off), err)
		}
		in = append(in, v)
	}

	outs := m.fn.Call(in)
	var res []any
	for i, out := range outs {
		if i == len(outs)-1 && ft.Out(i) == reflect.TypeFor[error]() {
			if !out.IsNil() {
				return nil, out.Interface().(error)
			}
			break
		}
		res = append(res, out.Interface())
	}
	switch len(res) {
	case 0:
		return nil, nil
	case 1:
		// kfake answers a topic, partition, or group it does not have with
		// a nil pointer. Printing nothing and exiting 0 hides that, so we
		// say what was not found and fail.
		if v := outs[0]; v.Kind() == reflect.Pointer && v.IsNil() {
			return nil, fmt.Errorf("%s: not found", strings.Join(append([]string{m.Name}, args...), " "))
		}
		return res[0], nil
	default:
		return res, nil
	}
}

// controlHandler serves the control endpoint for c.
func controlHandler(c *kfake.Cluster) http.Handler {
	list := controlMethods(c)
	byName := make(map[string]controlMethod, len(list))
	for _, m := range list {
		byName[m.Name] = m
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /methods", func(w http.ResponseWriter, _ *http.Request) {
		controlWrite(w, http.StatusOK, map[string]any{"methods": list})
	})
	mux.HandleFunc("POST /call/{method}", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("method")
		m, ok := byName[name]
		if !ok {
			controlWriteErr(w, http.StatusNotFound, usagef("unknown method %q; kcl fake control methods lists what we can call", name))
			return
		}
		var req struct {
			Args []string `json:"args"`
		}
		if err := json.UnmarshalRead(r.Body, &req, json.RejectUnknownMembers(true)); err != nil {
			controlWriteErr(w, http.StatusBadRequest, usageErr{err})
			return
		}
		res, err := m.call(r.Context(), req.Args)
		if err != nil {
			controlWriteErr(w, http.StatusBadRequest, err)
			return
		}
		controlWrite(w, http.StatusOK, map[string]any{"result": res})
	})
	faultRoutes(mux, newFaults(c))
	return mux
}

// controlHex renders a 16 byte array as hex rather than as the base64 that
// json gives a byte array, so a topic ID we print pastes straight back into a
// call that takes one, and matches how kcl prints topic IDs everywhere else.
var controlHex = json.WithMarshalers(json.MarshalFunc(func(id [16]byte) ([]byte, error) {
	return []byte(strconv.Quote(hex.EncodeToString(id[:]))), nil
}))

func controlWrite(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.MarshalWrite(w, v, controlHex) //nolint:errcheck // the client is gone, nothing to do
}

// controlWriteErr writes err as the error document. A usage error carries
// "usage":true so the client can tell what you asked for being wrong from
// the cluster refusing what you asked; see usageErr.
func controlWriteErr(w http.ResponseWriter, code int, err error) {
	doc := map[string]any{"error": err.Error()}
	if isUsage(err) {
		doc["usage"] = true
	}
	controlWrite(w, code, doc)
}

// usageErr is a failure you fix by calling differently: a method we do not
// have, the wrong number of arguments, an argument or a rule that does not
// parse. The client exits 2 for these and 1 for everything else, so a method
// that ran and returned an error is NOT one of these, and neither is a fault
// ID that does not exist or a wait that timed out.
type usageErr struct{ error }

func usagef(format string, args ...any) error {
	return usageErr{fmt.Errorf(format, args...)}
}

func isUsage(err error) bool {
	var u usageErr
	return errors.As(err, &u)
}
