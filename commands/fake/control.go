package fake

import (
	"encoding/hex"
	"encoding/json/v2"
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

	fn reflect.Value
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
			Signature: strings.TrimPrefix(fn.Type().String(), "func"),
			fn:        fn,
		})
	}
	slices.SortFunc(ms, func(a, b controlMethod) int { return strings.Compare(a.Name, b.Name) })
	return ms
}

// callable reports whether we can build every argument of fn from a string.
// Funcs are the interesting exclusion: they are how Control, ControlKey and
// SleepControl take their callbacks, and there is no sending one of those
// over a wire. We skip variadic methods too; kfake has none today, and one
// that appears later is better absent than half working.
func callable(fn reflect.Type) bool {
	if fn.IsVariadic() {
		return false
	}
	for i := range fn.NumIn() {
		switch fn.In(i).Kind() {
		case reflect.Func, reflect.Chan, reflect.Interface, reflect.UnsafePointer:
			return false
		}
	}
	return true
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

// call invokes m. A trailing error return comes back as an error rather than
// as a result; what is left is the result, alone if there is one and as a
// list if there are more.
func (m controlMethod) call(args []string) (any, error) {
	ft := m.fn.Type()
	if len(args) != ft.NumIn() {
		return nil, fmt.Errorf("%s takes %d argument(s), got %d", m.Name, ft.NumIn(), len(args))
	}
	in := make([]reflect.Value, len(args))
	for i, arg := range args {
		v, err := buildArg(ft.In(i), arg)
		if err != nil {
			return nil, fmt.Errorf("argument %d (%s): %v", i+1, ft.In(i), err)
		}
		in[i] = v
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
			controlWriteErr(w, http.StatusNotFound, fmt.Errorf("unknown method %q, GET /methods lists what we can call", name))
			return
		}
		var req struct {
			Args []string `json:"args"`
		}
		if err := json.UnmarshalRead(r.Body, &req, json.RejectUnknownMembers(true)); err != nil {
			controlWriteErr(w, http.StatusBadRequest, err)
			return
		}
		res, err := m.call(req.Args)
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

func controlWriteErr(w http.ResponseWriter, code int, err error) {
	controlWrite(w, code, map[string]any{"error": err.Error()})
}
