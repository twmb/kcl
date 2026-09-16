package fake

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
)

// groupWait is one wait on a group: which group, the shape to wait for, and
// how long to wait for it. Every selector is an AND filter and an unset
// selector matches anything, but a wait with no selector at all is a usage
// error, since it would return the moment the group exists.
type groupWait struct {
	Group    string `json:"group"`
	Members  *int   `json:"members,omitzero"`  // how many members the group has
	State    string `json:"state,omitzero"`    // Empty, Stable, ...; compared without case
	Assigned *int   `json:"assigned,omitzero"` // partitions assigned across all members
	Timeout  string `json:"timeout,omitzero"`  //
}

// match is the predicate we wait on. kfake calls it with nil while the group
// does not exist, and nothing we can select on is true of a group that is not
// there, so a wait for a group also waits for it to appear.
func (g groupWait) match(info *kfake.GroupInfo) bool {
	if info == nil {
		return false
	}
	if g.Members != nil && len(info.Members) != *g.Members {
		return false
	}
	if g.State != "" && !strings.EqualFold(info.State, g.State) {
		return false
	}
	if g.Assigned != nil && info.NumAssigned() != *g.Assigned {
		return false
	}
	return true
}

// groupShape says what the group looked like when we stopped waiting, in the
// order the selectors go on the command line.
func groupShape(info *kfake.GroupInfo) string {
	if info == nil {
		return "no such group"
	}
	return fmt.Sprintf("%s, %d member(s), %d assigned", info.State, len(info.Members), info.NumAssigned())
}

// groupRoutes adds the group endpoints to mux.
func groupRoutes(mux *http.ServeMux, c *kfake.Cluster) {
	// Wait blocks until the group has the shape the caller asked for, which
	// is how a caller out of process waits out a rebalance.
	mux.HandleFunc("POST /groups/wait", func(w http.ResponseWriter, r *http.Request) {
		req := groupWait{Timeout: "30s"}
		if err := json.UnmarshalRead(r.Body, &req, json.RejectUnknownMembers(true)); err != nil {
			controlWriteErr(w, http.StatusBadRequest, usageErr{err})
			return
		}
		if req.Group == "" {
			controlWriteErr(w, http.StatusBadRequest, usagef("no group given"))
			return
		}
		if req.Members == nil && req.State == "" && req.Assigned == nil {
			controlWriteErr(w, http.StatusBadRequest, usagef("give at least one of members, state, assigned"))
			return
		}
		timeout, err := time.ParseDuration(req.Timeout)
		if err != nil {
			controlWriteErr(w, http.StatusBadRequest, usagef("timeout %q: %v", req.Timeout, err))
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		info, err := c.WaitGroupInfo(ctx, req.Group, req.match)
		if err != nil {
			// The timeout we were given is what ran out, so say that
			// rather than handing back the context's own wording. Any
			// other failure is the caller hanging up, which is not.
			if errors.Is(err, context.DeadlineExceeded) {
				err = fmt.Errorf("timed out after %s waiting for group %s: %s", timeout, req.Group, groupShape(info))
			} else {
				err = fmt.Errorf("waiting for group %s: %v", req.Group, err)
			}
			controlWriteErr(w, http.StatusRequestTimeout, err)
			return
		}
		controlWrite(w, http.StatusOK, map[string]any{"group": info})
	})
}
