package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/pipeline"
	"github.com/arm092/migres-go/internal/schema"
	gm "github.com/go-mysql-org/go-mysql/mysql"
)

type Request struct {
	Command  string `json:"command"`
	File     string `json:"file"`
	Pos      uint32 `json:"pos"`
	GTID     string `json:"gtid"`
	Discard  bool   `json:"discard_pending"`
	AllowGap bool   `json:"allow_gap"`
	Table    string `json:"table"`
	ID       string `json:"id"`
}
type Response struct {
	Diagnostic string           `json:"diagnostic,omitempty"`
	Operation  *model.Operation `json:"operation,omitempty"`
	Stats      *model.Stats     `json:"stats,omitempty"`
	Error      string           `json:"error,omitempty"`
}

func Control(ctx context.Context, c config.Config, r Request) (Response, error) {
	if r.ID == "" && r.Command != "status" {
		r.ID = journal.ID()
	}
	socket := filepath.Join(c.RuntimeDir, "control.sock")
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	defer transport.CloseIdleConnections()
	body, err := json.Marshal(r)
	if err != nil {
		return Response{}, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://localhost/control", strings.NewReader(string(body)))
	if err != nil {
		return Response{}, err
	}
	resp, err := (&http.Client{Transport: transport}).Do(req)
	if err != nil {
		return Response{}, fmt.Errorf("operation %s: %w", r.ID, err)
	}
	defer resp.Body.Close()
	var out Response
	err = json.NewDecoder(resp.Body).Decode(&out)
	if err == nil && out.Error != "" {
		err = errors.New(out.Error)
	}
	return out, err
}

type controlMessage struct {
	req   Request
	reply chan Response
}

func (a *App) Serve(ctx context.Context) error {
	if err := os.MkdirAll(a.Config.RuntimeDir, 0700); err != nil {
		return err
	}
	path := filepath.Join(a.Config.RuntimeDir, "control.sock")
	if fi, e := os.Lstat(path); e == nil {
		if fi.Mode()&os.ModeSocket == 0 {
			return fmt.Errorf("control path is not a socket")
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return err
	}
	defer listener.Close()
	defer os.Remove(path)
	if err = os.Chmod(path, 0600); err != nil {
		return err
	}
	requests := make(chan controlMessage)
	stopSignals := a.signalControl(ctx, requests)
	defer stopSignals()
	mux := http.NewServeMux()
	mux.HandleFunc("/control", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", 405)
			return
		}
		var req Request
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 65536))
		dec.DisallowUnknownFields()
		if err := dec.Decode(&req); err != nil {
			http.Error(w, "invalid request", 400)
			return
		}
		if req.Command == "status" {
			if req.ID != "" {
				ops, err := a.Store.Operations()
				if err != nil {
					json.NewEncoder(w).Encode(Response{Error: err.Error()})
					return
				}
				for _, op := range ops {
					if op.ID == req.ID {
						copy := op
						json.NewEncoder(w).Encode(Response{Operation: &copy})
						return
					}
				}
				json.NewEncoder(w).Encode(Response{Error: "operation not found"})
				return
			}
			st, err := a.Store.Stats()
			out := Response{Stats: &st}
			a.Store.Meta("last_error", &out.Diagnostic)
			if err != nil {
				out.Error = err.Error()
			}
			json.NewEncoder(w).Encode(out)
			return
		}
		reply := make(chan Response, 1)
		select {
		case requests <- controlMessage{req: req, reply: reply}:
		case <-r.Context().Done():
			return
		case <-ctx.Done():
			return
		}
		select {
		case out := <-reply:
			json.NewEncoder(w).Encode(out)
		case <-r.Context().Done():
		case <-ctx.Done():
		}
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go server.Serve(listener)
	defer server.Close()
	probes := &http.Server{Addr: a.Config.Listen, Handler: a.Handler(), ReadHeaderTimeout: 5 * time.Second}
	probeErr := make(chan error, 1)
	go func() { probeErr <- probes.ListenAndServe() }()
	defer probes.Close()
	retry := time.NewTicker(3 * time.Second)
	defer retry.Stop()
	retryPending := false
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- a.Run(runCtx) }()
	running := true
	// A blocked pipeline remains available for status and recovery. Probes must not
	// turn a permanent data error into an uncontrolled restart loop.
	for {
		select {
		case err := <-probeErr:
			stop()
			if running {
				<-done
			}
			return err
		case <-retry.C:
			if retryPending && !running {
				runCtx, stop = context.WithCancel(ctx)
				go func(run context.Context) { done <- a.Run(run) }(runCtx)
				running = true
				retryPending = false
			}
		case <-ctx.Done():
			stop()
			if running {
				select {
				case <-done:
				case <-time.After(a.Config.ShutdownTimeout + time.Second):
					return fmt.Errorf("pipeline did not stop before deadline")
				}
			}
			return nil
		case err := <-done:
			if err == nil && a.Config.Migration.Mode == "snapshot" {
				stop()
				return nil
			}
			running = false
			a.Ready.Store(false)
			a.Blocked.Store(err != nil)
			retryPending = err != nil && pipeline.Transient(err)
		case msg := <-requests:
			req := msg.req
			if req.ID == "" {
				req.ID = journal.ID()
			}
			ops, err := a.Store.Operations()
			if err != nil {
				msg.reply <- Response{Error: err.Error()}
				continue
			}
			exists := false
			for _, op := range ops {
				if op.ID == req.ID {
					if op.Kind != req.Command || op.Position.File != req.File || op.Position.Pos != req.Pos || op.Position.GTID != req.GTID || op.Table != req.Table || op.DiscardPending != req.Discard || op.AllowGap != req.AllowGap {
						msg.reply <- Response{Error: "operation ID already belongs to a different request"}
						exists = true
						break
					}
					copy := op
					msg.reply <- Response{Operation: &copy, Error: copy.Error}
					exists = true
					break
				}
			}
			if exists {
				continue
			}
			op := model.Operation{ID: req.ID, Kind: req.Command, State: "validating", Position: model.Position{File: req.File, Pos: req.Pos, GTID: req.GTID}, DiscardPending: req.Discard, AllowGap: req.AllowGap, Table: req.Table}
			if err = a.validateRequest(ctx, req); err != nil {
				op.State = "failed"
				op.Error = err.Error()
				a.Store.Operation(op)
				msg.reply <- Response{Operation: &op, Error: op.Error}
				continue
			}
			if err = a.Store.Operation(op); err != nil {
				msg.reply <- Response{Error: err.Error()}
				continue
			}
			a.Ready.Store(false)
			stop()
			if running {
				select {
				case <-done:
					running = false
				case <-time.After(a.Config.ShutdownTimeout + time.Second):
					op.State = "failed"
					op.Error = "workers did not stop; no state changed"
					a.Store.Operation(op)
					msg.reply <- Response{Operation: &op, Error: op.Error}
					continue
				}
			}
			op.State = "applying"
			if err = a.Store.Operation(op); err != nil {
				msg.reply <- Response{Error: err.Error()}
				continue
			}
			err = a.applyRequest(ctx, req)
			if err != nil {
				op.State = "failed"
				op.Error = err.Error()
				a.Blocked.Store(true)
			} else {
				op.State = "done"
				if req.Command == "resnapshot" && req.Table != "" {
					op.State = "queued"
				}
				a.Blocked.Store(false)
			}
			if saveErr := a.Store.Operation(op); saveErr != nil {
				msg.reply <- Response{Operation: &op, Error: saveErr.Error()}
				a.Blocked.Store(true)
				continue
			}
			msg.reply <- Response{Operation: &op, Error: op.Error}
			if err == nil {
				runCtx, stop = context.WithCancel(ctx)
				go func(run context.Context) { done <- a.Run(run) }(runCtx)
				running = true
			}
		}
	}
}
func (a *App) validateRequest(ctx context.Context, r Request) error {
	switch r.Command {
	case "reset", "resnapshot":
		if r.Table != "" {
			if r.Command != "resnapshot" {
				return fmt.Errorf("reset does not accept --table")
			}
			owned, err := a.Store.Owned()
			if err != nil {
				return err
			}
			for _, sc := range owned {
				if sc.Table == r.Table {
					_, err = a.Source.Schema(ctx, r.Table)
					return err
				}
			}
			return fmt.Errorf("table is not owned by this pipeline")
		}
		return nil
	case "reposition":
		if (r.GTID != "") == (r.File != "" || r.Pos != 0) {
			return fmt.Errorf("provide either --gtid-set or --file and --pos")
		}
		var cp model.Position
		if err := a.Store.Meta("checkpoint", &cp); err != nil {
			return err
		}
		if r.GTID != "" {
			set, err := gm.ParseGTIDSet("mysql", r.GTID)
			if err != nil {
				return err
			}
			executed, err := a.Source.Position(ctx)
			if err != nil {
				return err
			}
			current, err := gm.ParseGTIDSet("mysql", executed.GTID)
			if err != nil {
				return err
			}
			if !current.Contain(set) {
				return fmt.Errorf("GTID is not a subset of executed source GTIDs")
			}
			resolved, err := a.resolveGTID(r.GTID)
			if err != nil {
				return err
			}
			r.File = resolved.File
			r.Pos = resolved.Pos
		}
		if r.File == "" || r.Pos < 4 {
			return fmt.Errorf("file and position >= 4 required")
		}
		var initial model.Position
		if err := a.Store.Meta("initial_position", &initial); err != nil {
			return err
		}
		target := model.Position{File: r.File, Pos: r.Pos}
		if a.Config.Migration.CDC.UseGTID && r.GTID == "" {
			boundaries, err := a.Store.Boundaries()
			if err != nil {
				return err
			}
			found := false
			for _, p := range boundaries {
				if p.File == r.File && p.Pos == r.Pos && p.GTID != "" {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("GTID mode requires an exact retained file/position boundary with a GTID set")
			}
		}
		if !pipeline.Reached(target, initial) {
			return fmt.Errorf("requested position predates retained schema history; use resnapshot")
		}
		if !pipeline.Reached(target, cp) {
			history, err := a.Store.History()
			if err != nil {
				return err
			}
			for _, e := range history {
				if pipeline.Reached(cp, e.Position) && !pipeline.Reached(target, e.Position) {
					return fmt.Errorf("rewind crosses DDL; use resnapshot to retain schema correctness")
				}
			}
		}
		if pipeline.Reached(target, cp) && (r.File != cp.File || r.Pos != cp.Pos) && !r.AllowGap {
			return fmt.Errorf("forward reposition requires --allow-gap")
		}
		if r.File == cp.File && r.Pos == cp.Pos {
			return nil
		}
		rows, err := a.Source.DB.QueryContext(ctx, "SHOW BINLOG EVENTS IN "+schema.Literal(r.File)+" FROM "+fmt.Sprint(r.Pos)+" LIMIT 1")
		if err != nil {
			return err
		}
		defer rows.Close()
		if !rows.Next() {
			return fmt.Errorf("binlog position not available")
		}
		var file, kind, info string
		var start, end, id uint64
		if err = rows.Scan(&file, &start, &kind, &id, &end, &info); err != nil {
			return err
		}
		if start != uint64(r.Pos) {
			return fmt.Errorf("position is not an event boundary")
		}
		switch kind {
		case "Gtid", "Anonymous_Gtid", "Format_desc", "Rotate":
			return nil
		case "Query":
			if strings.EqualFold(strings.TrimSpace(info), "BEGIN") {
				return nil
			}
		}
		return fmt.Errorf("position is not a safe transaction boundary")
	default:
		return fmt.Errorf("unsupported command")
	}
}
func (a *App) applyRequest(ctx context.Context, r Request) error {
	if r.Command == "resnapshot" && r.Table != "" {
		fresh, err := a.Source.Schema(ctx, r.Table)
		if err != nil {
			return err
		}
		owned, err := a.Store.Owned()
		if err != nil {
			return err
		}
		for _, old := range owned {
			if old.Table == r.Table {
				var cp model.Position
				if err = a.Store.Meta("checkpoint", &cp); err != nil {
					return err
				}
				if err = a.Store.SetMeta("resnapshot_pending", true); err != nil {
					return err
				}
				return a.Store.Enqueue([]model.Event{{Kind: "ddl", Table: r.Table, Schema: &fresh, Previous: &old, Rebuild: true, Position: cp, SourceID: "operation:" + r.ID, Operation: r.ID}})
			}
		}
		return fmt.Errorf("table is not owned")
	}
	if r.Command == "reposition" {
		// Workers are paused here. Validate again against the final checkpoint,
		// including on recovery, before discarding or delivering pending work.
		if err := a.validateRequest(ctx, r); err != nil {
			return err
		}
		if r.Discard {
			if err := a.Store.DiscardPending(r.ID); err != nil {
				return err
			}
		} else if err := a.drain(ctx); err != nil {
			return err
		}
		if err := a.validateRequest(ctx, r); err != nil {
			return err
		}
		p := model.Position{File: r.File, Pos: r.Pos, GTID: r.GTID}
		if r.GTID != "" {
			var err error
			p, err = a.resolveGTID(r.GTID)
			if err != nil {
				return err
			}
		} else if a.Config.Migration.CDC.UseGTID {
			boundaries, err := a.Store.Boundaries()
			if err != nil {
				return err
			}
			for _, known := range boundaries {
				if known.File == p.File && known.Pos == p.Pos {
					p.GTID = known.GTID
					break
				}
			}
		}
		schemas, err := a.schemasAt(p)
		if err != nil {
			return err
		}
		return a.Store.Reposition(p, schemas, r.AllowGap)
	}
	schemas, err := a.Store.Owned()
	if err != nil {
		return err
	}
	for _, sc := range schemas {
		if err = a.Sink.DropOwned(ctx, sc); err != nil {
			return err
		}
	}
	return a.Store.Reset()
}

func (a *App) schemasAt(p model.Position) ([]model.Schema, error) {
	var initial []model.Schema
	if err := a.Store.Meta("initial_schemas", &initial); err != nil {
		return nil, err
	}
	byName := map[string]model.Schema{}
	for _, sc := range initial {
		byName[sc.Table] = sc
	}
	history, err := a.Store.History()
	if err != nil {
		return nil, err
	}
	for _, e := range history {
		if !pipeline.Reached(p, e.Position) {
			continue
		}
		d, err := schema.Parse(e.Query)
		if err != nil {
			return nil, err
		}
		switch d.Kind {
		case "create", "alter":
			if e.Schema != nil {
				byName[e.Schema.Table] = *e.Schema
			}
		case "drop":
			for _, name := range d.Tables {
				delete(byName, name)
			}
		case "rename":
			for _, pair := range d.Pairs {
				if sc, ok := byName[pair[0]]; ok {
					delete(byName, pair[0])
					sc.Table = pair[1]
					byName[pair[1]] = sc
				}
			}
		}
	}
	out := []model.Schema{}
	for _, sc := range byName {
		out = append(out, sc)
	}
	return out, nil
}
func (a *App) drain(ctx context.Context) error {
	drainCtx, stop := context.WithTimeout(ctx, a.Config.ShutdownTimeout)
	defer stop()
	tctx, tstop := context.WithCancel(drainCtx)
	cctx, cstop := context.WithCancel(drainCtx)
	defer tstop()
	defer cstop()
	tdone := make(chan error, 1)
	cdone := make(chan error, 1)
	go func() {
		tdone <- (&pipeline.Transformer{Store: a.Store, Poll: a.Config.Migration.CDC.TransformerPoll}).Run(tctx)
	}()
	go func() {
		cdone <- (&pipeline.Consumer{Store: a.Store, Target: a, Workers: a.Config.Migration.ConsumerWorkers, Poll: a.Config.Migration.CDC.ConsumerPoll, Force: drainCtx}).Run(cctx)
	}()
	var result error
	var te, ce bool
loop:
	for {
		st, err := a.Store.Stats()
		if err != nil {
			result = err
			break
		}
		if st.Events == 0 && st.Tasks == 0 {
			break
		}
		select {
		case err = <-tdone:
			te = true
			result = err
			break loop
		case err = <-cdone:
			ce = true
			result = err
			break loop
		case <-drainCtx.Done():
			result = fmt.Errorf("pending queues did not drain before timeout")
			break loop
		case <-time.After(20 * time.Millisecond):
		}
	}
	tstop()
	cstop()
	if !te {
		<-tdone
	}
	if !ce {
		<-cdone
	}
	return result
}
