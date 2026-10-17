package pipeline

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strings"
	"sync"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
)

type Target interface {
	Execute(context.Context, model.Task) error
}
type Consumer struct {
	Store      *journal.Store
	Target     Target
	Workers    int
	Poll       time.Duration
	Force      context.Context
	BatchLimit int
}

func Transient(err error) bool {
	if errors.Is(err, schema.ErrSnapshotInvalidated) {
		return true
	}
	var ne net.Error
	if errors.As(err, &ne) {
		return true
	}
	var ce *ch.Exception
	if errors.As(err, &ce) {
		switch ce.Code {
		case 159, 202, 203, 209, 210, 225, 242, 252, 285, 319, 999:
			return true
		}
		return false
	}
	text := strings.ToLower(err.Error())
	for _, s := range []string{"connection", "broken pipe", "eof", "timeout", "keeper", "quorum", "temporarily unavailable"} {
		if strings.Contains(text, s) {
			return true
		}
	}
	return false
}
func (c *Consumer) execute(ctx context.Context, t model.Task) error {
	for attempt := 0; ; attempt++ {
		err := c.Target.Execute(ctx, t)
		if err == nil {
			return c.Store.Ack(t.ID)
		}
		if ctx.Err() != nil {
			return nil
		}
		if !Transient(err) {
			return fmt.Errorf("task %d table %s blocked: %w", t.ID, t.Table, err)
		}
		if err := c.Store.Increment("consumer_retries", 1); err != nil {
			return err
		}
		delay := time.Second << min(attempt, 5)
		delay += time.Duration(rand.Int64N(int64(delay/4 + 1)))
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(delay):
		}
	}
}
func (c *Consumer) Run(ctx context.Context) error {
	force := c.Force
	if force == nil {
		force = context.Background()
	}
	workCtx, cancel := context.WithCancel(force)
	inflight := map[uint64]bool{}
	tables := map[string]bool{}
	type result struct {
		id    uint64
		table string
		err   error
	}
	results := make(chan result, c.Workers)
	var wg sync.WaitGroup
	defer func() {
		for len(inflight) > 0 {
			select {
			case r := <-results:
				delete(inflight, r.id)
			case <-force.Done():
				cancel()
				wg.Wait()
				return
			}
		}
		cancel()
		wg.Wait()
	}()
	for {
		if ctx.Err() != nil {
			return nil
		}
		// Reconcile acknowledgements before fetching; inflight prevents repeat dispatch.
		for {
			select {
			case r := <-results:
				delete(inflight, r.id)
				delete(tables, r.table)
				if r.err != nil {
					cancel()
					return r.err
				}
			default:
				goto fetched
			}
		}
	fetched:
		limit := c.BatchLimit
		if limit <= 0 {
			limit = 100
		}
		tasks, err := c.Store.Tasks(limit)
		if err != nil {
			cancel()
			return err
		}
		launched := false
		for _, t := range tasks {
			if inflight[t.ID] {
				continue
			}
			if t.Kind == "ddl" || t.Kind == "fence" {
				if len(inflight) == 0 {
					if err = c.execute(workCtx, t); err != nil {
						cancel()
						return err
					}
					launched = true
				}
				break
			}
			if tables[t.Table] || len(inflight) >= c.Workers {
				continue
			}
			inflight[t.ID] = true
			tables[t.Table] = true
			wg.Add(1)
			launched = true
			go func(task model.Task) {
				defer wg.Done()
				err := c.execute(workCtx, task)
				results <- result{id: task.ID, table: task.Table, err: err}
			}(t)
		}
		if !launched {
			select {
			case <-ctx.Done():
				return nil
			case r := <-results:
				delete(inflight, r.id)
				delete(tables, r.table)
				if r.err != nil {
					cancel()
					return r.err
				}
			case <-time.After(c.Poll):
			}
		}
	}
}
