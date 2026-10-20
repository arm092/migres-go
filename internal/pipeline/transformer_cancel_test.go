package pipeline

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
)

// Cancel at a deterministic context checkpoint, avoiding scheduling or row-count
// timing assumptions when exercising cancellation partway through conversion.
type transformCancelContext struct {
	context.Context
	remaining int
	cancel    context.CancelFunc
}

func (c *transformCancelContext) Err() error {
	c.remaining--
	if c.remaining == 0 {
		c.cancel()
	}
	return c.Context.Err()
}
func cancellingTransformContext(checks int) *transformCancelContext {
	ctx, cancel := context.WithCancel(context.Background())
	return &transformCancelContext{Context: ctx, remaining: checks, cancel: cancel}
}
func cancellationEvent() model.Event {
	s := model.Schema{Table: "t", Columns: []model.Column{{Name: "id"}}, Keys: []string{"id"}}
	e := model.Event{Kind: "insert", Table: "t", Schema: &s}
	for i := 0; i < 20; i++ {
		e.Rows = append(e.Rows, model.Change{After: []model.Value{model.Encode(i)}})
	}
	return e
}
func TestTransformCancellationDiscardsPartialRows(t *testing.T) {
	for _, checks := range []int{1, 8} {
		t.Run(fmt.Sprintf("cancel_at_checkpoint_%d", checks), func(t *testing.T) {
			ctx := cancellingTransformContext(checks)
			defer ctx.cancel()
			tasks, err := transform(ctx, cancellationEvent())
			if !errors.Is(err, context.Canceled) || len(tasks) != 0 {
				t.Fatalf("cancelled conversion returned partial tasks: %d %v", len(tasks), err)
			}
		})
	}
}
func TestTransformerCancelledBatchRemainsDurable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.db")
	s, err := journal.Open(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	first := cancellationEvent()
	first.Rows = first.Rows[:1]
	if err = s.Append([]model.Event{first, cancellationEvent()}, model.Position{}); err != nil {
		t.Fatal(err)
	}
	// First event converts successfully. Cancellation interrupts conversion of the
	// second event; neither event may be acknowledged by a partial batch commit.
	ctx := cancellingTransformContext(10)
	defer ctx.cancel()
	if err = (&Transformer{Store: s}).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = journal.Open(path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	events, err := s.Events(10)
	if err != nil || len(events) != 2 {
		t.Fatalf("raw batch lost: %d %v", len(events), err)
	}
	tasks, err := s.Tasks(10)
	if err != nil || len(tasks) != 0 {
		t.Fatalf("partial prepared batch: %d %v", len(tasks), err)
	}
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	done := make(chan error, 1)
	go func() { done <- (&Transformer{Store: s, Poll: time.Millisecond}).Run(runCtx) }()
	deadline := time.After(2 * time.Second)
	for {
		tasks, err = s.Tasks(10)
		if err != nil {
			t.Fatal(err)
		}
		rows := 0
		for _, task := range tasks {
			rows += len(task.Rows)
		}
		if rows == 21 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("reopened batch did not transform")
		case <-time.After(time.Millisecond):
		}
	}
	stop()
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	events, err = s.Events(10)
	if err != nil || len(events) != 0 {
		t.Fatalf("successful transform did not atomically hand off batch: %d %v", len(events), err)
	}
}
