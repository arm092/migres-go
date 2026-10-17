package pipeline

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
)

type blockingTarget struct {
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
}

func (b *blockingTarget) Execute(ctx context.Context, t model.Task) error {
	if b.calls.Add(1) == 1 {
		close(b.started)
	}
	select {
	case <-b.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func TestConsumerDoesNotDispatchInflightTwiceAndDrains(t *testing.T) {
	s, err := journal.Open(filepath.Join(t.TempDir(), "db"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Append([]model.Event{{Kind: "insert"}}, model.Position{})
	events, _ := s.Events(1)
	s.Transform(events[0].Sequence, []model.Task{{Kind: "insert", Table: "t"}})
	target := &blockingTarget{started: make(chan struct{}), release: make(chan struct{})}
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	force, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- (&Consumer{Store: s, Target: target, Workers: 2, Poll: time.Millisecond, Force: force}).Run(ctx)
	}()
	<-target.started
	time.Sleep(30 * time.Millisecond)
	if target.calls.Load() != 1 {
		t.Fatal("duplicate in-flight dispatch")
	}
	stop()
	select {
	case <-done:
		t.Fatal("consumer exited before current insert completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(target.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	tasks, _ := s.Tasks(1)
	if len(tasks) != 0 {
		t.Fatal("successful drain was not acknowledged")
	}
}
