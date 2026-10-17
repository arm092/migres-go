package test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
	"github.com/arm092/migres-go/internal/sink"
)

func TestReplicaLossQuorumAndFailover(t *testing.T) {
	if os.Getenv("MIGRES_FAULTS") != "1" {
		t.Skip("opt-in: stops only ch-r2 in migres-go-replicated fixture")
	}
	c := testConfig(t)
	c.ClickHouse.Addresses = []string{"127.0.0.1:39001", "127.0.0.1:39002"}
	c.ClickHouse.Engine = "ReplicatedReplacingMergeTree"
	c.ClickHouse.Cluster = "analytics"
	c.ClickHouse.WriteAck.Timeout = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	s := model.Schema{Table: fmt.Sprintf("fault_%d", time.Now().UnixNano()), Generation: fmt.Sprintf("gen_%d", time.Now().UnixNano()), Columns: []model.Column{{Name: "id", CHType: "Int64"}}, Keys: []string{"id"}}
	if err = a.Sink.Ensure(ctx, s); err != nil {
		t.Fatal(err)
	}
	fixture := func(args ...string) error {
		all := append([]string{"compose", "-p", "migres-go-replicated", "-f", "replicated.yml"}, args...)
		out, e := exec.Command("docker", all...).CombinedOutput()
		if e != nil {
			return fmt.Errorf("fixture: %w: %s", e, out)
		}
		return nil
	}
	defer func() {
		if e := fixture("up", "-d", "--wait", "ch-r2"); e != nil {
			t.Error(e)
		}
		if e := a.Sink.DropOwned(context.Background(), s); e != nil {
			t.Error(e)
		}
	}()
	if err = fixture("stop", "ch-r2"); err != nil {
		t.Fatal(err)
	}
	task := model.Task{Kind: "insert", Table: s.Table, Schema: &s, Token: s.Generation + ":quorum", Rows: []model.Row{{Values: []model.Value{model.Encode(int64(1))}, Version: 2}}}
	short, stop := context.WithTimeout(ctx, 5*time.Second)
	err = a.Sink.Execute(short, task)
	stop()
	if err == nil {
		t.Fatal("majority quorum acknowledged with one of two replicas offline")
	}
	singleConfig := c
	singleConfig.ClickHouse.WriteAck.Mode = "single"
	singleConfig.ClickHouse.Addresses = []string{"127.0.0.1:39002", "127.0.0.1:39001"}
	single, err := sink.Open(singleConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer single.Close()
	if err = single.Check(ctx); err != nil {
		t.Fatal(err)
	}
	other := task
	other.Token = s.Generation + ":single"
	other.Rows = []model.Row{{Values: []model.Value{model.Encode(int64(2))}, Version: 3}}
	if err = single.Execute(ctx, other); err != nil {
		t.Fatal(err)
	}
	if err = fixture("up", "-d", "--wait", "ch-r2"); err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, func() bool { return a.Sink.Execute(ctx, task) == nil })
	for _, endpoint := range c.ClickHouse.Addresses {
		pc := c
		pc.ClickHouse.Addresses = []string{endpoint}
		peer, e := sink.Open(pc)
		if e != nil {
			t.Fatal(e)
		}
		defer peer.Close()
		wait(t, ctx, func() bool {
			var n uint64
			e := peer.Conn.QueryRow(ctx, "SELECT count() FROM "+schema.Ref(c.ClickHouse.Database, s.Table)+" FINAL WHERE _migres_deleted=0").Scan(&n)
			return e == nil && n == 2
		})
	}
}
