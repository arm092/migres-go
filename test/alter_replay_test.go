package test

import (
	"context"
	"fmt"
	"github.com/arm092/migres-go/internal/sink"
	"net"
	"os"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
)

func TestCompoundAlterReplayReusedName(t *testing.T) {
	c := testConfig(t)
	if os.Getenv("MIGRES_REPLICATED") == "1" {
		c.ClickHouse.Addresses = []string{net.JoinHostPort(c.ClickHouse.Host, "39001"), net.JoinHostPort(c.ClickHouse.Host, "39002")}
		c.ClickHouse.Engine = "ReplicatedReplacingMergeTree"
		c.ClickHouse.Cluster = "analytics"
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	s := model.Schema{Table: fmt.Sprintf("alter_replay_%d", time.Now().UnixNano()), Generation: fmt.Sprintf("gen_%d", time.Now().UnixNano()), Columns: []model.Column{{Name: "id", MySQLType: "bigint", CHType: "Int64"}, {Name: "a", MySQLType: "int", CHType: "Int32"}, {Name: "obsolete", MySQLType: "int", CHType: "Int32"}, {Name: "label", MySQLType: "varchar(20)", CHType: "String"}}, Keys: []string{"id"}}
	if err = a.Sink.Ensure(ctx, s); err != nil {
		t.Fatal(err)
	}
	defer a.Sink.DropOwned(context.Background(), s)
	if err = a.Sink.Execute(ctx, model.Task{Kind: "insert", Table: s.Table, Schema: &s, Token: s.Generation + ":row", Rows: []model.Row{{Values: []model.Value{model.Encode(int64(1)), model.Encode(int32(99)), model.Encode(int32(7)), model.Encode("preserved")}, Version: 2}}}); err != nil {
		t.Fatal(err)
	}
	query := "ALTER TABLE `" + s.Table + "` RENAME COLUMN a TO b, ADD COLUMN a INT NOT NULL DEFAULT 0, DROP COLUMN obsolete, CHANGE COLUMN label title VARCHAR(20) NOT NULL DEFAULT 'new'"
	fresh, rebuild, err := schema.Apply(s, query, c)
	if err != nil || rebuild {
		t.Fatal(rebuild, err)
	}
	task := model.Task{Kind: "ddl", Table: s.Table, Schema: &fresh, Previous: &s, SQL: query, Token: s.Generation + ":ddl"}
	for i := 0; i < 2; i++ {
		if err = a.Sink.Alter(ctx, task); err != nil {
			t.Fatalf("attempt %d: %v", i, err)
		}
	}
	var old, freshValue int32
	var title string
	if err = a.Sink.Conn.QueryRow(ctx, "SELECT b,a,title FROM "+schema.Ref(c.ClickHouse.Database, s.Table)+" FINAL WHERE _migres_deleted=0").Scan(&old, &freshValue, &title); err != nil || old != 99 || freshValue != 0 || title != "preserved" {
		t.Fatalf("data changed during replay: %d %d %v", old, freshValue, err)
	}
}

func TestRenameReplay(t *testing.T) {
	c := testConfig(t)
	if os.Getenv("MIGRES_REPLICATED") == "1" {
		c.ClickHouse.Addresses = []string{net.JoinHostPort(c.ClickHouse.Host, "39001"), net.JoinHostPort(c.ClickHouse.Host, "39002")}
		c.ClickHouse.Engine = "ReplicatedReplacingMergeTree"
		c.ClickHouse.Cluster = "analytics"
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		owned, _ := a.Store.Owned()
		for _, s := range owned {
			a.Sink.DropOwned(context.Background(), s)
		}
		a.Close()
	}()
	first := model.Schema{Table: fmt.Sprintf("swap_%d", time.Now().UnixNano()), Generation: fmt.Sprintf("ga_%d", time.Now().UnixNano()), Columns: []model.Column{{Name: "id", CHType: "Int64"}}, Keys: []string{"id"}}
	second := first
	second.Table += "b"
	second.Generation += "b"
	temp := first.Table + "tmp"
	for i, s := range []model.Schema{first, second} {
		if err = a.Sink.Ensure(ctx, s); err != nil {
			t.Fatal(err)
		}
		if err = a.Sink.Execute(ctx, model.Task{Kind: "insert", Table: s.Table, Schema: &s, Token: s.Generation, Rows: []model.Row{{Values: []model.Value{model.Encode(int64(i + 1))}, Version: 2}}}); err != nil {
			t.Fatal(err)
		}
	}
	pairs := [][2]string{{first.Table, temp}, {second.Table, first.Table}, {temp, second.Table}}
	task := model.Task{Token: first.Generation + ":rename"}
	plan := []struct {
		Pair       [2]string
		Generation string
	}{{pairs[0], first.Generation}, {pairs[1], second.Generation}, {pairs[2], first.Generation}}
	if err = a.Store.SetMeta("renameplan:"+task.Token, plan); err != nil {
		t.Fatal(err)
	}
	cluster := ""
	if c.ClickHouse.Engine == "ReplicatedReplacingMergeTree" {
		cluster = " ON CLUSTER `analytics`"
	}
	// Crash after SQL/ownership commit but before the step acknowledgement.
	if err = a.Sink.Rename(ctx, pairs[0], cluster); err != nil {
		t.Fatal(err)
	}
	a.Close()
	a, err = app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = a.Sink.RenameTask(ctx, task, pairs, cluster); err != nil {
			t.Fatal(err)
		}
	}
	endpoints := c.ClickHouse.Addresses
	if len(endpoints) == 0 {
		endpoints = []string{""}
	}
	for _, endpoint := range endpoints {
		conn := a.Sink.Conn
		var peer *sink.ClickHouse
		if endpoint != "" {
			cfg := c
			cfg.ClickHouse.Addresses = []string{endpoint}
			peer, err = sink.Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			conn = peer.Conn
		}
		for i, table := range []string{first.Table, second.Table} {
			var id int64
			if err = conn.QueryRow(ctx, "SELECT id FROM "+schema.Ref(c.ClickHouse.Database, table)+" FINAL WHERE _migres_deleted=0").Scan(&id); err != nil || id != int64(2-i) {
				t.Fatalf("swap replay changed %s on %s id=%d err=%v", table, endpoint, id, err)
			}
		}
		if peer != nil {
			peer.Close()
		}
	}
}
