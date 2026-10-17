package test

import (
	"context"
	"fmt"
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
		c.ClickHouse.Addresses = []string{"127.0.0.1:39001", "127.0.0.1:39002"}
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
	s := model.Schema{Table: fmt.Sprintf("alter_replay_%d", time.Now().UnixNano()), Generation: fmt.Sprintf("gen_%d", time.Now().UnixNano()), Columns: []model.Column{{Name: "id", MySQLType: "bigint", CHType: "Int64"}, {Name: "a", MySQLType: "int", CHType: "Int32"}}, Keys: []string{"id"}}
	if err = a.Sink.Ensure(ctx, s); err != nil {
		t.Fatal(err)
	}
	defer a.Sink.DropOwned(context.Background(), s)
	if err = a.Sink.Execute(ctx, model.Task{Kind: "insert", Table: s.Table, Schema: &s, Token: s.Generation + ":row", Rows: []model.Row{{Values: []model.Value{model.Encode(int64(1)), model.Encode(int32(99))}, Version: 2}}}); err != nil {
		t.Fatal(err)
	}
	query := "ALTER TABLE `" + s.Table + "` RENAME COLUMN a TO b, ADD COLUMN a INT NOT NULL DEFAULT 0"
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
	if err = a.Sink.Conn.QueryRow(ctx, "SELECT b,a FROM "+schema.Ref(c.ClickHouse.Database, s.Table)+" FINAL WHERE _migres_deleted=0").Scan(&old, &freshValue); err != nil || old != 99 || freshValue != 0 {
		t.Fatalf("data changed during replay: %d %d %v", old, freshValue, err)
	}
}
