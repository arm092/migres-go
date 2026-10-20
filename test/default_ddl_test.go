package test

import (
	"context"
	"fmt"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
	"github.com/arm092/migres-go/internal/sink"
	"github.com/arm092/migres-go/internal/source"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func TestAlterDefaultCDC(t *testing.T) {
	c := testConfig(t)
	name := fmt.Sprintf("default_cdc_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	m, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	exec := func(q string) {
		t.Helper()
		if _, e := m.DB.ExecContext(ctx, q); e != nil {
			t.Fatal(e)
		}
	}
	exec("CREATE TABLE `" + name + "` (id INT PRIMARY KEY,v BIGINT NOT NULL DEFAULT -10,d DECIMAL(12,2) NULL DEFAULT 1.25,stamp TIMESTAMP NULL DEFAULT NULL)")
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	exec("INSERT INTO `" + name + "` VALUES(1,-10,-2.50,'2020-01-02 03:04:05')")
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
	run, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- a.Run(run) }()
	defer func() {
		stop()
		if e := <-done; e != nil {
			t.Error(e)
		}
	}()
	wait(t, ctx, func() bool { return a.Ready.Load() })
	var version string
	if err = m.DB.QueryRowContext(ctx, "SELECT VERSION()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	timestampClause := ", ALTER COLUMN stamp SET DEFAULT (CURRENT_TIMESTAMP)"
	if strings.HasPrefix(version, "5.7.") {
		timestampClause = ""
	}
	for i, clause := range []string{"ALTER COLUMN v SET DEFAULT 6, ALTER COLUMN d SET DEFAULT 8.75" + timestampClause, "ALTER COLUMN v DROP DEFAULT, ALTER COLUMN d DROP DEFAULT, ALTER COLUMN stamp DROP DEFAULT"} {
		exec("ALTER TABLE `" + name + "` " + clause)
		if i == 0 {
			exec("INSERT INTO `" + name + "` (id) VALUES(2)")
		} else {
			exec("INSERT INTO `" + name + "` (id,v,d,stamp) VALUES(3,7,NULL,NULL)")
		}
		expected := uint64(i + 2)
		wait(t, ctx, func() bool {
			var n uint64
			e := a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+name+"` FINAL WHERE _migres_deleted=0").Scan(&n)
			return e == nil && n == expected
		})
		var typ, kind, expr string
		if e := a.Sink.Conn.QueryRow(ctx, "SELECT type,default_kind,default_expression FROM system.columns WHERE database=currentDatabase() AND table=? AND name='v'", name).Scan(&typ, &kind, &expr); e != nil || typ != "Int64" || (i == 0 && (kind != "DEFAULT" || expr != "6")) || (i == 1 && kind != "") {
			t.Fatalf("metadata: %s %s %s %v", typ, kind, expr, e)
		}
		for _, column := range []struct{ name, typ string }{{"d", "Nullable(Decimal(12, 2))"}, {"stamp", "Nullable(DateTime64(0, 'UTC'))"}} {
			if e := a.Sink.Conn.QueryRow(ctx, "SELECT type,default_kind FROM system.columns WHERE database=currentDatabase() AND table=? AND name=?", name, column.name).Scan(&typ, &kind); e != nil || typ != column.typ || (i == 1 && kind != "") {
				t.Fatalf("%s metadata: %s %s %v", column.name, typ, kind, e)
			}
		}
	}
	var n uint64
	if e := a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+name+"` FINAL WHERE _migres_deleted=0 AND ((id=1 AND v=-10 AND d=-2.50 AND stamp=toDateTime('2020-01-02 03:04:05','UTC')) OR (id=2 AND v=6 AND d=8.75 AND (stamp IS NOT NULL OR "+fmt.Sprint(strings.HasPrefix(version, "5.7."))+")) OR (id=3 AND v=7 AND d IS NULL AND stamp IS NULL))").Scan(&n); e != nil || n != 3 {
		t.Fatalf("defaults changed stored rows or future values: %d %v", n, e)
	}
}

func TestAlterDefaultReplayAfterLostResponse(t *testing.T) {
	c := testConfig(t)
	defaultReplicaConfig(&c)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	name := fmt.Sprintf("default_replay_%d", time.Now().UnixNano())
	s, err := schema.FromCreate("CREATE TABLE `"+name+"`(id INT PRIMARY KEY,v BIGINT NOT NULL DEFAULT -10)", c.MySQL.Database, name, c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { a.Sink.DropOwned(context.Background(), s); a.Close() }()
	if err = a.Sink.Ensure(ctx, s); err != nil {
		t.Fatal(err)
	}
	if err = a.Sink.Execute(ctx, model.Task{Kind: "insert", Table: name, Schema: &s, Token: name + ":row", Rows: []model.Row{{Values: []model.Value{model.Encode(1), model.Encode(int64(-10))}, Version: 1}}}); err != nil {
		t.Fatal(err)
	}
	for i, clause := range []string{"SET DEFAULT 6", "DROP DEFAULT"} {
		query := "ALTER TABLE `" + name + "` ALTER COLUMN v " + clause
		next, rebuild, e := schema.Apply(s, query, c)
		if e != nil || rebuild {
			t.Fatal(e, rebuild)
		}
		task := model.Task{Kind: "ddl", Table: name, Previous: &s, Schema: &next, SQL: query, Token: fmt.Sprintf("%s:ddl:%d", name, i)}
		sql := "MODIFY COLUMN v Int64 DEFAULT 6"
		if i == 1 {
			sql = "MODIFY COLUMN v REMOVE DEFAULT"
		}
		// Freeze is durable before the first replica applies the new default.
		defaultPeers(t, ctx, c, func(peer *sink.ClickHouse) {
			if e := peer.Conn.Exec(ctx, "ALTER TABLE "+schema.Ref(c.ClickHouse.Database, name)+" MATERIALIZE COLUMN v SETTINGS mutations_sync=2"); e != nil {
				t.Fatal(e)
			}
		})
		if e = a.Store.SetMeta("alter:"+task.Token+":step:0:frozen", true); e != nil {
			t.Fatal(e)
		}
		// The server applied DDL, but the client lost the response before its marker.
		peerConfig := c
		if len(c.ClickHouse.Addresses) > 0 {
			peerConfig.ClickHouse.Addresses = c.ClickHouse.Addresses[:1]
		}
		first, e := sink.Open(peerConfig)
		if e != nil {
			t.Fatal(e)
		}
		if e = first.Conn.Exec(ctx, "ALTER TABLE "+schema.Ref(c.ClickHouse.Database, name)+" "+sql); e != nil {
			t.Fatal(e)
		}
		first.Close()
		a.Close()
		a, e = app.Open(c)
		if e != nil {
			t.Fatal(e)
		}
		for attempt := 0; attempt < 2; attempt++ {
			if e = a.Sink.Alter(ctx, task); e != nil {
				t.Fatal(e)
			}
		}
		defaultPeers(t, ctx, c, func(peer *sink.ClickHouse) {
			var typ, kind, expr string
			var value int64
			if e = peer.Conn.QueryRow(ctx, "SELECT type,default_kind,default_expression FROM system.columns WHERE database=currentDatabase() AND table=? AND name='v'", name).Scan(&typ, &kind, &expr); e != nil || typ != "Int64" || (i == 0 && (kind != "DEFAULT" || expr != "6")) || (i == 1 && (kind != "" || expr != "")) {
				t.Fatalf("%s %s %v", typ, kind, e)
			}
			if e = peer.Conn.QueryRow(ctx, "SELECT v FROM "+schema.Ref(c.ClickHouse.Database, name)+" FINAL WHERE id=1 LIMIT 1").Scan(&value); e != nil || value != -10 {
				t.Fatalf("stored negative changed: %d %v", value, e)
			}
		})
		s = next
	}
}

func TestAlterDefaultCompoundBackfill(t *testing.T) {
	c := testConfig(t)
	defaultReplicaConfig(&c)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { a.Close() }()
	name := fmt.Sprintf("default_compound_%d", time.Now().UnixNano())
	s, err := schema.FromCreate("CREATE TABLE `"+name+"`(id INT PRIMARY KEY)", c.MySQL.Database, name, c)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Sink.Ensure(ctx, s); err != nil {
		t.Fatal(err)
	}
	defer func() { a.Sink.DropOwned(context.Background(), s) }()
	if err = a.Sink.Execute(ctx, model.Task{Kind: "insert", Table: name, Schema: &s, Token: name + ":row", Rows: []model.Row{{Values: []model.Value{model.Encode(1)}, Version: 1}}}); err != nil {
		t.Fatal(err)
	}
	sql := "ALTER TABLE `" + name + "` ADD COLUMN v INT NOT NULL DEFAULT 1, ALTER COLUMN v SET DEFAULT 2"
	next, rebuild, err := schema.Apply(s, sql, c)
	if err != nil || rebuild {
		t.Fatal(err, rebuild)
	}
	task := model.Task{Kind: "ddl", Table: name, Previous: &s, Schema: &next, SQL: sql, Token: name + ":ddl"}
	for i := 0; i < 2; i++ {
		if err = a.Sink.Alter(ctx, task); err != nil {
			t.Fatal(err)
		}
	}
	var value int32
	var expr string
	if err = a.Sink.Conn.QueryRow(ctx, "SELECT v FROM "+schema.Ref(c.ClickHouse.Database, name)+" FINAL WHERE id=1 LIMIT 1").Scan(&value); err != nil || value != 1 {
		t.Fatalf("ADD backfill changed: %d %v", value, err)
	}
	if err = a.Sink.Conn.QueryRow(ctx, "SELECT default_expression FROM system.columns WHERE database=currentDatabase() AND table=? AND name='v'", name).Scan(&expr); err != nil || expr != "2" {
		t.Fatalf("final default: %s %v", expr, err)
	}
	if err = a.Sink.Conn.Exec(ctx, "ALTER TABLE "+schema.Ref(c.ClickHouse.Database, name)+defaultCluster(c)+" ADD COLUMN legacy Int32 DEFAULT 3"); err != nil {
		t.Fatal(err)
	}
	next.Columns = append(next.Columns, model.Column{Name: "legacy", MySQLType: "INT", CHType: "Int32"})
	previous := next
	sql = "ALTER TABLE `" + name + "` ALTER COLUMN legacy SET DEFAULT 9"
	next, _, err = schema.Apply(previous, sql, c)
	if err != nil {
		t.Fatal(err)
	}
	task = model.Task{Kind: "ddl", Table: name, Previous: &previous, Schema: &next, SQL: sql, Token: name + ":legacy"}
	// Freeze acknowledgement survives a crash after the new DEFAULT applied.
	if err = a.Sink.Conn.Exec(ctx, "ALTER TABLE "+schema.Ref(c.ClickHouse.Database, name)+defaultCluster(c)+" MATERIALIZE COLUMN legacy SETTINGS mutations_sync=2"); err != nil {
		t.Fatal(err)
	}
	if err = a.Store.SetMeta("alter:"+task.Token+":step:0:frozen", true); err != nil {
		t.Fatal(err)
	}
	if err = a.Sink.Conn.Exec(ctx, "ALTER TABLE "+schema.Ref(c.ClickHouse.Database, name)+" MODIFY COLUMN legacy Int32 DEFAULT 9"); err != nil {
		t.Fatal(err)
	}
	a.Close()
	a, err = app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	if err = a.Sink.Alter(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err = a.Sink.Conn.QueryRow(ctx, "SELECT legacy FROM "+schema.Ref(c.ClickHouse.Database, name)+" FINAL WHERE id=1 LIMIT 1").Scan(&value); err != nil || value != 3 {
		t.Fatalf("legacy virtual default changed: %d %v", value, err)
	}

	previous = next
	sql = "ALTER TABLE `" + name + "` ALTER COLUMN legacy DROP DEFAULT"
	next, _, err = schema.Apply(previous, sql, c)
	if err != nil {
		t.Fatal(err)
	}
	task = model.Task{Kind: "ddl", Table: name, Previous: &previous, Schema: &next, SQL: sql, Token: name + ":legacydrop"}
	if err = a.Sink.Alter(ctx, task); err != nil {
		t.Fatal(err)
	}
	if err = a.Sink.Conn.QueryRow(ctx, "SELECT legacy FROM "+schema.Ref(c.ClickHouse.Database, name)+" FINAL WHERE id=1 LIMIT 1").Scan(&value); err != nil || value != 3 {
		t.Fatalf("later DROP changed frozen legacy default: %d %v", value, err)
	}

	// MODIFY/CHANGE without a source DEFAULT must remove an existing target one.
	for i, clause := range []string{"ALTER COLUMN legacy SET DEFAULT 5", "MODIFY COLUMN legacy INT NOT NULL"} {
		previous = next
		sql = "ALTER TABLE `" + name + "` " + clause
		next, _, err = schema.Apply(previous, sql, c)
		if err != nil {
			t.Fatal(err)
		}
		task = model.Task{Kind: "ddl", Table: name, Previous: &previous, Schema: &next, SQL: sql, Token: fmt.Sprintf("%s:legacy-modify:%d", name, i)}
		if err = a.Sink.Alter(ctx, task); err != nil {
			t.Fatal(err)
		}
	}
	defaultPeers(t, ctx, c, func(peer *sink.ClickHouse) {
		var value int32
		var typ, kind, expr string
		if e := peer.Conn.QueryRow(ctx, "SELECT legacy FROM "+schema.Ref(c.ClickHouse.Database, name)+" FINAL WHERE id=1 LIMIT 1").Scan(&value); e != nil || value != 3 {
			t.Fatalf("replica legacy value %d %v", value, e)
		}
		if e := peer.Conn.QueryRow(ctx, "SELECT type,default_kind,default_expression FROM system.columns WHERE database=currentDatabase() AND table=? AND name='legacy'", name).Scan(&typ, &kind, &expr); e != nil || typ != "Int32" || kind != "" || expr != "" {
			t.Fatalf("replica legacy metadata %s %s %s %v", typ, kind, expr, e)
		}
	})

}

func defaultReplicaConfig(c *config.Config) {
	if os.Getenv("MIGRES_REPLICATED") != "1" {
		return
	}
	c.ClickHouse.Addresses = []string{net.JoinHostPort(c.ClickHouse.Host, "39001"), net.JoinHostPort(c.ClickHouse.Host, "39002")}
	c.ClickHouse.Engine = "ReplicatedReplacingMergeTree"
	c.ClickHouse.Cluster = "analytics"
}
func defaultCluster(c config.Config) string {
	if c.ClickHouse.Engine == "ReplicatedReplacingMergeTree" {
		return " ON CLUSTER `analytics`"
	}
	return ""
}
func defaultPeers(t *testing.T, ctx context.Context, c config.Config, check func(*sink.ClickHouse)) {
	t.Helper()
	endpoints := c.ClickHouse.Addresses
	if len(endpoints) == 0 {
		endpoints = []string{net.JoinHostPort(c.ClickHouse.Host, fmt.Sprint(c.ClickHouse.Port))}
	}
	for _, endpoint := range endpoints {
		cfg := c
		cfg.ClickHouse.Addresses = []string{endpoint}
		peer, e := sink.Open(cfg)
		if e != nil {
			t.Fatal(e)
		}
		check(peer)
		peer.Close()
	}
}

// Independent MergeTree replicas make a partial distributed metadata response
// deterministic, without relying on ReplicatedMergeTree queue timing.
type defaultPartialCluster struct {
	driver.Conn
	fired bool
}

func (p *defaultPartialCluster) Exec(ctx context.Context, q string, args ...any) error {
	if !p.fired && strings.Contains(q, " ON CLUSTER `analytics` MODIFY COLUMN") {
		p.fired = true
		return p.Conn.Exec(ctx, strings.Replace(q, " ON CLUSTER `analytics`", "", 1), args...)
	}
	return p.Conn.Exec(ctx, q, args...)
}
func TestAlterDefaultRejectsPartialClusterMetadata(t *testing.T) {
	if os.Getenv("MIGRES_REPLICATED") != "1" {
		t.Skip("requires two replica endpoints")
	}
	c := testConfig(t)
	defaultReplicaConfig(&c)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	name := fmt.Sprintf("default_partial_%d", time.Now().UnixNano())
	s, err := schema.FromCreate("CREATE TABLE `"+name+"`(id INT PRIMARY KEY,v BIGINT NOT NULL DEFAULT -10)", c.MySQL.Database, name, c)
	if err != nil {
		t.Fatal(err)
	}
	defaultPeers(t, ctx, c, func(peer *sink.ClickHouse) {
		cfg := c
		cfg.ClickHouse.Engine = "MergeTree"
		ddl, e := schema.Create(s, cfg)
		if e != nil {
			t.Fatal(e)
		}
		if e = peer.Conn.Exec(ctx, ddl); e != nil {
			t.Fatal(e)
		}
		if e = peer.Conn.Exec(ctx, "INSERT INTO "+schema.Ref(c.ClickHouse.Database, name)+" (id,v,_migres_version,_migres_deleted) VALUES(1,-10,1,0)"); e != nil {
			t.Fatal(e)
		}
	})
	defer defaultPeers(t, context.Background(), c, func(peer *sink.ClickHouse) {
		peer.Conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+schema.Ref(c.ClickHouse.Database, name)+" SYNC")
	})
	query := "ALTER TABLE `" + name + "` ALTER COLUMN v SET DEFAULT 6"
	next, _, err := schema.Apply(s, query, c)
	if err != nil {
		t.Fatal(err)
	}
	task := model.Task{Kind: "ddl", Table: name, Previous: &s, Schema: &next, SQL: query, Token: name + ":ddl"}
	raw := a.Sink.Conn
	partial := &defaultPartialCluster{Conn: raw}
	a.Sink.Conn = partial
	if err = a.Sink.Alter(ctx, task); err == nil || !strings.Contains(err.Error(), "DEFAULT expression incomplete") {
		t.Fatalf("partial metadata acknowledged: %v", err)
	}
	var completed bool
	if e := a.Store.Meta("alter:"+task.Token+":step:0", &completed); e == nil && completed {
		t.Fatal("partial DDL completion persisted")
	}
	a.Sink.Conn = raw
	if err = a.Sink.Alter(ctx, task); err != nil {
		t.Fatal(err)
	}
	defaultPeers(t, ctx, c, func(peer *sink.ClickHouse) {
		var typ, kind, expr string
		var value int64
		if e := peer.Conn.QueryRow(ctx, "SELECT type,default_kind,default_expression FROM system.columns WHERE database=currentDatabase() AND table=? AND name='v'", name).Scan(&typ, &kind, &expr); e != nil || typ != "Int64" || kind != "DEFAULT" || expr != "6" {
			t.Fatalf("partial replay metadata %s %s %s %v", typ, kind, expr, e)
		}
		if e := peer.Conn.QueryRow(ctx, "SELECT v FROM "+schema.Ref(c.ClickHouse.Database, name)+" WHERE id=1 LIMIT 1").Scan(&value); e != nil || value != -10 {
			t.Fatalf("partial replay value %d %v", value, e)
		}
	})
}
