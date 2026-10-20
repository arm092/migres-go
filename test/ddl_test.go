package test

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/arm092/migres-go/internal/app"
	"github.com/arm092/migres-go/internal/source"
)

func TestDDLAndHistoricalSchema(t *testing.T) {
	c := testConfig(t)
	base := fmt.Sprintf("ddl_%d", time.Now().UnixNano())
	other := base + "_new"
	renamed := base + "_renamed"
	c.MySQL.Include = []string{base, other}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	m, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	exec := func(q string) {
		t.Helper()
		if _, err := m.DB.ExecContext(ctx, q); err != nil {
			t.Fatal(err)
		}
	}
	exec("CREATE TABLE `" + base + "` (id BIGINT PRIMARY KEY, value VARCHAR(50))")
	defer func() { m.DB.Exec("DROP TABLE IF EXISTS `" + base + "`, `" + other + "`, `" + renamed + "`") }()
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	runCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- a.Run(runCtx) }()
	defer func() {
		stop()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	wait(t, ctx, func() bool { return a.Ready.Load() })
	exec("ALTER TABLE `" + base + "` ADD COLUMN extra INT NOT NULL DEFAULT 4")
	exec("INSERT INTO `" + base + "` (id,value) VALUES (1,'42')")
	exec("ALTER TABLE `" + base + "` CHANGE COLUMN value label VARCHAR(50)")
	exec("UPDATE `" + base + "` SET label='43' WHERE id=1")
	wait(t, ctx, func() bool {
		var v string
		var n int32
		err := a.Sink.Conn.QueryRow(ctx, "SELECT label,extra FROM `"+base+"` FINAL WHERE _migres_deleted=0").Scan(&v, &n)
		return err == nil && v == "43" && n == 4
	})
	exec("ALTER TABLE `" + base + "` DROP COLUMN extra")
	exec("CREATE TABLE `" + other + "` (id BIGINT PRIMARY KEY, label VARCHAR(20))")
	exec("INSERT INTO `" + other + "` VALUES(1,'created')")
	exec("ALTER TABLE `" + c.MySQL.Database + "`.`" + other + "` RENAME TO `" + renamed + "`")
	exec("UPDATE `" + renamed + "` SET label='renamed' WHERE id=1")
	wait(t, ctx, func() bool {
		var v string
		err := a.Sink.Conn.QueryRow(ctx, "SELECT label FROM `"+renamed+"` FINAL WHERE _migres_deleted=0").Scan(&v)
		return err == nil && v == "renamed"
	})
	exec("TRUNCATE TABLE `" + renamed + "`")
	wait(t, ctx, func() bool {
		var n uint64
		err := a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+renamed+"` FINAL").Scan(&n)
		return err == nil && n == 0
	})
}

func TestDefaultExpressionBackfill(t *testing.T) {
	c := testConfig(t)
	c.Migration.Timezone = "Asia/Yerevan"
	name := fmt.Sprintf("defexpr_%d", time.Now().UnixNano())
	c.MySQL.Include = []string{name}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	m, err := source.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if _, err = m.DB.ExecContext(ctx, "CREATE TABLE `"+name+"`(id INT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	if _, err = m.DB.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
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
	if _, err = m.DB.ExecContext(ctx, "ALTER TABLE `"+name+"` ADD COLUMN stamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP"); err != nil {
		t.Fatal(err)
	}
	var expected uint32
	if err = m.DB.QueryRowContext(ctx, "SELECT UNIX_TIMESTAMP(stamp) FROM `"+name+"` WHERE id=1").Scan(&expected); err != nil {
		t.Fatal(err)
	}
	wait(t, ctx, func() bool {
		var actual uint32
		err := a.Sink.Conn.QueryRow(ctx, "SELECT toUnixTimestamp(stamp) FROM `"+name+"` FINAL WHERE _migres_deleted=0 AND id=1").Scan(&actual)
		return err == nil && expected == actual
	})
}

// Mixed source DDL must follow selected tables without touching excluded target data.
func TestMixedTableDDLSelection(t *testing.T) {
	c := testConfig(t)
	base := fmt.Sprintf("mixed_ddl_%d", time.Now().UnixNano())
	selected, excluded := base+"_yes", base+"_no"
	moved, ignored := selected+"_moved", excluded+"_moved"
	c.MySQL.Include = []string{selected, excluded}
	c.MySQL.Exclude = []string{excluded, ignored}
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
	for _, name := range []string{selected, excluded} {
		exec("CREATE TABLE `" + name + "` (id INT PRIMARY KEY, value INT NOT NULL)")
		exec("INSERT INTO `" + name + "` VALUES (1,10)")
	}
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + selected + "`,`" + excluded + "`,`" + moved + "`,`" + ignored + "`")
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		owned, e := a.Store.Owned()
		if e != nil {
			t.Error(e)
		}
		for _, sc := range owned {
			if e := a.Sink.DropOwned(context.Background(), sc); e != nil {
				t.Error(e)
			}
		}
		a.Close()
	}()
	// An unrelated target table with the excluded source name is never owned.
	for _, name := range []string{excluded, ignored} {
		if err = a.Sink.Conn.Exec(ctx, "CREATE TABLE `"+name+"` (id Int32) ENGINE=MergeTree ORDER BY id"); err != nil {
			t.Fatal(err)
		}
		defer a.Sink.Conn.Exec(context.Background(), "DROP TABLE IF EXISTS `"+name+"` SYNC")
		if err = a.Sink.Conn.Exec(ctx, "INSERT INTO `"+name+"` VALUES (777)"); err != nil {
			t.Fatal(err)
		}
	}
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
	// Valid MySQL DDL unsupported by replication must not block excluded tables.
	exec("ALTER TABLE `" + excluded + "` ADD COLUMN extra INT NOT NULL DEFAULT 4, RENAME TO `" + ignored + "`")
	exec("ALTER TABLE `" + ignored + "` ALTER COLUMN value SET DEFAULT 7")
	exec("RENAME TABLE `" + ignored + "` TO `" + excluded + "`")
	exec("RENAME TABLE `" + excluded + "` TO `" + ignored + "`, `" + selected + "` TO `" + moved + "`")
	exec("UPDATE `" + moved + "` SET value=20 WHERE id=1")
	wait(t, ctx, func() bool {
		var v int32
		e := a.Sink.Conn.QueryRow(ctx, "SELECT value FROM `"+moved+"` FINAL WHERE _migres_deleted=0").Scan(&v)
		return e == nil && v == 20
	})
	exec("DROP TABLE `" + ignored + "`, `" + moved + "`")
	wait(t, ctx, func() bool {
		var n uint64
		e := a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=currentDatabase() AND name=?", moved).Scan(&n)
		return e == nil && n == 0
	})
	for _, name := range []string{excluded, ignored} {
		var id int32
		if e := a.Sink.Conn.QueryRow(ctx, "SELECT id FROM `"+name+"`").Scan(&id); e != nil || id != 777 {
			t.Fatalf("excluded target %s changed: id=%d err=%v", name, id, e)
		}
	}
}

func TestCompoundAlterCDC(t *testing.T) {
	c := testConfig(t)
	name := fmt.Sprintf("compound_ddl_%d", time.Now().UnixNano())
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
	exec("CREATE TABLE `" + name + "` (id INT PRIMARY KEY, old_value INT NOT NULL DEFAULT 1, obsolete INT NOT NULL, label VARCHAR(20) NOT NULL DEFAULT 'old')")
	defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`")
	exec("INSERT INTO `" + name + "` VALUES(1,7,9,'before')")
	a, err := app.Open(c)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		owned, e := a.Store.Owned()
		if e != nil {
			t.Error(e)
		}
		for _, sc := range owned {
			if e := a.Sink.DropOwned(context.Background(), sc); e != nil {
				t.Error(e)
			}
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
	rename := "RENAME COLUMN old_value TO value"
	if strings.HasPrefix(version, "5.7.") {
		rename = "CHANGE COLUMN old_value value INT NOT NULL DEFAULT 1"
	}
	exec("ALTER TABLE `" + name + "` " + rename + ", DROP COLUMN obsolete, CHANGE COLUMN label title VARCHAR(20) NOT NULL DEFAULT 'new', ADD COLUMN extra INT NOT NULL DEFAULT 4")
	exec("UPDATE `" + name + "` SET value=8 WHERE id=1")
	exec("INSERT INTO `" + name + "` (id) VALUES(2)")
	wait(t, ctx, func() bool {
		var count uint64
		e := a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+name+"` FINAL WHERE _migres_deleted=0 AND ((id=1 AND value=8 AND title='before' AND extra=4) OR (id=2 AND value=1 AND title='new' AND extra=4))").Scan(&count)
		return e == nil && count == 2
	})
	var obsolete uint64
	if err = a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM system.columns WHERE database=currentDatabase() AND table=? AND name IN ('obsolete','old_value','label')", name).Scan(&obsolete); err != nil || obsolete != 0 {
		t.Fatalf("old columns remain: %d %v", obsolete, err)
	}
}

func TestUnsupportedAlterStopsBeforeApplying(t *testing.T) {
	for _, tc := range []struct{ name, clause, expected string }{
		{"combined_table_rename", "ADD COLUMN extra INT NOT NULL DEFAULT 4, RENAME TO `%s`", "combined ALTER RENAME TABLE"},
		{"alter_default", "ALTER COLUMN value SET DEFAULT 7", "unsupported ALTER operation"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := testConfig(t)
			name := fmt.Sprintf("unsupported_ddl_%d", time.Now().UnixNano())
			renamed := name + "_renamed"
			c.MySQL.Include = []string{name}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			m, err := source.Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			if _, err = m.DB.ExecContext(ctx, "CREATE TABLE `"+name+"` (id INT PRIMARY KEY, value INT NOT NULL DEFAULT 1)"); err != nil {
				t.Fatal(err)
			}
			defer m.DB.Exec("DROP TABLE IF EXISTS `" + name + "`,`" + renamed + "`")
			if _, err = m.DB.ExecContext(ctx, "INSERT INTO `"+name+"` VALUES(1,3)"); err != nil {
				t.Fatal(err)
			}
			a, err := app.Open(c)
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				owned, e := a.Store.Owned()
				if e != nil {
					t.Error(e)
				}
				for _, sc := range owned {
					if e := a.Sink.DropOwned(context.Background(), sc); e != nil {
						t.Error(e)
					}
				}
				a.Close()
			}()
			run, stop := context.WithCancel(ctx)
			defer stop()
			done := make(chan error, 1)
			go func() { done <- a.Run(run) }()
			consumed := false
			defer func() {
				stop()
				if !consumed {
					<-done
				}
			}()
			wait(t, ctx, func() bool { return a.Ready.Load() })
			wait(t, ctx, func() bool {
				var n uint64
				e := a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM `"+name+"` FINAL WHERE id=1 AND value=3 AND _migres_deleted=0").Scan(&n)
				return e == nil && n == 1
			})
			before, err := a.Store.Schema(name)
			if err != nil {
				t.Fatal(err)
			}
			clause := tc.clause
			if strings.Contains(clause, "%s") {
				clause = fmt.Sprintf(clause, renamed)
			}
			if _, err = m.DB.ExecContext(ctx, "ALTER TABLE `"+name+"` "+clause); err != nil {
				t.Fatal(err)
			}
			select {
			case err = <-done:
				consumed = true
				if err == nil || !strings.Contains(err.Error(), tc.expected) {
					t.Fatalf("expected %q, got %v", tc.expected, err)
				}
			case <-ctx.Done():
				t.Fatal("unsupported DDL did not stop pipeline")
			}
			after, err := a.Store.Schema(name)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("unsupported DDL advanced historical schema")
			}
			var value int32
			if err = a.Sink.Conn.QueryRow(ctx, "SELECT value FROM `"+name+"` FINAL WHERE id=1 AND _migres_deleted=0").Scan(&value); err != nil || value != 3 {
				t.Fatalf("target data changed: %d %v", value, err)
			}
			var extras uint64
			if err = a.Sink.Conn.QueryRow(ctx, "SELECT count() FROM system.columns WHERE database=currentDatabase() AND table=? AND name='extra'", name).Scan(&extras); err != nil || extras != 0 {
				t.Fatalf("partial ALTER applied: %d %v", extras, err)
			}
		})
	}
}
