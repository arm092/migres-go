// Package sink is the only package that writes target data.
package sink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
	"github.com/shopspring/decimal"
)

type ClickHouse struct {
	Conn      driver.Conn
	Config    config.Config
	Store     *journal.Store
	archiveMu sync.Mutex
}

func Open(c config.Config) (*ClickHouse, error) {
	addresses := c.ClickHouse.Addresses
	if len(addresses) == 0 {
		addresses = []string{net.JoinHostPort(c.ClickHouse.Host, strconv.Itoa(c.ClickHouse.Port))}
	}
	opts := &ch.Options{Addr: addresses, Auth: ch.Auth{Database: c.ClickHouse.Database, Username: c.ClickHouse.User, Password: c.ClickHouse.Password}, DialTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second, MaxOpenConns: c.Migration.ConsumerWorkers + 3, MaxIdleConns: c.Migration.ConsumerWorkers + 2, ConnOpenStrategy: ch.ConnOpenInOrder, Settings: ch.Settings{"mutations_sync": 2, "distributed_ddl_output_mode": "throw", "distributed_ddl_task_timeout": 30}}
	if c.ClickHouse.Secure {
		tc := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: !c.ClickHouse.Verify}
		if c.ClickHouse.CACerts != "" {
			pem, e := os.ReadFile(c.ClickHouse.CACerts)
			if e != nil {
				return nil, e
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(pem) {
				return nil, fmt.Errorf("invalid ClickHouse CA")
			}
			tc.RootCAs = pool
		}
		opts.TLS = tc
	}
	conn, err := ch.Open(opts)
	if err != nil {
		return nil, err
	}
	return &ClickHouse{Conn: conn, Config: c}, nil
}
func (c *ClickHouse) Close() error { return c.Conn.Close() }
func (c *ClickHouse) Check(ctx context.Context) error {
	if err := c.Conn.Ping(ctx); err != nil {
		return err
	}
	var engine string
	if err := c.Conn.QueryRow(ctx, "SELECT engine FROM system.databases WHERE name=?", c.Config.ClickHouse.Database).Scan(&engine); err != nil {
		return err
	}
	if engine != "Atomic" {
		return fmt.Errorf("target database must use Atomic, got %s", engine)
	}
	if c.Config.ClickHouse.Engine == "ReplicatedReplacingMergeTree" {
		var shards, replicas uint64
		if err := c.Conn.QueryRow(ctx, "SELECT uniqExact(shard_num), count() FROM system.clusters WHERE cluster=?", c.Config.ClickHouse.Cluster).Scan(&shards, &replicas); err != nil {
			return err
		}
		if shards != 1 || replicas == 0 {
			return fmt.Errorf("cluster must contain one shard")
		}
		if uint64(len(c.Config.ClickHouse.Addresses)) != replicas {
			return fmt.Errorf("replicated target requires addresses for all %d replicas", replicas)
		}
		seen := map[string]bool{}
		for _, address := range c.Config.ClickHouse.Addresses {
			cfg := c.Config
			cfg.ClickHouse.Addresses = []string{address}
			peer, err := Open(cfg)
			if err != nil {
				return err
			}
			var replica string
			err = peer.Conn.QueryRow(ctx, "SELECT substitution FROM system.macros WHERE macro=?", c.Config.ClickHouse.Replication.ReplicaMacro).Scan(&replica)
			peer.Close()
			if err != nil {
				var networkError net.Error
				if errors.As(err, &networkError) {
					continue
				}
				return err
			}
			if seen[replica] {
				return fmt.Errorf("addresses must identify distinct replicas")
			}
			seen[replica] = true
		}
		q := c.Config.ClickHouse.WriteAck.Quorum
		if q != "auto" {
			n, _ := strconv.Atoi(q)
			if uint64(n) > replicas {
				return fmt.Errorf("quorum exceeds replicas")
			}
		}
	}
	return nil
}
func (c *ClickHouse) Ensure(ctx context.Context, s model.Schema) error {
	var count uint64
	if err := c.Conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database=? AND name=?", c.Config.ClickHouse.Database, s.Table).Scan(&count); err != nil {
		return err
	}
	if count > 0 {
		var comment string
		if err := c.Conn.QueryRow(ctx, "SELECT comment FROM system.tables WHERE database=? AND name=?", c.Config.ClickHouse.Database, s.Table).Scan(&comment); err != nil {
			return err
		}
		if comment != "migres:"+s.Generation {
			return fmt.Errorf("target %s is not owned by generation %s", s.Table, s.Generation)
		}
	}
	q, err := schema.Create(s, c.Config)
	if err != nil {
		return err
	}
	if err = c.Conn.Exec(ctx, q); err != nil {
		return err
	}
	if err = c.verifyGeneration(ctx, s); err != nil {
		return err
	}
	if c.Store != nil {
		return c.Store.Own(s)
	}
	return nil
}
func Value(v model.Value, col model.Column) (any, error) {
	if v.Kind == "null" {
		if !col.Nullable {
			return nil, fmt.Errorf("NULL for non-nullable %s", col.Name)
		}
		return nil, nil
	}
	typ := col.CHType
	for {
		old := typ
		if strings.HasPrefix(typ, "Nullable(") || strings.HasPrefix(typ, "LowCardinality(") {
			typ = typ[strings.IndexByte(typ, '(')+1 : len(typ)-1]
		}
		if typ == old {
			break
		}
	}
	raw := v.String()
	switch {
	case strings.HasPrefix(typ, "UInt"):
		return strconv.ParseUint(raw, 10, 64)
	case strings.HasPrefix(typ, "Int"):
		return strconv.ParseInt(raw, 10, 64)
	case strings.HasPrefix(typ, "Float"):
		return strconv.ParseFloat(raw, 64)
	case strings.HasPrefix(typ, "Decimal"):
		return decimal.NewFromString(raw)
	case strings.HasPrefix(typ, "DateTime"):
		if strings.HasPrefix(raw, "0000-") {
			return nil, fmt.Errorf("zero datetime cannot be represented")
		}
		return time.ParseInLocation("2006-01-02 15:04:05.999999", strings.ReplaceAll(raw, "T", " "), time.UTC)
	case typ == "Date" || typ == "Date32":
		return time.Parse("2006-01-02", raw)
	default:
		return raw, nil
	}
}
func (c *ClickHouse) Execute(ctx context.Context, t model.Task) error {
	if t.Kind == "archive" {
		if len(t.Rows) != 1 {
			return fmt.Errorf("invalid archive task")
		}
		return c.archive(ctx, "events", t.Rows[0].Version, t.Payload)
	}
	if t.Kind == "fence" {
		if c.Store == nil {
			return fmt.Errorf("fence requires journal")
		}
		return c.Store.SetMeta("snapshot_done", true)
	}
	if t.Kind == "ddl" {
		return c.DDL(ctx, t)
	}
	if t.Kind != "insert" || t.Schema == nil {
		return fmt.Errorf("invalid target task")
	}
	settings := ch.Settings{"insert_deduplication_token": t.Token, "insert_deduplicate": 1}
	if c.Config.ClickHouse.Engine == "ReplicatedReplacingMergeTree" && c.Config.ClickHouse.WriteAck.Mode == "quorum" {
		q := c.Config.ClickHouse.WriteAck.Quorum
		if q == "auto" {
			settings["insert_quorum"] = "auto"
		} else {
			n, _ := strconv.Atoi(q)
			settings["insert_quorum"] = n
		}
		settings["insert_quorum_timeout"] = c.Config.ClickHouse.WriteAck.Timeout.Milliseconds()
		settings["insert_quorum_parallel"] = 0
	}
	ctx = ch.Context(ctx, ch.WithSettings(settings))
	columns := []string{}
	for _, col := range t.Schema.Columns {
		columns = append(columns, schema.Quote(col.Name))
	}
	columns = append(columns, "`_migres_version`", "`_migres_deleted`")
	batch, err := c.Conn.PrepareBatch(ctx, "INSERT INTO "+schema.Ref(c.Config.ClickHouse.Database, t.Table)+" ("+strings.Join(columns, ",")+")")
	if err != nil {
		return err
	}
	defer batch.Close()
	for _, row := range t.Rows {
		if len(row.Values) != len(t.Schema.Columns) {
			return fmt.Errorf("target row shape mismatch")
		}
		values := make([]any, 0, len(row.Values)+2)
		for i, v := range row.Values {
			x, e := Value(v, t.Schema.Columns[i])
			if e != nil {
				return fmt.Errorf("column %s: %w", t.Schema.Columns[i].Name, e)
			}
			values = append(values, x)
		}
		deleted := uint8(0)
		if row.Deleted {
			deleted = 1
		}
		values = append(values, row.Version, deleted)
		if err = batch.Append(values...); err != nil {
			return err
		}
	}
	return batch.Send()
}
func (c *ClickHouse) DDL(ctx context.Context, t model.Task) error {
	d, err := schema.Parse(t.SQL)
	if err != nil {
		return err
	}
	cluster := ""
	if c.Config.ClickHouse.Engine == "ReplicatedReplacingMergeTree" {
		cluster = " ON CLUSTER " + schema.Quote(c.Config.ClickHouse.Cluster)
	}
	switch d.Kind {
	case "create":
		if t.Schema == nil {
			return fmt.Errorf("CREATE lacks schema")
		}
		return c.Ensure(ctx, *t.Schema)
	case "drop":
		if c.Store == nil {
			return fmt.Errorf("DROP requires ownership journal")
		}
		owned, err := c.Store.Owned()
		if err != nil {
			return err
		}
		for _, table := range d.Tables {
			for _, sc := range owned {
				if sc.Table == table {
					if err = c.DropOwned(ctx, sc); err != nil {
						return err
					}
				}
			}
		}
		return nil
	case "truncate":
		return c.Conn.Exec(ctx, "TRUNCATE TABLE IF EXISTS "+schema.Ref(c.Config.ClickHouse.Database, t.Table)+cluster+" SYNC")
	case "rename":
		return c.RenameTask(ctx, t, d.Pairs, cluster)
	case "alter":
		return c.Alter(ctx, t)
	default:
		return fmt.Errorf("unsupported DDL")
	}
}
