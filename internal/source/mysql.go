// Package source owns MySQL connections, metadata and bounded snapshot reads.
package source

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/arm092/migres-go/internal/charset"
	"github.com/arm092/migres-go/internal/config"
	"github.com/arm092/migres-go/internal/journal"
	"github.com/arm092/migres-go/internal/model"
	"github.com/arm092/migres-go/internal/schema"
	driver "github.com/go-sql-driver/mysql"
)

type MySQL struct {
	DatabaseCharset string
	DB              *sql.DB
	Config          config.Config
	TLS             *tls.Config
}

func Open(c config.Config) (*MySQL, error) {
	d := driver.NewConfig()
	d.Net = "tcp"
	d.Addr = net.JoinHostPort(c.MySQL.Host, strconv.Itoa(c.MySQL.Port))
	d.User = c.MySQL.User
	d.Passwd = c.MySQL.Password
	d.DBName = c.MySQL.Database
	d.Timeout = 10 * time.Second
	d.ReadTimeout = 30 * time.Second
	d.WriteTimeout = 30 * time.Second
	d.ParseTime = false
	d.InterpolateParams = false
	d.Params = map[string]string{"charset": "utf8mb4", "time_zone": "'+00:00'", "lock_wait_timeout": "10", "max_execution_time": "30000"}
	var tc *tls.Config
	if c.MySQL.SSLCA != "" && !c.MySQL.SSLDisabled {
		pem, err := os.ReadFile(c.MySQL.SSLCA)
		if err != nil {
			return nil, err
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("invalid MySQL CA")
		}
		tc = &tls.Config{RootCAs: pool, ServerName: c.MySQL.Host, MinVersion: tls.VersionTLS12}
		name := "migres-" + journal.ID()
		if err = driver.RegisterTLSConfig(name, tc); err != nil {
			return nil, err
		}
		d.TLSConfig = name
	}
	db, err := sql.Open("mysql", d.FormatDSN())
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(c.Migration.Workers + 4)
	db.SetMaxIdleConns(c.Migration.Workers + 2)
	db.SetConnMaxLifetime(5 * time.Minute)
	return &MySQL{DB: db, Config: c, TLS: tc}, nil
}
func (m *MySQL) Close() error { return m.DB.Close() }
func (m *MySQL) Check(ctx context.Context) error {
	var format, image string
	if err := m.DB.QueryRowContext(ctx, "SELECT @@global.binlog_format, @@global.binlog_row_image").Scan(&format, &image); err != nil {
		return err
	}
	if format != "ROW" || image != "FULL" {
		return fmt.Errorf("CDC requires binlog_format=ROW and binlog_row_image=FULL")
	}
	rows, err := m.DB.QueryContext(ctx, "SHOW GLOBAL VARIABLES WHERE Variable_name IN ('binlog_transaction_compression', 'binlog_row_value_options', 'gtid_mode')")
	if err != nil {
		return err
	}
	defer rows.Close()
	gtid := "OFF"
	for rows.Next() {
		var name, value string
		if err := rows.Scan(&name, &value); err != nil {
			return err
		}
		switch strings.ToLower(name) {
		case "binlog_transaction_compression":
			if strings.EqualFold(value, "ON") {
				return fmt.Errorf("binlog_transaction_compression must be OFF")
			}
		case "binlog_row_value_options":
			if value != "" {
				return fmt.Errorf("binlog_row_value_options must be empty; partial JSON is unsupported")
			}
		case "gtid_mode":
			gtid = value
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if m.Config.Migration.CDC.UseGTID && gtid != "ON" {
		return fmt.Errorf("use_gtid requires gtid_mode=ON")
	}
	return m.DB.QueryRowContext(ctx, "SELECT DEFAULT_CHARACTER_SET_NAME FROM information_schema.SCHEMATA WHERE SCHEMA_NAME=?", m.Config.MySQL.Database).Scan(&m.DatabaseCharset)
}
func (m *MySQL) Position(ctx context.Context) (model.Position, error) {
	rows, err := m.DB.QueryContext(ctx, "SHOW MASTER STATUS")
	if err != nil {
		rows, err = m.DB.QueryContext(ctx, "SHOW BINARY LOG STATUS")
	}
	if err != nil {
		return model.Position{}, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return model.Position{}, err
	}
	if !rows.Next() {
		return model.Position{}, fmt.Errorf("binary logging is unavailable")
	}
	v := make([]any, len(cols))
	dest := make([]any, len(cols))
	for i := range v {
		dest[i] = &v[i]
	}
	if err = rows.Scan(dest...); err != nil {
		return model.Position{}, err
	}
	p := model.Position{File: model.Encode(v[0]).String(), Timestamp: time.Now().Unix()}
	n, err := strconv.ParseUint(model.Encode(v[1]).String(), 10, 32)
	if err != nil {
		return p, err
	}
	p.Pos = uint32(n)
	if len(v) > 4 {
		p.GTID = model.Encode(v[4]).String()
	}
	return p, rows.Err()
}
func (m *MySQL) Tables(ctx context.Context) ([]string, error) {
	rows, err := m.DB.QueryContext(ctx, "SELECT TABLE_NAME, ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA=? AND TABLE_TYPE='BASE TABLE' ORDER BY TABLE_NAME", m.Config.MySQL.Database)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name, engine string
		if err = rows.Scan(&name, &engine); err != nil {
			return nil, err
		}
		if !m.Config.Allowed(name) {
			continue
		}
		if engine != "InnoDB" {
			return nil, fmt.Errorf("table %s uses %s; only InnoDB supports online snapshots", name, engine)
		}
		out = append(out, name)
	}
	return out, rows.Err()
}
func (m *MySQL) Schema(ctx context.Context, table string) (model.Schema, error) {
	s := model.Schema{Database: m.Config.MySQL.Database, Table: table, Generation: journal.ID()}
	rows, err := m.DB.QueryContext(ctx, "SELECT COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA, CHARACTER_SET_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? ORDER BY ORDINAL_POSITION", s.Database, table)
	if err != nil {
		return s, err
	}
	for rows.Next() {
		var col model.Column
		var nullable, extra string
		var def, sourceCharset sql.NullString
		if err = rows.Scan(&col.Name, &col.MySQLType, &nullable, &def, &extra, &sourceCharset); err != nil {
			rows.Close()
			return s, err
		}
		col.Charset = sourceCharset.String
		if col.Charset != "" && col.Charset != "binary" {
			if err := charset.Validate(col.Charset); err != nil {
				rows.Close()
				return s, err
			}
		}
		col.Nullable = nullable == "YES"
		col.Generated = strings.Contains(extra, "GENERATED")
		col.CHType, err = schema.Type(col.MySQLType, col.Nullable, m.Config.Migration.Timezone, m.Config.Migration.LowCardinality)
		if err != nil {
			rows.Close()
			return s, err
		}
		if def.Valid && !strings.Contains(strings.ToLower(def.String), "current_timestamp") {
			value := def.String
			col.Default = &value
		}
		s.Columns = append(s.Columns, col)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return s, err
	}
	if len(s.Columns) == 0 {
		return s, fmt.Errorf("table %s has no columns", table)
	}
	idx, err := m.DB.QueryContext(ctx, "SELECT INDEX_NAME, COLUMN_NAME, SUB_PART FROM information_schema.STATISTICS WHERE TABLE_SCHEMA=? AND TABLE_NAME=? AND NON_UNIQUE=0 ORDER BY INDEX_NAME, SEQ_IN_INDEX", s.Database, table)
	if err != nil {
		return s, err
	}
	indexes := map[string][]string{}
	valid := map[string]bool{}
	for idx.Next() {
		var name, col string
		var prefix sql.NullInt64
		if err = idx.Scan(&name, &col, &prefix); err != nil {
			idx.Close()
			return s, err
		}
		indexes[name] = append(indexes[name], col)
		if _, ok := valid[name]; !ok {
			valid[name] = true
		}
		if prefix.Valid {
			valid[name] = false
		}
	}
	err = idx.Err()
	idx.Close()
	if err != nil {
		return s, err
	}
	keys := indexes["PRIMARY"]
	if custom := m.Config.MySQL.Keys[table]; len(custom) > 0 {
		keys = custom
	}
	match := false
	for name, cols := range indexes {
		if !valid[name] || len(cols) != len(keys) {
			continue
		}
		same := true
		for i := range cols {
			if cols[i] != keys[i] {
				same = false
			}
		}
		if same {
			match = true
		}
	}
	if len(keys) == 0 || !match {
		return s, fmt.Errorf("table %s requires a full PRIMARY KEY or configured UNIQUE NOT NULL key", table)
	}
	for _, k := range keys {
		for _, col := range s.Columns {
			if col.Name == k && col.Nullable {
				return s, fmt.Errorf("key %s.%s is nullable", table, k)
			}
		}
	}
	s.Keys = keys
	var tableName string
	if err = m.DB.QueryRowContext(ctx, "SHOW CREATE TABLE "+schema.Ref(s.Database, table)).Scan(&tableName, &s.SourceDDL); err != nil {
		return s, err
	}
	parsed, e := schema.FromCreate(s.SourceDDL, s.Database, s.Generation, m.Config)
	if e != nil {
		return s, e
	}
	s.Charset = parsed.Charset
	for i := range s.Columns {
		for _, col := range parsed.Columns {
			if col.Name == s.Columns[i].Name {
				s.Columns[i].Elements = col.Elements
				s.Columns[i].DefaultExpression = col.DefaultExpression
				s.Columns[i].Default = col.Default
				s.Columns[i].DefaultSQL = col.DefaultSQL
			}
		}
	}
	return s, nil
}
func (m *MySQL) Chunk(ctx context.Context, s model.Schema, last []model.Value, limit int) ([][]model.Value, error) {
	cols := make([]string, len(s.Columns))
	for i, c := range s.Columns {
		cols[i] = schema.Quote(c.Name)
	}
	keys := make([]string, len(s.Keys))
	for i, k := range s.Keys {
		keys[i] = schema.Quote(k)
	}
	q := "SELECT " + strings.Join(cols, ",") + " FROM " + schema.Ref(s.Database, s.Table)
	var args []any
	if len(last) > 0 {
		if len(last) != len(keys) {
			return nil, fmt.Errorf("snapshot key shape changed")
		}
		marks := make([]string, len(keys))
		for i, v := range last {
			marks[i] = "?"
			args = append(args, v.SQL())
		}
		q += " WHERE (" + strings.Join(keys, ",") + ") > (" + strings.Join(marks, ",") + ")"
	}
	q += " ORDER BY " + strings.Join(keys, ",") + " LIMIT " + strconv.Itoa(limit)
	rows, err := m.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := [][]model.Value{}
	for rows.Next() {
		v := make([]any, len(cols))
		ptr := make([]any, len(cols))
		for i := range ptr {
			ptr[i] = &v[i]
		}
		if err = rows.Scan(ptr...); err != nil {
			return nil, err
		}
		row, e := NormalizeRow(s, v)
		if e != nil {
			return nil, e
		}
		out = append(out, row)
	}
	return out, rows.Err()
}
func Key(s model.Schema, row []model.Value) ([]model.Value, error) {
	var out []model.Value
	for _, key := range s.Keys {
		found := false
		for i, c := range s.Columns {
			if c.Name == key && i < len(row) {
				out = append(out, row[i])
				found = true
			}
		}
		if !found {
			return nil, fmt.Errorf("missing key %s", key)
		}
	}
	return out, nil
}
