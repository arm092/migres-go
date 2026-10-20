// Package config reads strict YAML and explicit environment overrides.
package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

type MySQL struct {
	Host        string              `yaml:"host"`
	Port        int                 `yaml:"port"`
	User        string              `yaml:"user"`
	Password    string              `yaml:"password"`
	Database    string              `yaml:"database"`
	Include     []string            `yaml:"include_tables"`
	Exclude     []string            `yaml:"exclude_tables"`
	SSLCA       string              `yaml:"ssl_ca"`
	SSLDisabled bool                `yaml:"ssl_disabled"`
	Keys        map[string][]string `yaml:"keys"`
}
type Ack struct {
	Mode    string        `yaml:"mode"`
	Quorum  string        `yaml:"quorum"`
	Timeout time.Duration `yaml:"timeout"`
}
type Replication struct {
	PathPrefix   string `yaml:"path_prefix"`
	ReplicaMacro string `yaml:"replica_macro"`
}
type ClickHouse struct {
	Addresses   []string    `yaml:"addresses"`
	Host        string      `yaml:"host"`
	Port        int         `yaml:"port"`
	User        string      `yaml:"user"`
	Password    string      `yaml:"password"`
	Database    string      `yaml:"database"`
	Secure      bool        `yaml:"secure"`
	Verify      bool        `yaml:"verify"`
	CACerts     string      `yaml:"ca_certs"`
	Engine      string      `yaml:"engine"`
	Cluster     string      `yaml:"cluster"`
	Replication Replication `yaml:"replication"`
	WriteAck    Ack         `yaml:"write_ack"`
}
type CDC struct {
	Heartbeat       time.Duration `yaml:"heartbeat"`
	ServerID        uint32        `yaml:"server_id"`
	UseGTID         bool          `yaml:"use_gtid"`
	SnapshotBefore  bool          `yaml:"snapshot_before"`
	ForcePosition   string        `yaml:"force_binlog_position"`
	DBDebug         bool          `yaml:"db_debug"`
	ProducerBatch   int           `yaml:"producer_batch_size"`
	ProducerFlush   time.Duration `yaml:"producer_flush_interval"`
	TransformerPoll time.Duration `yaml:"transformer_poll_interval"`
	ConsumerPoll    time.Duration `yaml:"consumer_poll_interval"`
	BatchMaxWait    time.Duration `yaml:"batch_max_wait"`
	RawMax          int           `yaml:"raw_events_max"`
	ResumeRatio     float64       `yaml:"raw_events_resume_ratio"`
	PreparedLimit   int           `yaml:"prepared_queries_batch_limit"`
	CheckpointRows  int           `yaml:"checkpoint_interval_rows"`
}
type Migration struct {
	Mode            string `yaml:"mode"`
	Debug           bool   `yaml:"debug"`
	BatchRows       int    `yaml:"batch_rows"`
	Workers         int    `yaml:"workers"`
	ConsumerWorkers int    `yaml:"consumer_workers"`
	LowCardinality  bool   `yaml:"low_cardinality_strings"`
	Timezone        string `yaml:"clickhouse_timezone"`
	SnapshotMode    string `yaml:"snapshot_mode"`
	SignalTable     string `yaml:"signal_table"`
	CDC             CDC    `yaml:"cdc"`
}
type Provider struct {
	Enabled    bool   `yaml:"enabled"`
	WebhookURL string `yaml:"webhook_url"`
	BotToken   string `yaml:"bot_token"`
	ChatID     string `yaml:"chat_id"`
}
type Notifications struct {
	Enabled   bool          `yaml:"enabled"`
	RateLimit time.Duration `yaml:"rate_limit"`
	Teams     Provider      `yaml:"teams"`
	Slack     Provider      `yaml:"slack"`
	Telegram  Provider      `yaml:"telegram"`
}
type Config struct {
	MySQL               MySQL         `yaml:"mysql"`
	ClickHouse          ClickHouse    `yaml:"clickhouse"`
	Migration           Migration     `yaml:"migration"`
	Notifications       Notifications `yaml:"notifications"`
	Environment         string        `yaml:"environment"`
	DataDir             string        `yaml:"data_dir"`
	RuntimeDir          string        `yaml:"runtime_dir"`
	Listen              string        `yaml:"listen"`
	ShutdownTimeout     time.Duration `yaml:"shutdown_timeout"`
	MaxQueueBytes       int64         `yaml:"max_queue_bytes"`
	MaxTransactionBytes int64         `yaml:"max_transaction_bytes"`
	MinFreeDiskBytes    int64         `yaml:"min_free_disk_bytes"`
	DebugRetention      time.Duration `yaml:"debug_retention"`
}

func Default() Config {
	return Config{MySQL: MySQL{Host: "127.0.0.1", Port: 3306, Keys: map[string][]string{}}, ClickHouse: ClickHouse{Host: "127.0.0.1", Port: 9000, User: "default", Verify: true, Engine: "ReplacingMergeTree", Replication: Replication{PathPrefix: "/migres/tables", ReplicaMacro: "replica"}, WriteAck: Ack{Mode: "quorum", Quorum: "auto", Timeout: 30 * time.Second}}, Migration: Migration{Mode: "cdc", BatchRows: 5000, Workers: 4, ConsumerWorkers: 4, Timezone: "UTC", SnapshotMode: "auto", SignalTable: "_migres_signal", CDC: CDC{Heartbeat: 5 * time.Second, ServerID: 4379, SnapshotBefore: true, ProducerBatch: 100, ProducerFlush: time.Second, TransformerPoll: 100 * time.Millisecond, ConsumerPoll: 100 * time.Millisecond, BatchMaxWait: time.Second, RawMax: 50000, ResumeRatio: .8, PreparedLimit: 100, CheckpointRows: 1000}}, Environment: "prod", DataDir: "data", RuntimeDir: "data/run", Listen: "127.0.0.1:8080", ShutdownTimeout: 60 * time.Second, MaxQueueBytes: 10 << 30, MaxTransactionBytes: 1 << 30, MinFreeDiskBytes: 1 << 30, DebugRetention: 24 * time.Hour, Notifications: Notifications{RateLimit: time.Minute}}
}
func Load(path string) (Config, error) {
	c := Default()
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	d := yaml.NewDecoder(f)
	d.KnownFields(true)
	if err = d.Decode(&c); err != nil {
		return c, err
	}
	if err = env(reflect.ValueOf(&c).Elem(), ""); err != nil {
		return c, err
	}
	return c, c.Validate()
}
func env(v reflect.Value, prefix string) error {
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		f := v.Field(i)
		name := strings.ToUpper(t.Field(i).Tag.Get("yaml"))
		if prefix != "" {
			name = prefix + "_" + name
		}
		name = strings.Replace(name, "MIGRATION_CDC_", "CDC_", 1)
		if f.Kind() == reflect.Struct {
			if err := env(f, name); err != nil {
				return err
			}
			continue
		}
		raw, ok := os.LookupEnv(name)
		if !ok {
			continue
		}
		var err error
		switch {
		case f.Type() == reflect.TypeOf(time.Duration(0)):
			var d time.Duration
			d, err = time.ParseDuration(raw)
			f.SetInt(int64(d))
		case f.Kind() == reflect.String:
			f.SetString(raw)
		case f.Kind() == reflect.Bool:
			var b bool
			b, err = strconv.ParseBool(raw)
			f.SetBool(b)
		case f.Kind() == reflect.Int || f.Kind() == reflect.Int64:
			var n int64
			n, err = strconv.ParseInt(raw, 10, 64)
			f.SetInt(n)
		case f.Kind() == reflect.Uint32:
			var n uint64
			n, err = strconv.ParseUint(raw, 10, 32)
			f.SetUint(n)
		case f.Kind() == reflect.Float64:
			var n float64
			n, err = strconv.ParseFloat(raw, 64)
			f.SetFloat(n)
		case f.Kind() == reflect.Slice:
			if raw == "" {
				f.Set(reflect.MakeSlice(f.Type(), 0, 0))
			} else {
				f.Set(reflect.ValueOf(strings.Split(raw, ",")))
			}
		default:
			err = fmt.Errorf("unsupported environment override")
		}
		if err != nil {
			return fmt.Errorf("invalid %s: %w", name, err)
		}
	}
	return nil
}
func (c Config) Validate() error {
	if c.MySQL.Database == "" || c.ClickHouse.Database == "" {
		return fmt.Errorf("mysql.database and clickhouse.database are required")
	}
	if c.MySQL.Port < 1 || c.MySQL.Port > 65535 || c.ClickHouse.Port < 1 || c.ClickHouse.Port > 65535 {
		return fmt.Errorf("invalid database port")
	}
	if c.Migration.Mode != "snapshot" && c.Migration.Mode != "cdc" {
		return fmt.Errorf("mode must be snapshot or cdc")
	}
	if c.Migration.Workers < 1 || c.Migration.ConsumerWorkers < 1 || c.Migration.BatchRows < 1 || c.Migration.BatchRows > 100000 {
		return fmt.Errorf("workers and batch_rows must be positive (batch_rows <= 100000)")
	}
	if c.ShutdownTimeout <= 0 || c.MaxQueueBytes <= 0 || c.Migration.CDC.ServerID == 0 || c.MaxTransactionBytes <= 0 || c.MinFreeDiskBytes < 0 {
		return fmt.Errorf("timeouts, queue limit and server_id must be positive")
	}
	if c.DebugRetention < time.Second || c.DebugRetention%time.Second != 0 {
		return fmt.Errorf("debug_retention must be a whole number of seconds (at least 1s)")
	}
	if c.Migration.CDC.TransformerPoll <= 0 || c.Migration.CDC.ConsumerPoll <= 0 || c.Migration.CDC.ProducerFlush <= 0 || c.Migration.CDC.Heartbeat <= 0 {
		return fmt.Errorf("stage intervals must be positive")
	}
	if c.Migration.CDC.RawMax < 1 || c.Migration.CDC.ResumeRatio <= 0 || c.Migration.CDC.ResumeRatio >= 1 {
		return fmt.Errorf("invalid backpressure settings")
	}
	if c.Migration.CDC.ProducerBatch < 1 || c.Migration.CDC.PreparedLimit < 1 || c.Migration.CDC.CheckpointRows < 1 || c.Migration.CDC.BatchMaxWait <= 0 {
		return fmt.Errorf("stage batch limits and batch_max_wait must be positive")
	}
	switch c.ClickHouse.Engine {
	case "ReplacingMergeTree", "ReplicatedReplacingMergeTree", "MergeTree":
	default:
		return fmt.Errorf("unsupported ClickHouse engine")
	}
	if c.ClickHouse.Engine == "ReplicatedReplacingMergeTree" && c.ClickHouse.Cluster == "" {
		return fmt.Errorf("replicated engine requires cluster")
	}
	if c.ClickHouse.WriteAck.Mode != "single" && c.ClickHouse.WriteAck.Mode != "quorum" {
		return fmt.Errorf("write_ack.mode must be single or quorum")
	}
	if c.ClickHouse.WriteAck.Quorum != "auto" {
		n, e := strconv.Atoi(c.ClickHouse.WriteAck.Quorum)
		if e != nil || n < 1 {
			return fmt.Errorf("quorum must be auto or positive integer")
		}
	}
	if _, err := time.LoadLocation(c.Migration.Timezone); err != nil {
		return fmt.Errorf("invalid timezone: %w", err)
	}
	switch c.Migration.SnapshotMode {
	case "auto", "gtid", "signal":
	default:
		return fmt.Errorf("snapshot_mode must be auto, gtid or signal")
	}
	return nil
}
func (c Config) Allowed(t string) bool {
	for _, x := range c.MySQL.Exclude {
		if x == t {
			return false
		}
	}
	if t == c.Migration.SignalTable {
		return false
	}
	if len(c.MySQL.Include) == 0 {
		return true
	}
	for _, x := range c.MySQL.Include {
		if x == t {
			return true
		}
	}
	return false
}
