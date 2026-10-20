// Package model contains the versioned, durable wire format between stages.
package model

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"
)

type Position struct {
	Timestamp int64  `json:"timestamp,omitempty"`
	File      string `json:"file"`
	Pos       uint32 `json:"pos"`
	GTID      string `json:"gtid,omitempty"`
}
type Value struct {
	Kind string `json:"kind"`
	Text string `json:"text,omitempty"`
}

func Encode(v any) Value {
	switch x := v.(type) {
	case string:
		if !utf8.ValidString(x) {
			return Value{Kind: "bytes", Text: base64.StdEncoding.EncodeToString([]byte(x))}
		}
		return Value{Kind: "string", Text: x}
	case nil:
		return Value{Kind: "null"}
	case []byte:
		return Value{Kind: "bytes", Text: base64.StdEncoding.EncodeToString(x)}
	case time.Time:
		return Value{Kind: "time", Text: x.Format("2006-01-02 15:04:05.999999")}
	case int64:
		return Value{Kind: "int", Text: strconv.FormatInt(x, 10)}
	case uint64:
		return Value{Kind: "uint", Text: strconv.FormatUint(x, 10)}
	case int:
		return Value{Kind: "int", Text: strconv.Itoa(x)}
	case int8, int16, int32:
		return Value{Kind: "int", Text: fmt.Sprint(x)}
	case uint, uint8, uint16, uint32:
		return Value{Kind: "uint", Text: fmt.Sprint(x)}
	case float32, float64:
		return Value{Kind: "float", Text: fmt.Sprint(x)}
	default:
		return Value{Kind: "string", Text: fmt.Sprint(x)}
	}
}
func (v Value) String() string {
	if v.Kind == "bytes" {
		b, _ := base64.StdEncoding.DecodeString(v.Text)
		return string(b)
	}
	return v.Text
}
func (v Value) SQL() any {
	if v.Kind == "null" {
		return nil
	}
	if v.Kind == "bytes" {
		b, _ := base64.StdEncoding.DecodeString(v.Text)
		return b
	}
	return v.Text
}

type Column struct {
	DefaultSQL        string   `json:"default_sql,omitempty"`
	DefaultExpression bool     `json:"default_expression,omitempty"`
	Charset           string   `json:"charset,omitempty"`
	Elements          []string `json:"elements,omitempty"`
	Name              string   `json:"name"`
	MySQLType         string   `json:"mysql_type"`
	CHType            string   `json:"ch_type"`
	Nullable          bool     `json:"nullable"`
	Default           *string  `json:"default,omitempty"`
	Generated         bool     `json:"generated,omitempty"`
}
type Schema struct {
	Charset    string   `json:"charset,omitempty"`
	Revision   string   `json:"revision"`
	Database   string   `json:"database"`
	Table      string   `json:"table"`
	Columns    []Column `json:"columns"`
	Keys       []string `json:"keys"`
	Generation string   `json:"generation"`
	SourceDDL  string   `json:"source_ddl"`
}
type Change struct {
	Before []Value `json:"before,omitempty"`
	After  []Value `json:"after,omitempty"`
}
type Event struct {
	Operation string   `json:"operation,omitempty"`
	Sequence  uint64   `json:"sequence"`
	SourceID  string   `json:"source_id,omitempty"`
	Kind      string   `json:"kind"`
	Table     string   `json:"table"`
	Schema    *Schema  `json:"schema,omitempty"`
	Previous  *Schema  `json:"previous,omitempty"`
	Rebuild   bool     `json:"rebuild"`
	Rows      []Change `json:"rows,omitempty"`
	Query     string   `json:"query,omitempty"`
	Position  Position `json:"position"`
}
type Row struct {
	Values  []Value `json:"values"`
	Version uint64  `json:"version"`
	Deleted bool    `json:"deleted"`
}
type Task struct {
	Payload   string   `json:"payload,omitempty"`
	Position  Position `json:"position"`
	ID        uint64   `json:"id"`
	Kind      string   `json:"kind"`
	Table     string   `json:"table"`
	Schema    *Schema  `json:"schema,omitempty"`
	Previous  *Schema  `json:"previous,omitempty"`
	Rebuild   bool     `json:"rebuild"`
	Rows      []Row    `json:"rows,omitempty"`
	SQL       string   `json:"sql,omitempty"`
	Token     string   `json:"token"`
	Operation string   `json:"operation,omitempty"`
}
type Progress struct {
	Table    string   `json:"table"`
	LastKey  []Value  `json:"last_key,omitempty"`
	Done     bool     `json:"done"`
	Boundary Position `json:"boundary"`
}
type Stats struct {
	StagedEvents     int      `json:"staged_events"`
	JournalBytes     int64    `json:"journal_bytes"`
	ReusableBytes    int64    `json:"reusable_bytes"`
	HistoryBytes     int64    `json:"history_bytes"`
	HistoryEntries   int      `json:"history_entries"`
	OldestAgeSeconds int64    `json:"oldest_age_seconds"`
	Events           int      `json:"events"`
	Tasks            int      `json:"tasks"`
	Bytes            int64    `json:"bytes"`
	Checkpoint       Position `json:"checkpoint"`
}
type Operation struct {
	ID             string   `json:"id"`
	Kind           string   `json:"kind"`
	State          string   `json:"state"`
	Error          string   `json:"error,omitempty"`
	Position       Position `json:"position"`
	DiscardPending bool     `json:"discard_pending"`
	AllowGap       bool     `json:"allow_gap"`
	Table          string   `json:"table,omitempty"`
}
