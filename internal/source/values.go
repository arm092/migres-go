package source

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/arm092/migres-go/internal/charset"
	"github.com/arm092/migres-go/internal/model"
	"unicode/utf8"
)

func NormalizeRow(s model.Schema, values []any) ([]model.Value, error) {
	if len(values) != len(s.Columns) {
		return nil, fmt.Errorf("row shape mismatch")
	}
	out := make([]model.Value, len(values))
	for i, v := range values {
		col := s.Columns[i]
		typ := strings.ToLower(col.MySQLType)
		if v == nil {
			out[i] = model.Encode(nil)
			continue
		}
		if textColumn(col) && !strings.HasPrefix(typ, "enum(") && !strings.HasPrefix(typ, "set(") {
			if b, ok := v.([]byte); ok {
				v = string(b)
			}
			if text, ok := v.(string); ok && !utf8.ValidString(text) {
				return nil, fmt.Errorf("invalid UTF-8 snapshot text")
			}
		}
		encodedNumber := model.Encode(v)
		if strings.Contains(typ, "unsigned") && encodedNumber.Kind == "int" {
			n, err := strconv.ParseInt(encodedNumber.Text, 10, 64)
			if err != nil {
				return nil, err
			}
			if n < 0 {
				switch {
				case strings.HasPrefix(typ, "tinyint"):
					v = uint64(uint8(n))
				case strings.HasPrefix(typ, "smallint"):
					v = uint64(uint16(n))
				case strings.HasPrefix(typ, "mediumint"):
					v = uint64(uint32(n) & 0xffffff)
				case strings.HasPrefix(typ, "int(") || strings.HasPrefix(typ, "int "):
					v = uint64(uint32(n))
				default:
					v = uint64(n)
				}
			}
		}
		if strings.HasPrefix(typ, "enum(") || strings.HasPrefix(typ, "set(") {
			encoded := model.Encode(v)
			if encoded.Kind == "int" || encoded.Kind == "uint" {
				n, err := strconv.ParseUint(encoded.Text, 10, 64)
				if err != nil {
					return nil, err
				}
				if strings.HasPrefix(typ, "enum(") {
					if n > uint64(len(col.Elements)) {
						return nil, fmt.Errorf("ENUM index outside historical schema")
					}
					text := ""
					if n > 0 {
						text = col.Elements[n-1]
					}
					v = text
				} else {
					parts := []string{}
					for bit, text := range col.Elements {
						if n&(uint64(1)<<bit) != 0 {
							parts = append(parts, text)
						}
					}
					v = strings.Join(parts, ",")
				}
			}
		}
		if strings.HasPrefix(typ, "bit(") {
			if b, ok := v.([]byte); ok {
				if len(b) > 8 {
					return nil, fmt.Errorf("BIT wider than 64")
				}
				var buf [8]byte
				copy(buf[8-len(b):], b)
				v = binary.BigEndian.Uint64(buf[:])
			}
		}
		if typ == "json" {
			raw := model.Encode(v).String()
			var decoded any
			decoder := json.NewDecoder(strings.NewReader(raw))
			decoder.UseNumber()
			if err := decoder.Decode(&decoded); err != nil {
				return nil, fmt.Errorf("invalid JSON source value")
			}
			data, err := json.Marshal(decoded)
			if err != nil {
				return nil, err
			}
			v = string(data)
		}
		out[i] = model.Encode(v)
	}
	return out, nil
}

func textColumn(c model.Column) bool {
	t := strings.ToLower(c.MySQLType)
	return c.Charset != "binary" && (strings.HasPrefix(t, "char") || strings.HasPrefix(t, "varchar") || strings.Contains(t, "text"))
}

// Snapshot SELECTs are already transcoded by MySQL to utf8mb4; binlog strings
// retain the column encoding. Never transcode BINARY/VARBINARY/BLOB payloads.
func NormalizeBinlogRow(s model.Schema, values []any) ([]model.Value, error) {
	if len(values) != len(s.Columns) {
		return nil, fmt.Errorf("row shape mismatch")
	}
	normalized := append([]any(nil), values...)
	for i, col := range s.Columns {
		if values[i] == nil || !textColumn(col) {
			continue
		}
		if col.Charset == "" {
			return nil, fmt.Errorf("missing historical charset for %s", col.Name)
		}
		var raw []byte
		switch v := values[i].(type) {
		case string:
			raw = []byte(v)
		case []byte:
			raw = v
		default:
			return nil, fmt.Errorf("unexpected source text representation")
		}
		text, err := charset.Decode(col.Charset, raw)
		if err != nil {
			return nil, err
		}
		normalized[i] = text
	}
	return NormalizeRow(s, normalized)
}
