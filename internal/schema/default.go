package schema

import (
	"encoding/hex"
	"fmt"
	"github.com/arm092/migres-go/internal/charset"
	"github.com/arm092/migres-go/internal/model"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"math/big"
	"regexp"
	"strings"
)

func setDefault(col *model.Column, expr ast.ExprNode) error {
	raw, err := restore(expr)
	if err != nil {
		return err
	}
	col.DefaultSQL = raw
	if data, ok := binaryDefault(raw); ok && col.Charset != "" && col.Charset != "binary" {
		if _, err := charset.Decode(col.Charset, data); err != nil {
			return err
		}
	}
	if value, ok := expr.(ast.ValueExpr); ok {
		if value.GetValue() != nil {
			text := fmt.Sprint(value.GetValue())
			col.Default = &text
		}
	} else {
		col.DefaultExpression = true
	}
	return nil
}

var numericDefault = regexp.MustCompile(`^[+-]?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)
var timestampDefault = regexp.MustCompile(`^CURRENT_TIMESTAMP(?:\(([0-6])?\))?$`)

// Binary literals are byte values, not the diagnostic "0x..." string returned
// by fmt.Sprint on the MySQL parser's BinaryLiteral value.
func binaryDefault(raw string) ([]byte, bool) {
	lower := strings.ToLower(raw)
	if strings.HasPrefix(lower, "0x") {
		raw = "x'" + raw[2:] + "'"
		lower = strings.ToLower(raw)
	}
	if len(raw) < 3 || raw[1] != '\'' || raw[len(raw)-1] != '\'' {
		return nil, false
	}
	digits := raw[2 : len(raw)-1]
	switch lower[0] {
	case 'x':
		if len(digits)%2 != 0 {
			digits = "0" + digits
		}
		data, err := hex.DecodeString(digits)
		return data, err == nil
	case 'b':
		n := new(big.Int)
		if digits == "" {
			return []byte{}, true
		}
		if _, ok := n.SetString(digits, 2); !ok {
			return nil, false
		}
		data := make([]byte, (len(digits)+7)/8)
		return n.FillBytes(data), true
	}
	return nil, false
}

// Default translates only constants and MySQL's clock default. Other expressions
// are evaluated by MySQL and arrive as explicit values in row events.
func Default(col model.Column) string {
	raw := strings.TrimSpace(col.DefaultSQL)
	// Older journals stored expression defaults as a string plus this flag.
	// Preserve the safe clock mapping without turning arbitrary expressions into
	// string literals when the new optional raw-SQL field is absent.
	if raw == "" && col.DefaultExpression && col.Default != nil {
		raw = strings.TrimSpace(*col.Default)
	}
	if raw == "NULL" {
		return "NULL"
	}
	if data, ok := binaryDefault(raw); ok {
		if strings.Contains(col.CHType, "String") || strings.Contains(col.CHType, "Date") {
			if col.Charset != "" && col.Charset != "binary" {
				text, err := charset.Decode(col.Charset, data)
				if err != nil {
					return ""
				} // setDefault rejects this before persistence.
				data = []byte(text)
			}
			return "unhex(" + Literal(hex.EncodeToString(data)) + ")"
		}
		return new(big.Int).SetBytes(data).String()
	}
	if numericDefault.MatchString(raw) {
		return raw
	}
	if match := timestampDefault.FindStringSubmatch(strings.ToUpper(raw)); match != nil {
		precision := "0"
		if match[1] != "" {
			precision = match[1]
		}
		return "now64(" + precision + ")"
	}
	if col.DefaultExpression {
		return ""
	}
	if col.Default != nil {
		return Literal(*col.Default)
	}
	return ""
}
