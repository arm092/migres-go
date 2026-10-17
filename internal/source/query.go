// Binlog queries keep the client's encoding, independently of table encodings.
package source

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/arm092/migres-go/internal/charset"
)

// QueryText reads the charset prefix of MySQL's ordered query status variables.
// Variables after Q_CHARSET_CODE are irrelevant to decoding the SQL bytes.
// https://dev.mysql.com/doc/dev/mysql-server/8.0.46/classbinary__log_1_1Query__event.html
func (m *MySQL) QueryText(ctx context.Context, raw, status []byte) (string, error) {
	ascii := true
	for _, b := range raw {
		if b >= 128 {
			ascii = false
			break
		}
	}
	if ascii {
		return string(raw), nil
	}
	id, err := queryCharset(status)
	if err != nil {
		return "", err
	}
	var name string
	if err := m.DB.QueryRowContext(ctx, "SELECT CHARACTER_SET_NAME FROM information_schema.COLLATIONS WHERE ID=?", id).Scan(&name); err != nil {
		return "", err
	}
	return charset.Decode(name, raw)
}
func queryCharset(status []byte) (uint16, error) {
	for len(status) > 0 {
		code := status[0]
		status = status[1:]
		size := 0
		switch code {
		case 0, 3:
			size = 4
		case 1:
			size = 8
		case 2:
			if len(status) < 1 {
				return 0, fmt.Errorf("truncated query status")
			}
			size = int(status[0]) + 2
		case 4:
			if len(status) < 6 {
				return 0, fmt.Errorf("truncated query charset")
			}
			return binary.LittleEndian.Uint16(status), nil
		default:
			return 0, fmt.Errorf("non-ASCII binlog query lacks a decodable client charset")
		}
		if len(status) < size {
			return 0, fmt.Errorf("truncated query status")
		}
		status = status[size:]
	}
	return 0, fmt.Errorf("non-ASCII binlog query lacks its client charset")
}
