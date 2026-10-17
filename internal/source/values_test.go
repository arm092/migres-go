package source

import (
	"github.com/arm092/migres-go/internal/model"
	"reflect"
	"testing"
)

func TestNormalizeTextCharsetKeepsSnapshotIdentity(t *testing.T) {
	s := model.Schema{Columns: []model.Column{{Name: "id", MySQLType: "varchar(20)", Charset: "latin1"}, {Name: "raw", MySQLType: "varbinary(20)"}}}
	snapshot, err := NormalizeRow(s, []any{[]byte("é€"), []byte{0xff}})
	if err != nil {
		t.Fatal(err)
	}
	cdc, err := NormalizeBinlogRow(s, []any{string([]byte{0xe9, 0x80}), []byte{0xff}})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(snapshot, cdc) {
		t.Fatalf("snapshot=%v CDC=%v", snapshot, cdc)
	}
	s.Columns[0].Charset = ""
	if _, err := NormalizeBinlogRow(s, []any{"x", nil}); err == nil {
		t.Fatal("missing historical charset accepted")
	}
}
