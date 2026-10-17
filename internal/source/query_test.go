package source

import "testing"

func TestQueryCharsetPrefix(t *testing.T) {
	status := []byte{0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 3, 1, 0, 1, 0, 4, 8, 0, 8, 0, 8, 0}
	id, err := queryCharset(status)
	if err != nil || id != 8 {
		t.Fatalf("%d %v", id, err)
	}
	for _, raw := range [][]byte{nil, {4}, {0, 1}, {12, 0}} {
		if _, err := queryCharset(raw); err == nil {
			t.Fatalf("incomplete charset accepted: %v", raw)
		}
	}
}
