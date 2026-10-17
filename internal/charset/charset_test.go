package charset

import "testing"

func TestSourceEncodings(t *testing.T) {
	for _, tc := range []struct {
		name string
		raw  []byte
		want string
	}{
		{"latin1", []byte{0xe9, 0x80}, "é€"},
		{"cp1251", []byte{0xcf, 0xf0, 0xe8, 0xe2, 0xe5, 0xf2}, "Привет"},
		{"utf16", []byte{0x05, 0x40, 0x05, 0x61, 0x05, 0x75}, "Հայ"},
		{"utf8mb4", []byte("Հայերեն"), "Հայերեն"},
	} {
		actual, err := Decode(tc.name, tc.raw)
		if err != nil || actual != tc.want {
			t.Fatalf("%s: %q %v", tc.name, actual, err)
		}
	}
	for _, name := range []string{"utf8mb4", "ascii"} {
		if _, err := Decode(name, []byte{0xff}); err == nil {
			t.Fatalf("invalid %s accepted", name)
		}
	}
	if err := Validate("unknown_charset"); err == nil {
		t.Fatal("unknown encoding accepted")
	}
}
