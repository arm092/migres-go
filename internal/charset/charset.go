// Package charset converts source text bytes using the persisted MySQL schema.
package charset

import (
	"bytes"
	"fmt"
	"strings"
	"unicode/utf8"

	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/ianaindex"
	"golang.org/x/text/encoding/japanese"
	"golang.org/x/text/encoding/korean"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/encoding/unicode/utf32"
)

func codec(name string) (encoding.Encoding, error) {
	switch strings.ToLower(name) {
	case "utf8", "utf8mb3", "utf8mb4", "ascii":
		return nil, nil
	case "latin1":
		return charmap.Windows1252, nil // MySQL's latin1 is cp1252.
	case "latin2":
		return charmap.ISO8859_2, nil
	case "latin5":
		return charmap.ISO8859_9, nil
	case "latin7":
		return charmap.ISO8859_13, nil
	case "koi8r":
		return charmap.KOI8R, nil
	case "koi8u":
		return charmap.KOI8U, nil
	case "cp850":
		return charmap.CodePage850, nil
	case "cp852":
		return charmap.CodePage852, nil
	case "cp866":
		return charmap.CodePage866, nil
	case "greek":
		return charmap.ISO8859_7, nil
	case "hebrew":
		return charmap.ISO8859_8, nil
	case "tis620":
		return charmap.Windows874, nil
	case "macroman":
		return charmap.Macintosh, nil
	case "cp1250":
		return charmap.Windows1250, nil
	case "cp1251":
		return charmap.Windows1251, nil
	case "cp1256":
		return charmap.Windows1256, nil
	case "cp1257":
		return charmap.Windows1257, nil
	case "utf32":
		return utf32.UTF32(utf32.BigEndian, utf32.IgnoreBOM), nil
	case "sjis", "cp932":
		return japanese.ShiftJIS, nil
	case "ujis":
		return japanese.EUCJP, nil
	case "euckr":
		return korean.EUCKR, nil
	case "gb2312", "gbk":
		return simplifiedchinese.GBK, nil
	case "gb18030":
		return simplifiedchinese.GB18030, nil
	case "big5":
		return traditionalchinese.Big5, nil
	case "ucs2", "utf16":
		return unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM), nil
	case "utf16le":
		return unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM), nil
	}
	e, err := ianaindex.IANA.Encoding(name)
	if err != nil || e == nil {
		return nil, fmt.Errorf("unsupported MySQL charset %s", name)
	}
	return e, nil
}
func Validate(name string) error { _, err := codec(name); return err }
func Decode(name string, raw []byte) (string, error) {
	e, err := codec(name)
	if err != nil {
		return "", err
	}
	if e == nil {
		if !utf8.Valid(raw) {
			return "", fmt.Errorf("invalid %s source text", name)
		}
		if name == "ascii" {
			for _, b := range raw {
				if b > 127 {
					return "", fmt.Errorf("invalid ASCII source text")
				}
			}
		}
		return string(raw), nil
	}
	decoded, err := e.NewDecoder().Bytes(raw)
	if err != nil {
		return "", fmt.Errorf("invalid %s source text", name)
	}
	// Reject replacement or lossy decoding rather than creating a different key.
	restored, err := e.NewEncoder().Bytes(decoded)
	if err != nil || !bytes.Equal(restored, raw) {
		return "", fmt.Errorf("non-roundtrippable %s source text", name)
	}
	return string(decoded), nil
}
