package model

import (
	"encoding/json"
	"testing"
)

func TestInvalidUTF8Roundtrip(t *testing.T) {
	input := string([]byte{0, 255, 1})
	value := Encode(input)
	b, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var result Value
	if err = json.Unmarshal(b, &result); err != nil {
		t.Fatal(err)
	}
	if result.String() != input {
		t.Fatal("binary bytes changed across durable JSON")
	}
}
