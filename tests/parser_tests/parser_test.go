package main

import (
	"github.com/Hermes-Bird/ml_db/parser"
	"testing"
)

func TestParser_Parse(t *testing.T) {
	p := parser.Parser{}
	command := `
	COLLECTION name
	INSERT
	DATA {"data": "data"}
`
	cmd, err := p.ParseExpression(command)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%#v", cmd)
}
