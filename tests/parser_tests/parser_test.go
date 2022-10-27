package main

import (
	"github.com/Hermes-Bird/ml_db/parser"
	"strings"
	"testing"
)

func TestParser_Parse(t *testing.T) {
	p := parser.Parser{}
	command := `
COLLECTION COLLECTION_NAME
UPDATE
WHERE name="1"
`
	cn, command, ds, _ := p.ParseCommand(strings.TrimSpace(command))
	t.Logf("%#v %#v %s", cn, command, ds)
}
