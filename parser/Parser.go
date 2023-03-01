package parser

import (
	"errors"
	"github.com/Hermes-Bird/ml_db/core/tx"
	"github.com/Hermes-Bird/ml_db/json_handler"
	"log"
	"regexp"
	"strconv"
	"strings"
)

type Parser struct {
}

var (
	collectionRegex = regexp.MustCompile("COLLECTION (.+)")
	commandRegex    = regexp.MustCompile("INSERT|SEARCH|UPDATE|DELETE")
	conditionRegex  = regexp.MustCompile("(LIKE|WHERE) (\x7b.*\x7d|[[:word:]]+((\x2e[[:word:]]+)*=(\".*\"|[0-9])))")
	whereRegex      = regexp.MustCompile("[[:word:]]+(\x2e[[:word:]]+)*=(\".*\"|[0-9])")
	dataRegex       = regexp.MustCompile("DATA (\x7b.*\x7d)")
)

// Parses first part of mlQL which contains COLLECTION <NAME>
// Returns a collection name
func parseCollectionStr(str string) (string, error) {
	res := collectionRegex.FindStringSubmatch(str)
	if res == nil || len(res) != 2 {
		return "", errors.New("collection name part is invalid")
	}
	return res[1], nil
}

// Parses command part of mlQL which contains name of the tx
// INSERT, SEARCH, UPDATE, DELETE or PATCH
// Returns a name of the command
func parseCommandPart(str string) (string, error) {
	res := commandRegex.FindStringSubmatch(str)
	if res == nil || len(res) != 1 {
		return "", errors.New("operation name part is invalid")
	}

	return res[0], nil
}

// Parses condition part of mlQL which contain WHERE or LIKE
// followed by JSON data in case of WHERE and json-field-path=data
// in case of LIKE
// Returns json byte slice of condition data
func parseConditionPart(str string) ([]byte, error) {
	dataSlice := conditionRegex.FindStringSubmatch(str)
	if dataSlice == nil {
		return nil, errors.New("condition part is invalid")
	}

	var data []byte
	if dataSlice[1] == "WHERE" {
		dt := dataSlice[2]
		if !whereRegex.MatchString(dt) {
			log.Println("Invalid syntax u should use WHERE with = expressions")
			return nil, errors.New("data part is invalid")
		}

		dtSlice := strings.Split(dt, "=")
		var value any
		if regexp.MustCompile("^[0-9]$").MatchString(dtSlice[1]) {
			v, _ := strconv.ParseInt(dtSlice[1], 10, 64)
			value = v
		} else {
			value = dtSlice[1]
		}
		data = json_handler.GetJsonFromValue(value, dtSlice[0])
	} else {
		dJs := []byte(dataSlice[2])
		if json_handler.IsValidJson(dJs) {
			return nil, errors.New("Invalid json provided")
		}

		data = dJs
	}

	return data, nil
}

// Parses data part of mlQL expression that has
// the following structure DATA <json-format-data>
// Returns slice of bytes of json data
func parseDataPart(str string) ([]byte, error) {
	res := dataRegex.FindStringSubmatch(str)
	if res == nil {
		return nil, errors.New("data part: Invalid data part")
	}

	if !json_handler.IsValidJsonString(res[1]) {
		return nil, errors.New("data part: invalid json provided")
	}

	return []byte(res[1]), nil
}

func (p *Parser) ParseExpression(text string) (*tx.Operation, error) {
	expr := p.PreParseProcessing(text)

	cmd := &tx.Operation{}

	cn, err := parseCollectionStr(expr[0])
	if err != nil {
		return nil, err
	}
	cmd.Collection = cn

	command, err := parseCommandPart(expr[1])
	if err != nil {
		return nil, err
	}
	cmd.Command = command

	if command == "INSERT" && len(expr) == 3 {
		data, err := parseDataPart(expr[2])
		if err != nil {
			return nil, err
		}
		cmd.Data = data
		log.Println("Insert part >>> ", cmd)
		return cmd, nil
	}

	cond, err := parseConditionPart(expr[2])
	if err != nil {
		return nil, err
	}
	cmd.Condition = cond

	if command == "UPDATE" && len(expr) == 4 {
		data, err := parseDataPart(expr[3])
		if err != nil {
			return nil, err
		}
		cmd.Data = data
	}

	return cmd, nil
}

func (p *Parser) PreParseProcessing(text string) []string {
	txt := strings.TrimSpace(text)
	strs := strings.Split(txt, "\n")
	resStrs := make([]string, 0)
	for _, str := range strs {
		if str != "" {
			resStrs = append(resStrs, strings.TrimSpace(str))
		}
	}

	return resStrs
}
