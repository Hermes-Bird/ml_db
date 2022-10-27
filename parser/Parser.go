package parser

import (
	"errors"
	"github.com/Hermes-Bird/ml_db/json_handler"
	"log"
	"regexp"
	"strconv"
	"strings"
)

type Parser struct {
}

func (p *Parser) ParseCommand(text string) (cn string, command string, data []byte, err error) {

	collectionRegex := regexp.MustCompile("COLLECTION (.+)")

	strs := strings.Split(text, "\n")
	cInf := strs[0]

	res := collectionRegex.FindAllStringSubmatch(cInf, -1)
	if res == nil || len(res) != 1 {
		return "", "", nil, errors.New("Collection name part is invalid")
	}

	cn = res[0][1]

	commandInf := strs[1]
	commandRegex := regexp.MustCompile("(INSERT|SEARCH|UPDATE|DELETE)")
	res = commandRegex.FindAllStringSubmatch(commandInf, -1)
	if res == nil || len(res) != 1 {
		return "", "", nil, errors.New("Command name part is invalid")
	}

	command = res[0][1]

	dataRegex := regexp.MustCompile("(LIKE|WHERE) (\x7b.*\x7d|[[:word:]]+((\x2e[[:word:]]+)*=(\".*\"|[0-9])))")
	dataSlice := dataRegex.FindStringSubmatch(strs[2])
	if dataSlice == nil {
		log.Println("Data part is invalid")
		return "", "", nil, errors.New("Data part is invalid")
	}
	whereReg := regexp.MustCompile("[[:word:]]+(\x2e[[:word:]]+)*=(\".*\"|[0-9])")
	log.Println(dataSlice)
	if dataSlice[1] == "WHERE" {
		dt := dataSlice[2]
		if !whereReg.MatchString(dt) {
			log.Println("Invalid syntax u should use where with = expressions")
			return "", "", nil, errors.New("Data part is invalid")
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
			return "", "", nil, errors.New("Invalid json provided")
		}

		data = dJs
	}

	return cn, command, data, nil
}
