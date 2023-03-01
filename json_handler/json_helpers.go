package json_handler

import (
	"encoding/json"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

func GetJsonFromValue(v any, path string) []byte {
	resJ := []byte("{}")
	res, err := sjson.SetBytes(resJ, path, v)
	if err != nil {
		return nil
	}

	return res
}

func IsValidJson(j []byte) bool {
	return gjson.ValidBytes(j)
}

func IsValidJsonString(j string) bool {
	return IsValidJson([]byte(j))
}

func ToJsonBytes(d any) ([]byte, error) {
	return json.Marshal(d)
}
