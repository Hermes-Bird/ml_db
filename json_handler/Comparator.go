package json_handler

import (
	"fmt"
	data "github.com/Hermes-Bird/ml_db/core/data_structures"
	"github.com/tidwall/gjson"
	"log"
)

type ComparableImpl struct {
	M map[string]gjson.Result
}

func NewComparator(base []byte) ComparableJSON {
	v := gjson.ParseBytes(base)
	m := map[string]gjson.Result{}
	s := data.NewStack()
	s.Push("")
	for {
		cur := s.Pop()
		if cur == nil {
			break
		}

		curPath, _ := cur.(string)

		var curV gjson.Result
		if curPath == "" {
			curV = v
		} else {
			curV = v.Get(curPath)
		}

		if curV.Type.String() != "JSON" {
			m[curPath] = curV
		} else {
			for key := range curV.Map() {
				if curPath != "" {
					s.Push(fmt.Sprintf("%s.%s", curPath, key))
				} else {
					s.Push(key)
				}
			}
		}
	}
	log.Println(m)
	return &ComparableImpl{M: m}
}

func (c ComparableImpl) Matches(data []byte) bool {
	curJson := gjson.ParseBytes(data)

	for path, val := range c.M {
		curV := curJson.Get(path)
		if curV.Exists() && curV.Str != val.Str {
			return false
		}
	}
	return true
}
