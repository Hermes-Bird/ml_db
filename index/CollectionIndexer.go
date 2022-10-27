package index

import (
	"fmt"
	data "github.com/Hermes-Bird/ml_db/core/data_structures"
)

type CollectionIndexerImpl struct {
	DelIndex map[string]*data.Queue
	Index    map[string]map[uint64]int64
	IdIndex  map[string]uint64
}

func (ci CollectionIndexerImpl) SetFreePos(cn string, size uint32, pos int64) {
	name := fmt.Sprintf("%s%d", cn, size)
	q, ok := ci.DelIndex[name]
	if !ok {
		q = data.NewQueue()
		q.Queue(pos)
		ci.DelIndex[name] = q
	} else {
		q.Queue(pos)
	}
}

func (ci CollectionIndexerImpl) GenerateNextId(cn string) uint64 {
	currId := ci.IdIndex[cn]
	ci.IdIndex[cn] += 1
	return currId
}

func (ci CollectionIndexerImpl) SetPosForId(cn string, id uint64, pos int64) {
	collIndex, ok := ci.Index[cn]
	if !ok {
		collIndex = map[uint64]int64{}
		collIndex[id] = pos
		ci.Index[cn] = collIndex
	} else {
		collIndex[id] = pos
	}
}

func (ci CollectionIndexerImpl) GetPosById(cn string, id uint64) (int64, bool) {
	cm, ok := ci.Index[cn]
	if !ok {
		return 0, false
	}

	pos, ok := cm[id]
	if !ok {
		return 0, false
	}

	return pos, true
}

func (ci CollectionIndexerImpl) GetFreePos(cn string, size uint32) (int64, bool) {
	/*
		TODO create check free pos function & think about solving problem with different cluster sizes & q
	*/
	s := fmt.Sprintf("%s%d", cn, size)

	q, ok := ci.DelIndex[s]
	if !ok {
		return 0, false
	}

	res := q.Unqueue()
	if res != nil {
		return 0, false
	}

	pos, ok := res.(int64)
	if !ok {
		return 0, false
	}

	return pos, true
}
