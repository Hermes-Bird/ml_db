package index

import data "github.com/Hermes-Bird/ml_db/core/data_structures"

type FileIndexer interface {
}

type CollectionIndexer interface {
	GenerateNextId(cn string) uint64
	GetFreePos(cn string, size uint32) (int64, bool)
	SetFreePos(cn string, size uint32, pos int64)
	SetPosForId(cn string, id uint64, pos int64)
}

func NewCollectionIndexer() CollectionIndexer {
	return &CollectionIndexerImpl{
		DelIndex: map[string]*data.Queue{},
		Index:    map[string]map[uint64]int64{},
		IdIndex:  map[string]uint64{},
	}
}
