package index

type FileIndexer interface {
}

type CollectionIndexer interface {
	GenerateNextId(cn string) uint64
	GetFreePos(cn string, size uint32) (int64, bool)
	SetFreePos(cn string, size uint32, pos int64)
	SetPosForId(cn string, id uint64, pos int64)
}
