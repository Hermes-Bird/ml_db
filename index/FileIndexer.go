package index

import "fmt"

type FileIndexerImpl struct {
	Fp map[string]string
}

func NewFileIndexer() *FileIndexerImpl {
	return &FileIndexerImpl{
		Fp: map[string]string{},
	}
}

func (fi FileIndexerImpl) GetFilePath(collectionName string) (string, bool) {
	path, ok := fi.Fp[collectionName]
	if ok {
		return path, true
	}

	newPath := fmt.Sprintf("/data/%s.mldf", collectionName)
	fi.Fp[collectionName] = newPath

	return newPath, false
}
