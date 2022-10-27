package files

import "os"

type FileKeeper interface {
	GetFileByCollection(collectionName string) (*os.File, error)
}
