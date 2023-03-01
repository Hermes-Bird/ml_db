package files

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

type FileKeeperImp struct {
	M                   sync.Mutex
	FileWgStore         map[string]*sync.WaitGroup
	FileCancelChanStore map[string]chan struct{}
	FileStore           map[string]*os.File
}

func NewFileKeeperImpl() FileKeeper {
	return &FileKeeperImp{
		M:                   sync.Mutex{},
		FileWgStore:         map[string]*sync.WaitGroup{},
		FileCancelChanStore: map[string]chan struct{}{},
		FileStore:           map[string]*os.File{},
	}
}

func (fk *FileKeeperImp) GetFilename(cn string) string {
	return fmt.Sprintf("%s.mldf", cn)
}

func (fk *FileKeeperImp) GetFileByCollection(collection string) (*os.File, error) {
	log.Println("Mutex file before lock")
	fk.M.Lock()

	filename := fk.GetFilename(collection)

	f, ok := fk.FileStore[filename]
	if !ok {
		file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0755)
		if err != nil {
			fk.M.Unlock()
			return nil, err
		}
		fk.FileStore[filename] = file
		f = file
	}

	wg, ok := fk.FileWgStore[filename]
	if !ok {
		waitGr := &sync.WaitGroup{}
		fk.FileWgStore[filename] = waitGr
		wg = waitGr
	}

	wg.Add(1)

	c, ok := fk.FileCancelChanStore[filename]
	if !ok {
		ch := make(chan struct{})
		fk.FileCancelChanStore[filename] = ch
		go fk.FileKeep(filename, wg, ch)
		c = ch
	}

	fk.M.Unlock()
	log.Println("Mutex file unlock")
	select {
	case c <- struct{}{}:
	default:
	}

	return f, nil
}

func (fk *FileKeeperImp) DoneCollectionTask(collectionName string) {
	filename := fk.GetFilename(collectionName)
	fk.M.Lock()
	wg, ok := fk.FileWgStore[filename]
	if ok {
		wg.Done()
	}
	fk.M.Unlock()
}

func (fk *FileKeeperImp) FileKeep(filename string, wg *sync.WaitGroup, c chan struct{}) {
Loop:
	for {
		wg.Wait()

		t := time.NewTimer(time.Second * 15)
		select {
		case <-t.C:
			fk.M.Lock()
			f := fk.FileStore[filename]
			f.Close()
			delete(fk.FileStore, filename)
			fk.M.Unlock()
			break Loop
		case <-c:
		}
	}

}
