package files

import (
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

func (fk *FileKeeperImp) GetFileByCollection(filename string, size int) (*os.File, *sync.WaitGroup, error) {
	fk.M.Lock()

	f, ok := fk.FileStore[filename]
	if !ok {
		file, err := os.OpenFile(filename, os.O_RDWR, 0666)
		if err != nil {
			return nil, nil, err
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

	select {
	case c <- struct{}{}:
	default:
	}

	return f, nil, nil
}

func (fk *FileKeeperImp) FileKeep(filename string, wg *sync.WaitGroup, c chan struct{}) {
Loop:
	for {
		wg.Wait()

		t := time.NewTimer(time.Second * 15)
		select {
		case <-t.C:
			f := fk.FileStore[filename]
			f.Close()
			delete(fk.FileStore, filename)
			break Loop
		case <-c:
		}
	}

}
