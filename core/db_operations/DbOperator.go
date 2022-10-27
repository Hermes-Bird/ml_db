package db_operations

import (
	"encoding/binary"
	"github.com/Hermes-Bird/ml_db/core/consts"
	"os"
)

type DbOperator struct {
}

// Delete returns size of cluster of deleted item
func (o DbOperator) Delete(f *os.File, pos int64) (uint32, error) {
	h, err := ReadHeader(f, pos)
	if err != nil {
		return 0, err
	}

	size := binary.BigEndian.Uint32(h[8:12])

	_, err = f.WriteAt(make([]byte, 4), pos+12)
	if err != nil {
		return 0, err
	}

	return size, nil
}

func (o DbOperator) Write(f *os.File, pos int64, data []byte) error {
	_, err := f.WriteAt(data, pos)
	return err
}

func (o DbOperator) InsertWrite(f *os.File, data []byte) (int64, error) {
	pos, err := f.Seek(0, 2)
	if err != nil {
		return 0, err
	}

	_, err = f.WriteAt(data, pos)
	if err != nil {
		return 0, err
	}

	return pos, nil
}

func (o *DbOperator) ReadOneRecordData(f *os.File, pos int64) ([]byte, error) {
	hBs, err := ReadHeader(f, pos)
	if err != nil {
		return nil, err
	}

	size := binary.BigEndian.Uint32(hBs[12:])

	d := make([]byte, size)
	_, err = f.ReadAt(d, pos+16)
	if err != nil {
		return nil, err
	}

	return d, nil
}

// Read structure [8]bytes id - [4] bytes of cluster size - [4] bytes of actual data size
func ReadHeader(f *os.File, pos int64) ([]byte, error) {
	hBs := make([]byte, 16)

	_, err := f.ReadAt(hBs, pos)
	if err != nil {
		return nil, err
	}

	return hBs, err
}

func GetHeaderData(h []byte) (uint64, uint32, uint32) {
	return binary.BigEndian.Uint64(h[:8]), binary.BigEndian.Uint32(h[8:12]), binary.BigEndian.Uint32(h[12:])
}

func MakeHeader(id uint64, data []byte) ([]byte, uint32) {
	hBs := make([]byte, 16)

	var idBs []byte
	binary.BigEndian.PutUint64(idBs, id)
	for i := range idBs {
		hBs[i] = idBs[i]
	}

	size := uint32(len(data))
	cSize := consts.GetClusterSize(size)

	var sizeBs []byte
	var cSizeBs []byte

	binary.BigEndian.PutUint32(sizeBs, size)
	binary.BigEndian.PutUint32(cSizeBs, cSize)

	for i := range cSizeBs {
		hBs[i+8] = cSizeBs[i]
	}

	for i := range sizeBs {
		hBs[i+12] = sizeBs[i]
	}

	return hBs, cSize
}
