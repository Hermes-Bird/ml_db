package db_operations

import (
	"encoding/binary"
	"io"
	"os"
)

type DbReader struct {
	CurPos   int64
	S        *os.File
	Readable bool
}

func NewDbReader(f *os.File) *DbReader {
	return &DbReader{
		CurPos:   0,
		S:        f,
		Readable: true,
	}
}

// ReadNext returns header + data byte slice, record position and possible error
func (r *DbReader) ReadNext() ([]byte, int64, error) {
	var h []byte
	for {
		hd, err := ReadHeader(r.S, r.CurPos)
		if err != nil {
			if err == io.EOF {
				r.Readable = false
			}
			return nil, 0, err
		}

		if binary.BigEndian.Uint32(hd[12:]) != 0 {
			h = hd
			break
		} else {
			r.CurPos += int64(binary.BigEndian.Uint32(hd[8:12]))
		}
	}

	sz := binary.BigEndian.Uint32(h[12:])
	res := make([]byte, sz)
	_, err := r.S.ReadAt(res, r.CurPos+16)
	if err != nil {
		if err == io.EOF {
			r.Readable = false
		} else {
			return nil, 0, err
		}
	}

	res = append(h, res...)

	cs := binary.BigEndian.Uint32(h[8:12])
	pos := r.CurPos
	r.CurPos += int64(cs)

	return res, pos, nil
}
