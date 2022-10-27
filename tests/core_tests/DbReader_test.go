package core_tests

import (
	"bytes"
	"encoding/binary"
	"github.com/Hermes-Bird/ml_db/core/db_operations"
	"io"
	"os"
	"testing"
)

func GetDummyData() []byte {
	data := `{"data": "data", "anotherData":"", "andAnotherData": ""}`
	h := make([]byte, 16)
	binary.BigEndian.PutUint32(h[8:12], uint32(len([]byte(data))+16))
	binary.BigEndian.PutUint32(h[12:], uint32(len([]byte(data))))

	return append(h, []byte(data)...)
}

func GetDummyDeletedData() []byte {
	wrongData := "wrong"
	wH := make([]byte, 16)
	binary.BigEndian.PutUint32(wH[8:12], uint32(len(wrongData)+16))

	return append(wH, []byte(wrongData)...)
}

func GetMockDummyDataFile() (*os.File, error) {
	f, err := os.Create("test_dt.dt")
	if err != nil {
		return nil, err
	}

	rec := GetDummyData()
	for i := 0; i < 15; i++ {
		f.Write(rec)
	}

	return f, nil
}

func GetMockDummyDeletedDataFile() (*os.File, error) {
	f, err := os.Create("test_dt.dt")
	if err != nil {
		return nil, err
	}

	rec := GetDummyData()
	dRec := GetDummyDeletedData()
	for i := 0; i < 15; i++ {
		f.Write(rec)
	}
	for i := 0; i < 15; i++ {
		f.Write(dRec)
	}
	for i := 0; i < 15; i++ {
		f.Write(rec)
	}
	for i := 0; i < 15; i++ {
		f.Write(dRec)
	}

	return f, nil
}

func TestDbReader(t *testing.T) {
	f, _ := GetMockDummyDataFile()
	r := db_operations.NewDbReader(f)
	t.Log("Test DbReader first read")
	{
		rec, pos, _ := r.ReadNext()

		t.Log("Position should be equal to 0")
		if pos != 0 {
			t.Errorf("--- Position of first read not equal to zero (%d)", pos)
		}

		t.Log("First read record should be equal to dummy one")
		if !bytes.Equal(rec, GetDummyData()) {
			t.Error("--- First read record not equal to dummy one")
		}
	}

	// reset test reader current position
	r.CurPos = 0

	t.Log("Test DbReader on non deleted data")
	{
		count := 0
		dd := GetDummyData()
		for r.Readable {
			data, _, err := r.ReadNext()
			if err != nil {
				if err == io.EOF {
					break
				}
			}
			if bytes.Equal(data, dd) {
				count++
			}
		}

		t.Log("15 records should be read on non deleted data")
		if count != 15 {
			t.Errorf("--- Amount of read record not equal to 15 (%d)", count)
		}
	}

	f, _ = GetMockDummyDeletedDataFile()
	r = db_operations.NewDbReader(f)
	t.Log("Test dbReader reading deleted and non deleted data")
	{
		count := 0
		dd := GetDummyData()

		var e error

		for r.Readable {
			data, _, err := r.ReadNext()
			if err == io.EOF {
				e = err
				break
			}

			if bytes.Equal(data, dd) {
				count++
			}
		}

		t.Log("Last error should be equal to EOF")
		if e != io.EOF {
			t.Errorf("--- Last error is not equal to EOF (%v)", e)
		}

		t.Log("Read count should be equal to 30")
		if count != 30 {
			t.Logf("--- Read count not equal to 30 (%d)", count)
		}
	}
}
