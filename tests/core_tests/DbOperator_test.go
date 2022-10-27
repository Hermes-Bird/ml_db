package core_tests

import (
	"bytes"
	"github.com/Hermes-Bird/ml_db/core/consts"
	"github.com/Hermes-Bird/ml_db/core/db_operations"
	"testing"
)

func TestDBOperator(t *testing.T) {
	dbOp := db_operations.DbOperator{}
	f, _ := GetMockDummyDataFile()
	dd := GetDummyData()
	var pos int64
	t.Log("Test insert write")
	{
		prevPos, _ := f.Seek(0, 2)
		writePos, _ := dbOp.InsertWrite(f, dd)
		t.Log("Returned position should be equal to the position at the end of the file")
		if prevPos != writePos {
			t.Fatalf("--- Returned position not equal to last end of the file (got %d instead of %d)", writePos, prevPos)
		}

		rec := make([]byte, len(dd))
		f.ReadAt(rec, writePos)
		t.Log("Written record should be equal to read")
		if !bytes.Equal(rec, dd) {
			t.Fatalf("--- Records are not equal")
		}

		pos = writePos
	}

	t.Log("Test Delete")
	{
		cs, _ := dbOp.Delete(f, pos)
		h := make([]byte, consts.HEADER_SIZE)
		f.ReadAt(h, pos)
		_, _, size := db_operations.GetHeaderData(h)
		t.Log("Should save zero as actual size of record at header")
		if size != 0 {
			t.Fatalf("--- Actual size of record at header not equal to zero (%d)", size)
		}

		t.Log("Should return right size of cluster")
		if cs != uint32(len(dd)) {
			t.Fatalf("--- Returned wrong size of cluster (got %d but should be %d)", cs, len(dd))
		}
	}

}
