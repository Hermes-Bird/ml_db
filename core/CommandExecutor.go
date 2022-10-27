package core

import (
	"encoding/binary"
	"github.com/Hermes-Bird/ml_db/core/consts"
	"github.com/Hermes-Bird/ml_db/core/db_operations"
	"github.com/Hermes-Bird/ml_db/files"
	"github.com/Hermes-Bird/ml_db/index"
	"github.com/Hermes-Bird/ml_db/json_handler"
	"github.com/martingallagher/go-jsonmp"
	"io"
	"log"
)

type CommandExecutor struct {
	Op  *db_operations.DbOperator
	Fk  files.FileKeeper
	Ind index.CollectionIndexer
}

func (c CommandExecutor) Search(cn string, data []byte) ([][]byte, error) {
	// TODO check for index & implement search with indexed field

	f, err := c.Fk.GetFileByCollection(cn)
	if err != nil {
		return nil, err
	}

	r := db_operations.NewDbReader(f)
	match := json_handler.NewComparator(data)
	res := make([][]byte, 0)
	for r.Readable {
		cur, _, err := r.ReadNext()
		if err != nil {
			log.Println("Search error", err)
		}

		curD := cur[16:]
		if match.Matches(curD) {
			res = append(res, curD)
		}
	}

	return res, nil
}

func (c CommandExecutor) Insert(cn string, data [][]byte) (int, error) {
	f, err := c.Fk.GetFileByCollection(cn)
	if err != nil {
		return 0, err
	}
	// TODO handle case with datasize more then config.LSize

	n := 0
	for i := range data {
		dBs, _ := db_operations.MakeHeader(c.Ind.GenerateNextId(cn), data[i])
		dBs = append(dBs, data[i]...)
		// TODO handle case
		pos, err := c.Op.InsertWrite(f, dBs)
		if err != nil {
			log.Println("Error while inserting data", err.Error())
		} else {
			c.Ind.SetPosForId(cn, binary.BigEndian.Uint64(dBs[:8]), pos)
		}
	}

	return n, nil
}

func (c CommandExecutor) Update(cn string, strat uint8, cond []byte, data []byte) (int, error) {
	f, err := c.Fk.GetFileByCollection(cn)
	if err != nil {
		return 0, err
	}

	// TODO index shit... again

	m := json_handler.NewComparator(cond)
	r := db_operations.NewDbReader(f)
	var dataSet map[int64][]byte
	for r.Readable {
		rec, pos, err := r.ReadNext()
		if err != nil {
			log.Println("Error while reading before update")
			continue
		}

		if m.Matches(rec[consts.HEADER_SIZE:]) {
			dataSet[pos] = rec
		}
	}

	count := 0
	for pos, d := range dataSet {
		var res []byte
		switch strat {
		case consts.STRAT_PATCH:
			res, err = jsonmp.Patch(d[consts.HEADER_SIZE:], data)
			if err != nil {
				log.Println("Error while patching data", err)
				continue
			}
		}
		id, cSize, _ := db_operations.GetHeaderData(d[consts.HEADER_SIZE:])
		h, newCSize := db_operations.MakeHeader(id, res)
		updRec := append(h, res...)
		if cSize != newCSize {
			cSize, err := c.Op.Delete(f, pos)
			if err != nil {
				log.Println("Error deleting updated record")
				continue
			}
			c.Ind.SetFreePos(cn, cSize, pos)

			newPos, err := c.Op.InsertWrite(f, updRec)
			if err != nil {
				continue
			}
			c.Ind.SetPosForId(cn, id, newPos)
			count++
		} else {
			err := c.Op.Write(f, pos, updRec)
			if err != nil {
				log.Println("Error while writing updated record")
			} else {
				count++
			}
		}
	}

	return 0, nil
}

func (c CommandExecutor) Delete(cn string, data []byte) (int, error) {
	f, err := c.Fk.GetFileByCollection(cn)
	if err != nil {
		return 0, err
	}
	// TODO indexed shit optimizations

	count := 0
	m := json_handler.NewComparator(data)
	r := db_operations.NewDbReader(f)
	for r.Readable {
		rec, pos, err := r.ReadNext()
		if err != nil {
			if err != io.EOF {
				log.Println(err)
			}
			continue
		}

		d := rec[consts.HEADER_SIZE:]
		if m.Matches(d) {
			size, err := c.Op.Delete(f, pos)
			if err != nil {
				log.Println("Delete error", err)
			}

			c.Ind.SetFreePos(cn, size, pos)
			count++
		}
	}

	return count, nil
}
