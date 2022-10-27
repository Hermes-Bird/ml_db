package consts

import "github.com/Hermes-Bird/ml_db/config"

const HEADER_SIZE = 16

const (
	STRAT_PATCH uint8 = iota
	SRTAT_UPDATE
)

func GetClusterSize(size uint32) uint32 {
	cSize := config.SSize
	if size > config.SSize*0.75 {
		cSize = config.MSize
	} else if size > config.MSize {
		cSize = config.LSize
	}

	return uint32(cSize)
}
