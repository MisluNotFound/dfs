package rs

import (
	"errors"
	"fmt"
	"io"
	"log"
	"moss/pkg/objectstream"
)

type RSGetStream struct {
	*decoder
}

func NewRSGetStream(locateInfo map[int]string, dataServers []string, hash string, size int64) (*RSGetStream, error) {
	if len(locateInfo)+len(dataServers) != ALL_SHARDS {
		return nil, errors.New("dataServers number mismatch")
	}

	readers := make([]io.Reader, ALL_SHARDS)
	// locateInfo includes empty value, size equals to ALL_SHARDS
	for i := 0; i < ALL_SHARDS; i++ {
		server := locateInfo[i]
		if server == "" {
			locateInfo[i] = dataServers[0]
			dataServers = dataServers[1:]
			continue
		}

		reader, err := objectstream.NewGetStream(server, fmt.Sprintf("%s.%d", hash, i))
		if err == nil {
			readers[i] = reader
		}
	}
	writers := make([]io.Writer, ALL_SHARDS)
	var preShard = (size + DATA_SHARDS - 1) / DATA_SHARDS
	var err error
	for i := range readers {
		if readers[i] == nil {
			writers[i], err = objectstream.NewTempPutStream(locateInfo[i], fmt.Sprintf("%s.%d", hash, i), preShard)
			if err != nil {
				return nil, err
			}
		}
	}
	dec := NewDecoder(readers, writers, size)
	return &RSGetStream{dec}, nil
}

func (s *RSGetStream) Close() {
	for i := range s.writers {
		if s.writers[i] != nil {
			s.writers[i].(*objectstream.TempPutStream).Commit(true)
		}
	}
}

// Seek read from s and begins at offset
func (s *RSGetStream) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekCurrent {
		log.Panic("only support SeekCurrent")
	}
	if offset < 0 {
		log.Panic("only support forward seek")
	}

	for offset > 0 {
		length := int64(BLOCK_SIZE)
		if offset < length {
			length = offset
		}
		buf := make([]byte, length)
		io.ReadFull(s, buf)
		offset -= length
	}
	return offset, nil
}
