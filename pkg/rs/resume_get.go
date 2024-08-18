package rs

import (
	"io"
	"moss/pkg/objectstream"
)

type RSResumableGetStream struct {
	*decoder
}

func NewRSResumableGetStream(dataServers []string, uuids []string, size int64) (*RSResumableGetStream, error) {
	readers := make([]io.Reader, ALL_SHARDS)
	for i := range readers {
		var err error
		readers[i], err = objectstream.NewTempGetStream(dataServers[i], uuids[i])
		if err != nil {
			return nil, err
		}
	}
	writers := make([]io.Writer, ALL_SHARDS)
	dec := NewDecoder(readers, writers, size)
	return &RSResumableGetStream{dec}, nil
}
