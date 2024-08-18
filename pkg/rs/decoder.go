package rs

import (
	"errors"
	"github.com/klauspost/reedsolomon"
	"io"
)

type decoder struct {
	readers   []io.Reader // include complete data shards
	writers   []io.Writer // include missing data shards
	enc       reedsolomon.Encoder
	size      int64  // data size
	total     int64  // the number of byte already read
	cache     []byte // to collect all shards data
	cacheSize int
}

func NewDecoder(readers []io.Reader, writers []io.Writer, size int64) *decoder {
	enc, _ := reedsolomon.New(DATA_SHARDS, PARIRY_SHARDS)
	return &decoder{readers, writers, enc, size, 0, nil, 0}
}

func (d *decoder) Read(p []byte) (n int, err error) {
	if d.cacheSize == 0 {
		err = d.getData()
		if err != nil {
			return 0, err
		}
	}
	length := len(p)
	if d.cacheSize < length {
		length = d.cacheSize
	}
	d.cacheSize -= length
	copy(p, d.cache[:length])
	d.cache = d.cache[length:]
	return length, nil
}

func (d *decoder) getData() error {
	if d.total == d.size {
		return io.EOF
	}

	shards := make([][]byte, ALL_SHARDS)
	repairIDs := make([]int, 0)
	for i := range shards {
		// data missing
		if d.readers[i] == nil {
			repairIDs = append(repairIDs, i)
		} else {
			shards[i] = make([]byte, BLOCK_PRE_SHARD)
			n, err := io.ReadFull(d.readers[i], shards[i])
			if err != nil && err != io.EOF && !errors.Is(err, io.ErrUnexpectedEOF) {
				shards[i] = nil
			} else if n != BLOCK_PRE_SHARD {
				shards[i] = shards[i][:n]
			}
		}
	}
	// repair missing data
	err := d.enc.Reconstruct(shards)
	if err != nil {
		return err
	}
	for i := range repairIDs {
		id := repairIDs[i]
		d.writers[id].Write(shards[i])
	}

	for i := 0; i < DATA_SHARDS; i++ {
		shardSize := int64(len(shards[i]))
		if d.total+shardSize > d.size {
			shardSize -= d.total + shardSize - d.size
		}
		d.cache = append(d.cache, shards[i][:shardSize]...)
		d.cacheSize += int(shardSize)
		d.total += shardSize
	}
	return nil
}
