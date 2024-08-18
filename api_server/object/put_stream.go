package object

import (
	"errors"
	"moss/api_server/heartbeat"
	"moss/pkg/rs"
)

func putStream(hash string, size int64) (*rs.RSPutStream, error) {
	servers := heartbeat.ChooseRandomDataServer(rs.ALL_SHARDS, nil)
	if len(servers) != rs.ALL_SHARDS {
		return nil, errors.New("cannot find enough data server")
	}

	return rs.NewRSPutStream(servers, hash, size)
}
