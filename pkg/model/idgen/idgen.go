package idgen

import (
	"context"
	"sync"
	"time"

	"github.com/cwupup/xq/pkg/config"
	"github.com/cwupup/xq/pkg/model/files/idgen"
	"github.com/cwupup/xq/pkg/model/storage"
	"github.com/cwupup/xq/pkg/util"

	"github.com/tikv/client-go/txnkv/kv"
)

// IDGenerator generates continuous incremental ids greater than 0.
// IDGenerator is distributed, so it can be used safely across hosts.
type IDGenerator struct {
	dfs storage.DFS
}

var idGen *IDGenerator
var idGenOnce sync.Once

func InitIDGenerator(cfg *config.Config, dfs storage.DFS) {
	idGenOnce.Do(func() {
		idGen = &IDGenerator{
			dfs: dfs,
		}
	})
}

func Next(ftype idgen.FileType) (int64, error) {
	return idGen.Next(ftype)
}

func (i *IDGenerator) Next(ftype idgen.FileType) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	txn, err := i.dfs.Begin(ctx)
	if err != nil {
		return -1, err
	}
	file := idgen.GetIDGenFile(ftype, txn)
	data, err := file.Read(ctx, nil)
	if err != nil && !kv.IsErrNotFound(err) {
		return -1, err
	}

	var curID int64
	if len(data) > 0 {
		_, curID, err = util.DecodeInt64Asc(data)
		if err != nil {
			return -1, err
		}
	}
	nextId := curID + 1
	encodeID := util.EncodeInt64Asc(nil, nextId)
	err = file.Write(ctx, nil, encodeID)
	if err != nil {
		txn.Rollback()
		return -1, err
	}
	err = txn.Commit(ctx)
	if err != nil {
		return -1, err
	}
	return nextId, nil
}
