package broker

import (
	"context"
	"time"

	"github.com/cwupup/xq/pkg/model/files/meta"
	"github.com/cwupup/xq/pkg/model/files/protocol"
	"github.com/cwupup/xq/pkg/model/pb"
	"github.com/cwupup/xq/pkg/model/storage"
	"github.com/cwupup/xq/pkg/util"
	"github.com/golang/protobuf/proto"
)

type controller struct {
	id   int32
	term int32
	dfs  storage.DFS
}

func newController(id int32, term int32, dfs storage.DFS) *controller {
	return &controller{id: id, term: term, dfs: dfs}
}

func (c *controller) handleOnlineRequest() {

}

func (c *controller) getID() int32 {
	return c.id
}

func (c *controller) refreshBrokerMeta(metaInfo *pb.BrokerMeta) error {
	metaKey := util.EncodeInt32Asc(nil, metaInfo.GetId())
	metaValue, err := proto.Marshal(metaInfo)
	if err != nil {
		return err
	}

	ctx, cancle := context.WithTimeout(context.Background(), time.Second)
	defer cancle()

	txn, err := c.dfs.Begin(ctx)
	if err != nil {
		return err
	}
	metaFile := meta.GetBrokerMetaFile(txn)
	err = c.checkTerm(ctx, txn)
	if err != nil {
		return err
	}
	err = metaFile.Write(ctx, metaKey, metaValue)
	if err != nil {
		txn.Rollback()
		return err
	}
	return txn.Commit(ctx)
}

func (c *controller) checkTerm(ctx context.Context, txn storage.Transaction) error {
	ctrlFile := protocol.GetControllerFile(txn)
	keys := [][]byte{
		util.EncodeInt32Asc(nil, ControllerID),
		util.EncodeInt32Asc(nil, ControllerTerm),
	}
	values, err := ctrlFile.BatchRead(ctx, keys)
	if err != nil {
		return err
	}
	_, nowID, _ := util.DecodeInt32Asc(values[0])
	_, nowTerm, _ := util.DecodeInt32Asc(values[1])
	if c.getID() != nowID || c.term < nowTerm {
		return util.ErrControllerOutOfTerm
	}

	return nil
}
