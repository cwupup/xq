package broker

import (
	"context"
	"time"

	"github.com/cwupup/xq/pkg/model/files/protocol"
	"github.com/cwupup/xq/pkg/util"

	"github.com/tikv/client-go/txnkv/kv"
)

const (
	ControllerID int32 = iota
	ControllerTerm
)

type ControllerElector struct {
	broker *Broker
}

func newControllerElector(b *Broker) *ControllerElector {
	return &ControllerElector{b}
}

func (cs *ControllerElector) TryElect(curID, curTerm int32) (int32, int32, error) {
	broker := cs.broker
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	txn, err := broker.dfs.Begin(ctx)
	if err != nil {
		return -1, -1, err
	}
	file := protocol.GetControllerFile(txn)
	keys := [][]byte{
		util.EncodeInt32Asc(nil, ControllerID),
		util.EncodeInt32Asc(nil, ControllerTerm),
	}
	values, err := file.BatchRead(ctx, keys)
	if err != nil && !kv.IsErrNotFound(err) {
		return -1, -1, err
	}

	nextTerm := int32(-1)

	// if it's the first controller election in the cluster
	if kv.IsErrNotFound(err) {
		nextTerm = 1
	} else {
		_, controllerID, _ := util.DecodeInt32Asc(values[0])
		_, controllerTerm, _ := util.DecodeInt32Asc(values[1])
		if controllerID != curID || controllerTerm != curTerm {
			return controllerID, controllerTerm, util.ErrControllerSelectionConflict
		}
		nextTerm = controllerTerm + 1
	}

	values = [][]byte{
		util.EncodeInt32Asc(nil, broker.ID()),
		util.EncodeInt32Asc(nil, nextTerm),
	}
	err = file.BatchWrite(ctx, keys, values)
	if err != nil {
		txn.Rollback()
		return -1, -1, err
	}

	err = txn.Commit(ctx)
	if err != nil {
		return -1, -1, err
	}

	return broker.ID(), nextTerm, nil
}

func (cs *ControllerElector) CurrentController() (int32, int32, error) {
	broker := cs.broker
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	txn, err := broker.dfs.Begin(ctx)
	if err != nil {
		return -1, -1, err
	}
	file := protocol.GetControllerFile(txn)
	keys := [][]byte{
		util.EncodeInt32Asc(nil, ControllerID),
		util.EncodeInt32Asc(nil, ControllerTerm),
	}
	values, err := file.BatchRead(ctx, keys)
	if err != nil && !kv.IsErrNotFound(err) {
		return -1, -1, err
	}
	if kv.IsErrNotFound(err) {
		return -1, -1, nil
	}
	_, controllerID, _ := util.DecodeInt32Asc(values[0])
	_, controllerTerm, _ := util.DecodeInt32Asc(values[1])

	return controllerID, controllerTerm, nil
}
