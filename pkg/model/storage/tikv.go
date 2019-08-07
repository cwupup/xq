package storage

import (
	"bytes"
	"context"
	"errors"

	"github.com/cwupup/xq/pkg/util"

	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/key"
	"github.com/tikv/client-go/rawkv"
	"github.com/tikv/client-go/txnkv"
)

type TikvDFS struct {
	rawClient *rawkv.Client
	txnClient *txnkv.Client
}

func NewTikvDFS(ctx context.Context, addrs []string, cfg interface{}) (DFS, error) {
	rawCli, err := rawkv.NewClient(ctx, addrs, config.Default())
	if err != nil {
		return nil, err
	}
	txnCli, err := txnkv.NewClient(ctx, addrs, config.Default())
	if err != nil {
		return nil, err
	}
	return &TikvDFS{rawCli, txnCli}, nil
}

func (t *TikvDFS) LocalFile(name []byte) File {
	return &TikvFile{name, t.rawClient}
}

func (t *TikvDFS) Begin(ctx context.Context) (Transaction, error) {
	txn, err := t.txnClient.Begin(ctx)
	if err != nil {
		return nil, err
	}
	return &TikvTxn{txn}, nil
}

type TikvTxn struct {
	txn *txnkv.Transaction
}

func (t *TikvTxn) RemoteFile(name []byte) File {
	return &TikvTxnFile{name, t}
}
func (t *TikvTxn) Commit(ctx context.Context) error {
	return t.txn.Commit(ctx)
}
func (t *TikvTxn) Rollback() error {
	return t.txn.Rollback()
}

type TikvFile struct {
	name   []byte
	client *rawkv.Client
}

func (t *TikvFile) Name() []byte {
	return t.name
}

func (t *TikvFile) Read(ctx context.Context, key []byte) ([]byte, error) {
	eKey := encodeKeyTikv(t.name, key)
	return t.client.Get(ctx, eKey)
}
func (t *TikvFile) Write(ctx context.Context, key []byte, value []byte) error {
	eKey := encodeKeyTikv(t.name, key)
	return t.client.Put(ctx, eKey, value)
}

func (t *TikvFile) Delete(ctx context.Context, key []byte) error {
	eKey := encodeKeyTikv(t.name, key)
	return t.client.Delete(ctx, eKey)
}

func (t *TikvFile) BatchRead(ctx context.Context, keys [][]byte) ([][]byte, error) {
	eKeys := batchEncodeKeysTikv(t.name, keys)
	return t.client.BatchGet(ctx, eKeys)
}
func (t *TikvFile) BatchWrite(ctx context.Context, keys [][]byte, values [][]byte) error {
	eKeys := batchEncodeKeysTikv(t.name, keys)
	return t.client.BatchPut(ctx, eKeys, values)
}

func (t *TikvFile) BatchDelete(ctx context.Context, keys [][]byte) error {
	eKeys := batchEncodeKeysTikv(t.name, keys)
	return t.client.BatchDelete(ctx, eKeys)
}

// DeleteRange deletes from [start, end)
func (t *TikvFile) DeleteRange(ctx context.Context, start []byte, end []byte) error {
	if len(end) == 0 {
		return errors.New("invalid upper bound")
	}
	var startKey, endKey []byte
	if len(start) == 0 {
		startKey = encodeKeyTikv(t.name, []byte{util.StrEncChar})
	} else {
		startKey = encodeKeyTikv(t.name, start)
	}
	if len(end) != 0 {
		endKey = encodeKeyTikv(t.name, end)
	}
	return t.client.DeleteRange(ctx, startKey, endKey)
}

// Scan reads from [start, end) by limit
func (t *TikvFile) Scan(ctx context.Context, start []byte, end []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	var startKey, endKey []byte
	var needCheck bool
	if len(start) == 0 {
		startKey = encodeKeyTikv(t.name, []byte{util.StrEncChar})
	} else {
		startKey = encodeKeyTikv(t.name, start)
	}
	if len(end) > 0 {
		endKey = encodeKeyTikv(t.name, end)
	} else {
		needCheck = true
	}

	retKeys, retValues, err := t.client.Scan(ctx, startKey, endKey, limit)
	if err != nil {
		return nil, nil, err
	}

	keys = batchDecodeKeysTikv(t.name, retKeys, needCheck)
	values = retValues
	return
}

func (t *TikvFile) ReverseScan(ctx context.Context, start []byte, end []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	var startKey, endKey []byte
	var needCheck bool
	if len(start) == 0 {
		startKey = encodeKeyTikv(t.name, []byte{util.StrEncChar})
	} else {
		startKey = encodeKeyTikv(t.name, start)
	}
	if len(end) > 0 {
		endKey = encodeKeyTikv(t.name, end)
	} else {
		needCheck = true
	}

	retKeys, retValues, err := t.client.ReverseScan(ctx, startKey, endKey, limit)
	if err != nil {
		return nil, nil, err
	}

	keys = batchDecodeKeysTikv(t.name, retKeys, needCheck)
	values = retValues
	return
}

type TikvTxnFile struct {
	name    []byte
	tikvtxn *TikvTxn
}

func (t *TikvTxnFile) transaction() *txnkv.Transaction {
	return t.tikvtxn.txn
}

func (t *TikvTxnFile) Name() []byte {
	return t.name
}

func (t *TikvTxnFile) Read(ctx context.Context, key []byte) ([]byte, error) {
	eKey := encodeKeyTikv(t.name, key)
	return t.transaction().Get(ctx, eKey)
}
func (t *TikvTxnFile) Write(_ context.Context, key []byte, value []byte) error {
	eKey := encodeKeyTikv(t.name, key)
	return t.transaction().Set(eKey, value)
}

func (t *TikvTxnFile) Delete(_ context.Context, key []byte) error {
	eKey := encodeKeyTikv(t.name, key)
	return t.transaction().Delete(eKey)
}

func (t *TikvTxnFile) BatchRead(ctx context.Context, keys [][]byte) ([][]byte, error) {
	pos := make(map[string]int)
	eKeys := make([]key.Key, len(keys))
	for i, k := range keys {
		eKeys[i] = key.Key(encodeKeyTikv(t.name, k))
		pos[string(eKeys[i])] = i
	}

	values, err := t.transaction().BatchGet(ctx, eKeys)
	if err != nil {
		return nil, err
	}
	ret := make([][]byte, len(keys))
	for k, v := range values {
		ret[pos[k]] = v
	}
	return ret, nil
}
func (t *TikvTxnFile) BatchWrite(_ context.Context, keys [][]byte, values [][]byte) error {
	for i, k := range keys {
		eKey := encodeKeyTikv(t.name, k)
		err := t.transaction().Set(eKey, values[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *TikvTxnFile) BatchDelete(_ context.Context, keys [][]byte) error {
	for _, k := range keys {
		eKey := encodeKeyTikv(t.name, k)
		err := t.transaction().Delete(eKey)
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteRange deletes value from [start, end)
func (t *TikvTxnFile) DeleteRange(ctx context.Context, start []byte, end []byte) error {
	var startKey, endKey []byte
	var needCheck bool
	if len(start) == 0 {
		startKey = encodeKeyTikv(t.name, []byte{util.StrEncChar})
	} else {
		startKey = encodeKeyTikv(t.name, start)
	}
	if len(end) == 0 {
		needCheck = true
	} else {
		endKey = encodeKeyTikv(t.name, end)
	}

	iter, err := t.transaction().Iter(ctx, startKey, endKey)
	if err != nil {
		return err
	}
	defer iter.Close()

	var keys [][]byte
	for iter.Valid() {
		if needCheck && decodeKeyTikv(t.name, iter.Key()[:], true) == nil {
			break
		}
		keys = append(keys, iter.Key()[:])
		iter.Next(context.TODO())
	}

	for _, key := range keys {
		err := t.transaction().Delete(key)
		if err != nil {
			return err
		}
	}

	return nil
}

// Scan reads value from [start, end) by limit
func (t *TikvTxnFile) Scan(ctx context.Context, start []byte, end []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	var startKey, endKey []byte
	var neeCheck bool
	if len(start) == 0 {
		startKey = encodeKeyTikv(t.name, []byte{util.StrEncChar})
	} else {
		startKey = encodeKeyTikv(t.name, start)
	}
	if len(end) > 0 {
		endKey = encodeKeyTikv(t.name, end)
	} else {
		neeCheck = true
	}

	iter, err := t.transaction().Iter(ctx, startKey, endKey)
	if err != nil {
		return nil, nil, err
	}
	defer iter.Close()

	for iter.Valid() && limit > 0 {
		dKey := decodeKeyTikv(t.name, iter.Key()[:], neeCheck)
		if dKey == nil {
			break
		}
		keys = append(keys, dKey)
		values = append(values, iter.Value()[:])
		limit--
		iter.Next(context.TODO())
	}
	return
}

// TODO:ReverseScan reads value from [start, end) by limit, start is greater than end in bytes compaction
func (t *TikvTxnFile) ReverseScan(ctx context.Context, start []byte, end []byte, limit int) (keys [][]byte, values [][]byte, err error) {
	return nil, nil, errors.New("IterReverse is not implemented")
	//if len(start) == 0 {
	//	return nil, nil, errors.New("reverse scan'start entry is nil")
	//}
	//var startKey, endKey []byte
	//startKey = encodeKeyTikv(t.name, start)
	//if len(end) == 0 {
	//	endKey = encodeKeyTikv(t.name, []byte{util.StrEncChar})
	//} else {
	//	endKey = encodeKeyTikv(t.name, end)
	//}
	//iter, err := t.transaction().IterReverse(ctx, startKey)
	//if err != nil {
	//	return nil, nil, err
	//}
	//defer iter.Close()
	//
	//for iter.Valid() && limit > 0 {
	//	if iter.Key().Cmp(endKey) < 0 {
	//		break
	//	}
	//	keys = append(keys, decodeKeyTikv(t.name, iter.Key()[:], false))
	//	values = append(values, iter.Value()[:])
	//	limit--
	//	iter.Next(context.TODO())
	//}
	//return
}

func encodeKeyTikv(fileName []byte, key []byte) []byte {
	k := make([]byte, len(fileName)+1+len(key))
	copy(k, fileName[:])
	k[len(fileName)] = '_'
	copy(k[len(fileName)+1:], key[:])
	return k
}

func decodeKeyTikv(fileName []byte, key []byte, needCheck bool) []byte {
	prefixLen := len(fileName) + 1
	if len(key) <= prefixLen {
		return nil
	}
	if needCheck {
		if key[len(fileName)] != '_' || bytes.Compare(key[:len(fileName)], fileName) != 0 {
			return nil
		}
	}
	return key[prefixLen:]
}

func batchEncodeKeysTikv(fileName []byte, keys [][]byte) [][]byte {
	encodeKeys := make([][]byte, len(keys))
	for i, k := range keys {
		encodeKeys[i] = encodeKeyTikv(fileName, k)
	}
	return encodeKeys
}

func batchDecodeKeysTikv(fileName []byte, keys [][]byte, needCheck bool) [][]byte {
	decodeKeys := make([][]byte, len(keys))
	for i, k := range keys {
		decodeKeys[i] = decodeKeyTikv(fileName, k, needCheck)
		if decodeKeys[i] == nil {
			return decodeKeys[:i]
		}
	}
	return decodeKeys
}
