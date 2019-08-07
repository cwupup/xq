package storage

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/cwupup/xq/pkg/util"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/rawkv"
	"github.com/tikv/client-go/txnkv"
	"github.com/tikv/client-go/txnkv/kv"
)

var tikvTestDFS DFS
var tikvTestDFSOnce sync.Once

func getTestTikvDFS() DFS {
	tikvTestDFSOnce.Do(func() {
		rawCli, err := rawkv.NewClient(context.TODO(), []string{"10.103.133.19:2379"}, config.Default())
		if err != nil {
			return
		}
		txnCli, err := txnkv.NewClient(context.TODO(), []string{"10.103.133.19:2379"}, config.Default())
		if err != nil {
			return
		}
		tikvTestDFS = &TikvDFS{rawCli, txnCli}
	})

	return tikvTestDFS
}

func TestTikvLocalFile(t *testing.T) {
	dfs := getTestTikvDFS()
	assert.Equal(t, true, dfs != nil)

	keys := [][]byte{
		util.EncodeInt64Asc(nil, 1),
		util.EncodeInt64Asc(nil, 2),
		util.EncodeInt64Asc(nil, 3),
	}
	msgs := [][]byte{
		[]byte(`{"msg":1}`),
		[]byte(`{"msg":2}`),
		[]byte(`{"msg":3}`),
	}

	msgFileName := []byte("t_1")
	msgFile := dfs.LocalFile(msgFileName)
	// write
	for i := 0; i < len(keys); i++ {
		err := msgFile.Write(context.Background(), keys[i], msgs[i])
		assert.Equal(t, nil, err)
	}
	// read
	for i := 0; i < len(keys); i++ {
		msg, _ := msgFile.Read(context.Background(), keys[i])
		assert.Equal(t, string(msgs[i]), string(msg))
	}
	// delete
	for i := 0; i < len(keys); i++ {
		msgFile.Delete(context.Background(), keys[i])
		msg, _ := msgFile.Read(context.Background(), keys[i])
		assert.Equal(t, 0, len(msg))
	}
	// batch write
	err := msgFile.BatchWrite(context.Background(), keys, msgs)
	assert.Equal(t, nil, err)
	// batch read
	readRets, _ := msgFile.BatchRead(context.Background(), keys)
	for i := 0; i < len(msgs); i++ {
		assert.Equal(t, string(msgs[i]), string(readRets[i]))
	}
	// batch delete
	msgFile.BatchDelete(context.Background(), keys)
	readRets, _ = msgFile.BatchRead(context.Background(), keys)
	for i := 0; i < len(keys); i++ {
		assert.Equal(t, 0, len(readRets[i]))
	}
	// delete range
	msgFile.BatchWrite(context.Background(), keys, msgs)
	readRets, _ = msgFile.BatchRead(context.Background(), keys)
	for i := 0; i < len(msgs); i++ {
		assert.Equal(t, string(msgs[i]), string(readRets[i]))
	}
	msgFile.DeleteRange(context.Background(), keys[0], util.EncodeInt64Asc(nil, 4))
	readRets, _ = msgFile.BatchRead(context.Background(), keys)
	for i := 0; i < len(keys); i++ {
		assert.Equal(t, 0, len(readRets[i]))
	}

	// scan
	msgFile.BatchWrite(context.Background(), keys, msgs)
	retkeys, retVals, _ := msgFile.Scan(context.Background(), keys[0], keys[2], 3)
	assert.Equal(t, 2, len(retkeys))
	assert.Equal(t, 2, len(retVals))
	for i := 0; i < 2; i++ {
		assert.Equal(t, string(keys[i]), string(retkeys[i]))
		assert.Equal(t, string(msgs[i]), string(retVals[i]))
	}
	retkeys, retVals, _ = msgFile.Scan(context.Background(), keys[0], util.EncodeInt64Asc(nil, 4), 3)
	assert.Equal(t, 3, len(retkeys))
	assert.Equal(t, 3, len(retVals))

	// reverse scan
	retkeys, retVals, _ = msgFile.ReverseScan(context.Background(), util.EncodeInt64Asc(nil, 4), keys[0], 3)
	for i := 0; i < len(keys); i++ {
		assert.Equal(t, string(keys[len(keys)-i-1]), string(retkeys[i]))
		assert.Equal(t, string(msgs[len(keys)-i-1]), string(retVals[i]))
	}
}

func TestTikvDFSRemoteFile(t *testing.T) {
	dfs := getTestTikvDFS()
	offsetFileName := []byte("o_1")
	metaName := []byte("p_offset")
	msgFileName := []byte("m_2")
	msgTpl := `{msg:%d}`
	var txnKeys [][]byte
	var txnVals [][]byte
	for i := int64(1); i <= 10; i++ {
		k := util.EncodeInt64Asc(nil, i)
		v := fmt.Sprintf(msgTpl, i)
		txnKeys = append(txnKeys, k)
		txnVals = append(txnVals, []byte(v))
	}

	//init
	txn1, _ := dfs.Begin(context.Background())
	offsetFile := txn1.RemoteFile(offsetFileName)
	messageFile := txn1.RemoteFile(msgFileName)
	err := offsetFile.Delete(context.Background(), metaName)
	assert.Equal(t, nil, err)
	err = messageFile.DeleteRange(context.Background(), util.EncodeInt64Asc(nil, 0), util.EncodeInt64Asc(nil, 30))
	assert.Equal(t, nil, err)
	if err != nil {
		txn1.Rollback()
		return
	}
	txn1.Commit(context.Background())

	// txn write and read
	var wg sync.WaitGroup
	produceMsg := func() {
		var errTxn error
		txn, _ := dfs.Begin(context.Background())
		defer func() {
			if errTxn != nil {
				txn.Rollback()
			} else {
				txn.Commit(context.Background())
			}
			wg.Done()
		}()

		offsetFileTMP := txn.RemoteFile(offsetFileName)
		msgFileTMP := txn.RemoteFile(msgFileName)
		offsetBytes, errTxn := offsetFileTMP.Read(context.Background(), metaName)
		if errTxn != nil && !kv.IsErrNotFound(errTxn) {
			return
		}
		pOffset, _ := strconv.ParseInt(string(offsetBytes), 10, 64)
		pOffset++
		strOffset := strconv.Itoa(int(pOffset))
		errTxn = offsetFileTMP.Write(context.Background(), metaName, []byte(strOffset))
		if errTxn != nil {
			return
		}
		errTxn = msgFileTMP.Write(nil, util.EncodeInt64Asc(nil, pOffset), []byte(fmt.Sprintf(msgTpl, pOffset)))
	}

	for i := 0; i < 1; i++ {
		wg.Add(1)
		go produceMsg()
	}
	wg.Wait()

	// txn batch read
	txn2, _ := dfs.Begin(context.Background())
	offsetFile = txn2.RemoteFile(offsetFileName)
	messageFile = txn2.RemoteFile(msgFileName)
	txn2BatchRead, err := messageFile.BatchRead(context.Background(), txnKeys)
	assert.Equal(t, nil, err)
	if err != nil {
		txn2.Rollback()
		return
	}
	successProduce := 0
	assert.Equal(t, len(txnKeys), len(txn2BatchRead))
	for i, val := range txn2BatchRead {
		if val != nil {
			successProduce++
			assert.Equal(t, string(txnVals[i]), string(val))
		}
	}
	fmt.Println("****************success produce:", successProduce)
	// txn batch delete
	err = messageFile.BatchDelete(nil, txnKeys)
	assert.Equal(t, nil, err)
	if err != nil {
		txn2.Rollback()
		return
	}
	err = txn2.Commit(context.Background())
	assert.Equal(t, nil, err)

	// txn scan [start, nil)
	txn3, _ := dfs.Begin(context.Background())
	messageFile = txn3.RemoteFile(msgFileName)
	txn3ScanKeys, txn3ScanVals, err := messageFile.Scan(context.Background(), txnKeys[0], nil, 20)
	assert.Equal(t, nil, err)
	assert.Equal(t, 0, len(txn3ScanKeys))
	assert.Equal(t, 0, len(txn3ScanVals))
	// txn batch write
	err = messageFile.BatchWrite(nil, txnKeys, txnVals)
	assert.Equal(t, nil, err)
	if err != nil {
		txn3.Rollback()
		return
	}
	err = txn3.Commit(context.Background())
	assert.Equal(t, nil, err)

	// txn scan [stat, end)
	txn4, _ := dfs.Begin(context.Background())
	messageFile = txn4.RemoteFile(msgFileName)
	txn4ScanKeys, txn4ScanVals, _ := messageFile.Scan(context.Background(), txnKeys[0], txnKeys[9], 10)
	assert.Equal(t, 9, len(txn4ScanKeys))
	assert.Equal(t, 9, len(txn4ScanVals))
	txn4ScanKeys, txn4ScanVals, _ = messageFile.Scan(context.Background(), txnKeys[0], nil, 10)
	assert.Equal(t, 10, len(txn4ScanKeys))
	assert.Equal(t, 10, len(txn4ScanVals))
	for i := 0; i < 10; i++ {
		assert.Equal(t, string(txnKeys[i]), string(txn4ScanKeys[i]))
		assert.Equal(t, string(txnVals[i]), string(txn4ScanVals[i]))
	}

	//TODO:txn reverse scan [stat, end)
	//txn4ScanKeys, txn4ScanVals, _ = messageFile.ReverseScan(context.Background(), txnKeys[9], txnKeys[0], 10)
	//assert.Equal(t, 9, len(txn4ScanKeys))
	//assert.Equal(t, 9, len(txn4ScanVals))
	//txn4ScanKeys, txn4ScanVals, _ = messageFile.ReverseScan(context.Background(), txnKeys[9], nil, 10)
	//assert.Equal(t, 10, len(txn4ScanKeys))
	//assert.Equal(t, 10, len(txn4ScanVals))
	//for i := 0; i < 10; i++ {
	//	assert.Equal(t, string(txnKeys[9-i]), string(txn4ScanKeys[i]))
	//	assert.Equal(t, string(txnVals[9-i]), string(txn4ScanVals[i]))
	//}

	// txn delete range
	err = messageFile.DeleteRange(context.Background(), txnKeys[0], nil)
	assert.Equal(t, nil, err)
	if err != nil {
		txn4.Rollback()
		return
	}
	err = txn4.Commit(context.Background())
	assert.Equal(t, nil, err)

	txn5, _ := dfs.Begin(context.Background())
	messageFile = txn5.RemoteFile(msgFileName)
	txn5ScanKeys, txn5ScanVals, _ := messageFile.Scan(context.Background(), txnKeys[0], nil, 10)
	assert.Equal(t, 0, len(txn5ScanKeys))
	assert.Equal(t, 0, len(txn5ScanVals))
	txn5.Commit(context.Background())
}
