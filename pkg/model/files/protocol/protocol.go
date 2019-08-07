package protocol

import (
	"github.com/cwupup/xq/pkg/model/files"
	"github.com/cwupup/xq/pkg/model/storage"
	"github.com/cwupup/xq/pkg/util"
)

type FileType int32

const (
	Heartbeat FileType = iota
	Controller
)

// GetHeartbeatFile returns a file in path:{clusterId}_{protoCategory}_{heartbeat}
func GetHeartbeatFile(dfs storage.DFS) storage.File {
	name := make([]byte, files.PrefixLen+4)
	pos := files.FillPrefix(name, files.Protocol)
	copy(name[pos:], util.EncodeInt32Asc(nil, int32(Heartbeat)))
	pos = pos + 4
	return dfs.LocalFile(name)
}

// GetControllerFile returns a file in path:{clusterId}_{protoCategory}_{controller}
func GetControllerFile(txn storage.Transaction) storage.File {
	name := make([]byte, files.PrefixLen+4)
	pos := files.FillPrefix(name, files.Protocol)
	copy(name[pos:], util.EncodeInt32Asc(nil, int32(Controller)))
	pos = pos + 4
	return txn.RemoteFile(name)
}
