package util

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/key"
)

func getBuffer(prefixKey []byte) key.Key {
	buf := make([]byte, 0, 64)
	buf = append(buf, prefixKey...)
	return buf
}

func TestInt16NumberCodec(t *testing.T) {
	prefix := []byte("test_")
	key1 := getBuffer(prefix)
	key2 := getBuffer(prefix)
	key3 := getBuffer(prefix)

	key1 = EncodeInt16Asc(key1, math.MinInt16)
	key2 = EncodeInt16Asc(key2, 0)
	key3 = EncodeInt16Asc(key3, math.MaxInt16)
	_, v1, _ := DecodeInt16Asc(key1[len(prefix):])
	_, v2, _ := DecodeInt16Asc(key2[len(prefix):])
	_, v3, _ := DecodeInt16Asc(key3[len(prefix):])
	ret12 := key1.Cmp(key2)
	ret23 := key2.Cmp(key3)

	assert.Equal(t, int16(math.MinInt16), v1)
	assert.Equal(t, int16(0), v2)
	assert.Equal(t, int16(math.MaxInt16), v3)
	assert.Equal(t, -1, ret12)
	assert.Equal(t, -1, ret23)
}

func TestInt32NumberCodec(t *testing.T) {
	prefix := []byte("test_")
	key1 := getBuffer(prefix)
	key2 := getBuffer(prefix)
	key3 := getBuffer(prefix)

	key1 = EncodeInt32Asc(key1, math.MinInt32)
	key2 = EncodeInt32Asc(key2, 0)
	key3 = EncodeInt32Asc(key3, math.MaxInt32)
	_, v1, _ := DecodeInt32Asc(key1[len(prefix):])
	_, v2, _ := DecodeInt32Asc(key2[len(prefix):])
	_, v3, _ := DecodeInt32Asc(key3[len(prefix):])
	ret12 := key1.Cmp(key2)
	ret23 := key2.Cmp(key3)

	assert.Equal(t, int32(math.MinInt32), v1)
	assert.Equal(t, int32(0), v2)
	assert.Equal(t, int32(math.MaxInt32), v3)
	assert.Equal(t, -1, ret12)
	assert.Equal(t, -1, ret23)
}

func TestInt64NumberCodec(t *testing.T) {
	prefix := []byte("test_")
	key1 := getBuffer(prefix)
	key2 := getBuffer(prefix)
	key3 := getBuffer(prefix)

	key1 = EncodeInt64Asc(key1, math.MinInt64)
	key2 = EncodeInt64Asc(key2, 0)
	key3 = EncodeInt64Asc(key3, math.MaxInt64)
	_, v1, _ := DecodeInt64Asc(key1[len(prefix):])
	_, v2, _ := DecodeInt64Asc(key2[len(prefix):])
	_, v3, _ := DecodeInt64Asc(key3[len(prefix):])
	ret12 := key1.Cmp(key2)
	ret23 := key2.Cmp(key3)

	assert.Equal(t, int64(math.MinInt64), v1)
	assert.Equal(t, int64(0), v2)
	assert.Equal(t, int64(math.MaxInt64), v3)
	assert.Equal(t, -1, ret12)
	assert.Equal(t, -1, ret23)
}

func TestUnitNumberCodec(t *testing.T) {
	prefix := []byte("test_")
	key1 := getBuffer(prefix)
	key2 := getBuffer(prefix)
	key3 := getBuffer(prefix)

	key1 = EncodeUintDesc(key1, math.MaxUint16)
	key2 = EncodeUintDesc(key2, math.MaxUint32)
	key3 = EncodeUintDesc(key3, math.MaxUint64)
	_, v1, _ := DecodeUintDesc(key1[len(prefix):])
	_, v2, _ := DecodeUintDesc(key2[len(prefix):])
	_, v3, _ := DecodeUintDesc(key3[len(prefix):])
	ret12 := key1.Cmp(key2)
	ret23 := key2.Cmp(key3)

	assert.Equal(t, uint64(math.MaxUint16), v1)
	assert.Equal(t, uint64(math.MaxUint32), v2)
	assert.Equal(t, uint64(math.MaxUint64), v3)
	assert.Equal(t, 1, ret12)
	assert.Equal(t, 1, ret23)
}
