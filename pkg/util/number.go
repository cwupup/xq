package util

import (
	"encoding/binary"
	"errors"

	"github.com/tikv/client-go/codec"
)

const (
	signMaskInt16 uint16 = 0x8000
	signMaskInt32 uint32 = 0x80000000
	signMaskInt64 uint64 = 0x8000000000000000
)

// EncodeInt16ToCmpUint makes int16 to comparable uint type
func EncodeInt16ToCmpUint(v int16) uint16 {
	return uint16(v) ^ signMaskInt16
}

// EncodeInt32ToCmpUint makes int32 to comparable uint type
func EncodeInt32ToCmpUint(v int32) uint32 {
	return uint32(v) ^ signMaskInt32
}

// EncodeInt64ToCmpUint makes int64  to comparable uint type
func EncodeInt64ToCmpUint(v int64) uint64 {
	return uint64(v) ^ signMaskInt64
}

// EncodeInt16Asc encodes int16 to bytes and appends to the given slice for ascending-order comparison
func EncodeInt16Asc(b []byte, v int16) []byte {
	var data [2]byte
	u := EncodeInt16ToCmpUint(v)
	binary.BigEndian.PutUint16(data[:], u)
	return append(b, data[:]...)
}

// EncodeInt32Asc encodes int32 to bytes and appends to the given slice for ascending-order comparison
func EncodeInt32Asc(b []byte, v int32) []byte {
	var data [4]byte
	u := EncodeInt32ToCmpUint(v)
	binary.BigEndian.PutUint32(data[:], u)
	return append(b, data[:]...)
}

// EncodeInt64Asc encodes int64 to bytes and appends to the given slice for ascending-order comparison
func EncodeInt64Asc(b []byte, v int64) []byte {
	var data [8]byte
	u := EncodeInt64ToCmpUint(v)
	binary.BigEndian.PutUint64(data[:], u)
	return append(b, data[:]...)
}

// DecodeInt16Asc decodes 2 bytes of b encoded by EncodeInt16Asc to int16 and returns the remaining bytes
func DecodeInt16Asc(b []byte) ([]byte, int16, error) {
	if len(b) < 2 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	data := b[:2]
	v := binary.BigEndian.Uint16(data)
	b = b[2:]
	return b, int16(v ^ signMaskInt16), nil
}

// DecodeInt32Asc decodes 4 bytes of b encoded by EncodeInt32Asc to int32 and returns the remaining bytes
func DecodeInt32Asc(b []byte) ([]byte, int32, error) {
	if len(b) < 4 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	data := b[:4]
	v := binary.BigEndian.Uint32(data)
	b = b[4:]
	return b, int32(v ^ signMaskInt32), nil
}

// DecodeInt64Asc decodes 8 bytes of b encoded by EncodeInt64Asc to int64 and returns the remaining bytes
func DecodeInt64Asc(b []byte) ([]byte, int64, error) {
	if len(b) < 8 {
		return nil, 0, errors.New("insufficient bytes to decode value")
	}

	data := b[:8]
	v := binary.BigEndian.Uint64(data)
	b = b[8:]
	return b, int64(v ^ signMaskInt64), nil
}

// EncodeUintDesc encodes uint for descending-order comparison
func EncodeUintDesc(b []byte, v uint64) []byte {
	return codec.EncodeUintDesc(b, v)
}

// DecodeUintDesc decodes 8 bytes of b encoded by EncodeUintDesc to uint64 and returns the remaining bytes
func DecodeUintDesc(b []byte) ([]byte, uint64, error) {
	return codec.DecodeUintDesc(b)
}
