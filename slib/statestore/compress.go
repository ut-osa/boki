package statestore

import (
	"bytes"
	"io"

	"github.com/golang/snappy"
)

func compressData(uncompressed []byte) []byte {
	return snappy.Encode(nil, uncompressed)
}

func decompressReader(compressed []byte) (io.Reader, error) {
	uncompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(uncompressed), nil
}
