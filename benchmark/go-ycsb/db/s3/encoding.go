package s3

import (
	"bytes"
	"sync"

	"github.com/ugorji/go/codec"
)

// msgpack handle & pools
var (
	mh codec.MsgpackHandle

	bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}

	encPool = sync.Pool{New: func() any { return codec.NewEncoder(nil, &mh) }}
	decPool = sync.Pool{New: func() any { return codec.NewDecoderBytes(nil, &mh) }}
)

func encodeValues(values map[string][]byte) ([]byte, error) {
	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()

	enc := encPool.Get().(*codec.Encoder)
	enc.Reset(buf)

	if err := enc.Encode(values); err != nil {
		encPool.Put(enc)
		bufPool.Put(buf)
		return nil, err
	}

	encoded := make([]byte, buf.Len())
	copy(encoded, buf.Bytes())

	encPool.Put(enc)
	bufPool.Put(buf)
	return encoded, nil
}

func decodeValues(data []byte) (map[string][]byte, error) {
	dec := decPool.Get().(*codec.Decoder)
	dec.ResetBytes(data)

	var m map[string][]byte
	if err := dec.Decode(&m); err != nil {
		decPool.Put(dec)
		return nil, err
	}

	decPool.Put(dec)
	return m, nil
}
