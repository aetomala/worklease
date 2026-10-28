// Package checkpoint provides typed serialization helpers for worklease
// checkpoints: the Codec interface, a JSON codec, and the generic Encode and
// Decode functions.
package checkpoint

import (
	"encoding/json"
	"fmt"
)

// Codec encodes and decodes checkpoint state. The generic Decode[T] helper
// returns the zero value for nil data without calling the codec, so an
// implementation of Unmarshal need not accept a nil data slice. All methods are
// safe for concurrent use.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// JSONCodec implements Codec using encoding/json. All methods are safe for
// concurrent use.
type JSONCodec struct{}

// JSON returns a new JSONCodec.
func JSON() JSONCodec { return JSONCodec{} }

// Marshal encodes v to JSON bytes.
func (JSONCodec) Marshal(v any) ([]byte, error) { return json.Marshal(v) }

// Unmarshal decodes data into v.
func (JSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// Encode encodes v using codec and returns the resulting bytes. Codec errors are
// wrapped with the "checkpoint: Encode:" prefix. The codec must not be nil.
func Encode[T any](codec Codec, v T) ([]byte, error) {
	data, err := codec.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("checkpoint: Encode: %w", err)
	}
	return data, nil
}

// Decode decodes data into a T using codec. Returns the zero value of T when
// data is nil, without error — this is the "no prior checkpoint" invariant:
// a first-time acquirer always receives a usable zero value regardless of codec.
// A non-nil empty slice is not nil and is passed to the codec; the backends
// never return one as a State. On a codec error,
// Decode returns the zero value of T — never a partially decoded one — and the
// error wrapped with the "checkpoint: Decode:" prefix. The codec must not be nil
// when data is non-nil.
func Decode[T any](codec Codec, data []byte) (T, error) {
	var zero T
	if data == nil {
		return zero, nil
	}
	var v T
	if err := codec.Unmarshal(data, &v); err != nil {
		return zero, fmt.Errorf("checkpoint: Decode: %w", err)
	}
	return v, nil
}
