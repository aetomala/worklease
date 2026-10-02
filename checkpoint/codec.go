package checkpoint

import (
	"encoding/json"
	"fmt"
)

// Codec encodes and decodes checkpoint state. All implementations must treat a
// nil data slice in Unmarshal as valid input — the generic Decode[T] helper
// enforces the nil-safe invariant before delegating to the codec. All methods
// are safe for concurrent use.
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
// wrapped with the "checkpoint: Encode:" prefix.
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
// A non-nil empty slice is not nil and is passed to the codec. On a codec error,
// Decode returns the zero value of T — never a partially decoded one — and the
// error wrapped with the "checkpoint: Decode:" prefix.
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
