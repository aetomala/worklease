package checkpoint

import "encoding/json"

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

// Encode encodes v using codec and returns the resulting bytes.
func Encode[T any](codec Codec, v T) ([]byte, error) {
	return codec.Marshal(v)
}

// Decode decodes data into a T using codec. Returns the zero value of T when
// data is nil, without error — this is the "no prior checkpoint" invariant:
// a first-time acquirer always receives a usable zero value regardless of codec.
func Decode[T any](codec Codec, data []byte) (T, error) {
	var zero T
	if data == nil {
		return zero, nil
	}
	if err := codec.Unmarshal(data, &zero); err != nil {
		return zero, err
	}
	return zero, nil
}
