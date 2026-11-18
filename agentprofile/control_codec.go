package agentprofile

import (
	"bytes"
	"reflect"

	"github.com/vkviyu/agent-temporal-engine/codec"
)

const controlDomain = "ATE-AGENT-CONTROL\x00"

// The binary codec walks only the statically declared closed command structs.
// Field order is schema order. Maps, interfaces, floats and callbacks are not
// encodable, and no runtime type supplied by a caller enters this path.
func controlFieldCount(t reflect.Type) int {
	return t.NumField()
}

func appendControlValue(b []byte, v reflect.Value, depth int) ([]byte, error) {
	if depth > 32 {
		return nil, ErrCommandShape
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < controlFieldCount(v.Type()); i++ {
			var err error
			b, err = appendControlValue(b, v.Field(i), depth+1)
			if err != nil {
				return nil, err
			}
		}
	case reflect.String:
		return codec.AppendString(b, v.String())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		b = codec.AppendUint(b, v.Uint())
	case reflect.Bool:
		x := uint64(0)
		if v.Bool() {
			x = 1
		}
		b = codec.AppendUint(b, x)
	case reflect.Pointer:
		// Only versioned, statically declared optional records are accepted.
		// Do not turn reflection into an open object serialization API.
		if !controlPointerAllowed(v.Type()) {
			return nil, ErrCommandShape
		}
		if v.IsNil() {
			return codec.AppendUint(b, 0), nil
		}
		return appendControlValue(codec.AppendUint(b, 1), v.Elem(), depth+1)
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return codec.AppendBytes(b, v.Bytes())
		}
		if v.Len() > MaxLifecycleRecords {
			return nil, ErrCommandShape
		}
		b = codec.AppendUint(b, uint64(v.Len()))
		for i := 0; i < v.Len(); i++ {
			var err error
			b, err = appendControlValue(b, v.Index(i), depth+1)
			if err != nil {
				return nil, err
			}
		}
	default:
		return nil, ErrCommandShape
	}
	if len(b) > MaxCommandBytes {
		return nil, ErrCommandShape
	}
	return b, nil
}

func readControlValue(d *codec.Decoder, v reflect.Value, depth int) error {
	if depth > 32 {
		return ErrCommandShape
	}
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < controlFieldCount(v.Type()); i++ {
			if err := readControlValue(d, v.Field(i), depth+1); err != nil {
				return err
			}
		}
	case reflect.String:
		s, err := d.String(MaxCommandBytes)
		if err != nil {
			return err
		}
		v.SetString(s)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := d.Uint()
		if err != nil || v.OverflowUint(n) {
			return ErrCommandShape
		}
		v.SetUint(n)
	case reflect.Bool:
		n, err := d.Uint()
		if err != nil || n > 1 {
			return ErrCommandShape
		}
		v.SetBool(n == 1)
	case reflect.Pointer:
		if !controlPointerAllowed(v.Type()) {
			return ErrCommandShape
		}
		present, err := d.Uint()
		if err != nil || present > 1 {
			return ErrCommandShape
		}
		if present == 0 {
			return nil
		}
		out := reflect.New(v.Type().Elem())
		if err := readControlValue(d, out.Elem(), depth+1); err != nil {
			return err
		}
		v.Set(out)
	case reflect.Slice:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			b, err := d.Bytes(MaxCommandBytes)
			if err != nil {
				return err
			}
			v.SetBytes(bytes.Clone(b))
			return nil
		}
		n, err := d.Uint()
		if err != nil || n > MaxLifecycleRecords || n > uint64(d.Remaining()) {
			return ErrCommandShape
		}
		out := reflect.MakeSlice(v.Type(), int(n), int(n))
		for i := 0; i < int(n); i++ {
			if err = readControlValue(d, out.Index(i), depth+1); err != nil {
				return err
			}
		}
		v.Set(out)
	default:
		return ErrCommandShape
	}
	return nil
}

func controlOperation(c ControlCommand) (uint64, reflect.Value, error) {
	v := reflect.ValueOf(c)
	var op uint64
	var selected reflect.Value
	for i := 0; i < v.NumField(); i++ {
		if !v.Field(i).IsNil() {
			if op != 0 {
				return 0, reflect.Value{}, ErrCommandShape
			}
			op = uint64(i + 1)
			selected = v.Field(i).Elem()
		}
	}
	if op == 0 {
		return 0, reflect.Value{}, ErrCommandShape
	}
	return op, selected, nil
}

func encodeControlCommand(c ControlCommand) ([]byte, error) {
	op, v, err := controlOperation(c)
	if err != nil {
		return nil, err
	}
	if err = validateControlShape(c); err != nil {
		return nil, err
	}
	b := codec.AppendUint([]byte(controlDomain), c.SchemaVersion())
	b = codec.AppendUint(b, op)
	b, err = appendControlValue(b, v, 0)
	if err != nil || len(b) > MaxCommandBytes {
		return nil, ErrCommandShape
	}
	return b, nil
}

// decodeControlCommand decodes only the current V1 control schema.
func decodeControlCommand(raw []byte) (*ControlCommand, error) {
	if len(raw) > MaxCommandBytes || !bytes.HasPrefix(raw, []byte(controlDomain)) {
		return nil, ErrCommandShape
	}
	d := codec.NewDecoder(raw[len(controlDomain):])
	version, err := d.Uint()
	if err != nil || version != currentControlSchema {
		return nil, ErrCommandVersion
	}
	op, err := d.Uint()
	if err != nil {
		return nil, err
	}
	out := &ControlCommand{}
	fields := reflect.ValueOf(out).Elem()
	if op < 1 || op > uint64(fields.NumField()) {
		return nil, ErrCommandVersion
	}
	f := fields.Field(int(op - 1))
	f.Set(reflect.New(f.Type().Elem()))
	if err = readControlValue(d, f.Elem(), 0); err != nil {
		return nil, err
	}
	if err = d.Done(); err != nil {
		return nil, err
	}
	b, err := encodeControlCommand(*out)
	if err != nil || !bytes.Equal(b, raw) {
		return nil, ErrCommandShape
	}
	return out, nil
}

func cloneControlValue(v reflect.Value) reflect.Value {
	switch v.Kind() {
	case reflect.Struct:
		o := reflect.New(v.Type()).Elem()
		for i := 0; i < v.NumField(); i++ {
			o.Field(i).Set(cloneControlValue(v.Field(i)))
		}
		return o
	case reflect.Slice:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		o := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
		for i := 0; i < v.Len(); i++ {
			o.Index(i).Set(cloneControlValue(v.Index(i)))
		}
		return o
	case reflect.Pointer:
		if v.IsNil() {
			return reflect.Zero(v.Type())
		}
		o := reflect.New(v.Type().Elem())
		o.Elem().Set(cloneControlValue(v.Elem()))
		return o
	default:
		return v
	}
}
func cloneRecovery(s *RecoverySnapshot) *RecoverySnapshot {
	if s == nil {
		return nil
	}
	v := cloneControlValue(reflect.ValueOf(*s)).Interface().(RecoverySnapshot)
	return &v
}
