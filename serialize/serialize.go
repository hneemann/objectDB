package serialize

import (
	"fmt"
	"io"
	"math"
	"math/bits"
	"reflect"
)

type typeCode uint8

const (
	invalidCode typeCode = iota
	boolCode
	int8Code
	int16Code
	int32Code
	int64Code
	uint8Code
	uint16Code
	uint32Code
	uint64Code
	float32Code
	float64Code
	stringCode
	structCode
	arrayCode
	mapCode
	interfaceCode
)

const pointerMask = 1 << 31

type serializer struct {
	typeList []reflect.Type
	typeMap  map[string]uint32
}

func New() *serializer {
	return &serializer{typeMap: map[string]uint32{}}
}

func (s *serializer) Register(i any) *serializer {
	t := reflect.TypeOf(i)
	s.typeMap[t.String()] = uint32(len(s.typeList))
	s.typeList = append(s.typeList, t)
	return s
}

func (s *serializer) Write(w io.Writer, data any) error {
	return s.writeValue(w, reflect.ValueOf(data), 0)
}

func (s *serializer) writeValue(w io.Writer, v reflect.Value, ptrDepth int) error {
	switch v.Kind() {
	case reflect.Bool:
		return s.writeBool(w, v)
	case reflect.Int8:
		return s.writeIntBytes(w, int8Code, v.Int(), 1)
	case reflect.Uint8:
		return s.writeIntBytes(w, uint8Code, v.Int(), 1)
	case reflect.Int16:
		return s.writeIntBytes(w, int16Code, v.Int(), 2)
	case reflect.Uint16:
		return s.writeIntBytes(w, uint16Code, v.Int(), 2)
	case reflect.Int32:
		return s.writeIntBytes(w, int32Code, v.Int(), 4)
	case reflect.Uint32:
		return s.writeIntBytes(w, uint32Code, v.Int(), 4)
	case reflect.Int64:
		return s.writeIntBytes(w, int64Code, v.Int(), 8)
	case reflect.Uint64:
		return s.writeIntBytes(w, uint64Code, v.Int(), 8)
	case reflect.Int:
		if bits.UintSize == 32 {
			return s.writeIntBytes(w, int32Code, v.Int(), 4)
		} else {
			return s.writeIntBytes(w, int64Code, v.Int(), 8)
		}
	case reflect.Uint:
		if bits.UintSize == 32 {
			return s.writeIntBytes(w, uint32Code, v.Int(), 4)
		} else {
			return s.writeIntBytes(w, uint64Code, v.Int(), 8)
		}
	case reflect.Float32:
		return s.writeIntBytes(w, float32Code, int64(math.Float32bits(float32(v.Float()))), 4)
	case reflect.Float64:
		return s.writeIntBytes(w, float64Code, int64(math.Float64bits(v.Float())), 8)
	case reflect.String:
		return s.writeString(w, v.String())
	case reflect.Struct:
		return s.writeStruct(w, v, ptrDepth)
	case reflect.Pointer:
		return s.writeValue(w, v.Elem(), ptrDepth+1)
	case reflect.Invalid:
		return s.writeTypeCode(w, invalidCode)
	case reflect.Slice, reflect.Array:
		return s.writeArray(w, v, ptrDepth)
	case reflect.Map:
		return s.writeMap(w, v, ptrDepth)
	case reflect.Interface:
		return s.writeInterface(w, v, ptrDepth)
	}

	return fmt.Errorf("unsuported type %v", v)
}

func (s *serializer) writeInterface(w io.Writer, v reflect.Value, depth int) error {
	err := s.writeTypeCode(w, interfaceCode)
	if err != nil {
		return err
	}

	val := v.Elem()

	pointer := false
	if val.Kind() == reflect.Pointer {
		pointer = true
		val = val.Elem()
	}

	ic, ok := s.typeMap[val.Type().String()]

	if !ok {
		return fmt.Errorf("found unregistered interface %v", val.Type())
	}

	if pointer {
		ic |= pointerMask
	}

	err = s.writeInt32(w, ic)
	if err != nil {
		return err
	}

	return s.writeValue(w, val, depth)
}

func (s *serializer) writeMap(w io.Writer, v reflect.Value, ptrDepth int) error {
	err := s.writeTypeCode(w, mapCode)
	if err != nil {
		return err
	}

	err = s.writeInt32(w, uint32(v.Len()))
	if err != nil {
		return err
	}

	it := v.MapRange()
	for it.Next() {
		err = s.writeValue(w, it.Key(), ptrDepth)
		if err != nil {
			return err
		}
		err = s.writeValue(w, it.Value(), ptrDepth)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *serializer) writeArray(w io.Writer, v reflect.Value, prtDepth int) error {
	err := s.writeTypeCode(w, arrayCode)
	if err != nil {
		return err
	}
	l := v.Len()
	err = s.writeInt32(w, uint32(l))
	if err != nil {
		return err
	}
	for i := 0; i < l; i++ {
		err = s.writeValue(w, v.Index(i), prtDepth)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *serializer) writeBool(w io.Writer, v reflect.Value) error {
	err := s.writeTypeCode(w, boolCode)
	if err != nil {
		return err
	}
	if v.Bool() {
		return s.writeBytes(w, 1)
	} else {
		return s.writeBytes(w, 0)
	}
}

func (s *serializer) writeStruct(w io.Writer, v reflect.Value, ptrDepth int) error {
	err := s.writeTypeCode(w, structCode)
	if err != nil {
		return err
	}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.CanSet() {
			err = s.writeValue(w, field, ptrDepth)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *serializer) writeString(w io.Writer, str string) error {
	err := s.writeTypeCode(w, stringCode)
	if err != nil {
		return err
	}
	err = s.writeInt32(w, uint32(len(str)))
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(str))
	return err
}

func (s *serializer) writeIntBytes(w io.Writer, code typeCode, v int64, n int) error {
	err := s.writeTypeCode(w, code)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		err = s.writeBytes(w, byte(v&0xff))
		if err != nil {
			return err
		}
		v = v >> 8
	}
	return err
}

func (s *serializer) writeInt32(w io.Writer, i uint32) error {
	return s.writeBytes(w,
		byte(i&0xff),
		byte((i>>8)&0xff),
		byte((i>>16)&0xff),
		byte((i>>24)&0xff),
	)
}

func (s *serializer) writeTypeCode(w io.Writer, c typeCode) error {
	_, err := w.Write([]byte{byte(c)})
	return err
}

func (s *serializer) writeBytes(w io.Writer, b ...byte) error {
	_, err := w.Write(b)
	return err
}

func (s *serializer) Read(r io.Reader, data any) (err error) {
	rv := reflect.ValueOf(data)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("invalid target type: %v", reflect.TypeOf(data))
	}

	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("error during decoding: %v", rec)
		}
	}()

	return s.readValue(r, rv)
}

func (s *serializer) readValue(r io.Reader, v reflect.Value) error {
	switch v.Kind() {
	case reflect.Struct:
		return s.readStruct(r, v)
	case reflect.Bool:
		return s.readBool(r, v)
	case reflect.Int, reflect.Uint:
		return s.readInt(r, v)
	case reflect.Uint8:
		return s.readSizedInt(r, v, uint8Code)
	case reflect.Uint16:
		return s.readSizedInt(r, v, uint16Code)
	case reflect.Uint32:
		return s.readSizedInt(r, v, uint32Code)
	case reflect.Uint64:
		return s.readSizedInt(r, v, uint64Code)
	case reflect.Int8:
		return s.readSizedInt(r, v, int8Code)
	case reflect.Int16:
		return s.readSizedInt(r, v, int16Code)
	case reflect.Int32:
		return s.readSizedInt(r, v, int32Code)
	case reflect.Int64:
		return s.readSizedInt(r, v, int64Code)
	case reflect.Float64:
		return s.readFloat64(r, v)
	case reflect.String:
		return s.readString(r, v)
	case reflect.Pointer:
		if v.IsNil() {
			nv := reflect.New(v.Type().Elem())
			v.Set(nv)
		}
		return s.readValue(r, v.Elem())
	case reflect.Slice:
		return s.readSlice(r, v)
	case reflect.Array:
		return s.readArray(r, v)
	case reflect.Map:
		return s.readMap(r, v)
	case reflect.Interface:
		return s.readInterface(r, v)
	}

	return fmt.Errorf("unsuported type %v", v.Type())
}

func (s *serializer) readInterface(r io.Reader, v reflect.Value) error {
	err := expect(r, interfaceCode)
	if err != nil {
		return err
	}
	ic, err := s.readInt32(r)
	if err != nil {
		return err
	}

	pointer := ic&pointerMask != 0
	ic &= pointerMask - 1

	intType := s.typeList[ic]

	val := reflect.New(intType)

	err = s.readValue(r, val)
	if err != nil {
		return err
	}

	if pointer {
		v.Set(val)
	} else {
		v.Set(val.Elem())
	}

	return nil
}

func (s *serializer) readMap(r io.Reader, v reflect.Value) error {
	err := expect(r, mapCode)
	if err != nil {
		return err
	}
	l, err := s.readInt32(r)
	if err != nil {
		return err
	}

	keyType := v.Type().Key()
	valType := v.Type().Elem()

	newMap := reflect.MakeMap(v.Type())
	for i := 0; i < l; i++ {
		key := reflect.New(keyType)
		err = s.readValue(r, key)
		if err != nil {
			return err
		}

		val := reflect.New(valType)
		err = s.readValue(r, val)
		if err != nil {
			return err
		}

		newMap.SetMapIndex(key.Elem(), val.Elem())
	}
	v.Set(newMap)

	return nil
}

func (s *serializer) readSlice(r io.Reader, v reflect.Value) error {
	err := expect(r, arrayCode)
	if err != nil {
		return err
	}
	l, err := s.readInt32(r)
	if err != nil {
		return err
	}

	slice := reflect.MakeSlice(v.Type(), l, l)
	for i := 0; i < l; i++ {
		err = s.readValue(r, slice.Index(i))
		if err != nil {
			return err
		}
	}
	v.Set(slice)

	return nil
}

func (s *serializer) readArray(r io.Reader, v reflect.Value) error {
	err := expect(r, arrayCode)
	if err != nil {
		return err
	}
	l, err := s.readInt32(r)
	if err != nil {
		return err
	}

	for i := 0; i < l; i++ {
		err = s.readValue(r, v.Index(i))
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *serializer) readBool(r io.Reader, v reflect.Value) error {
	err := expect(r, boolCode)
	if err != nil {
		return err
	}
	buf := make([]byte, 1)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return err
	}
	v.SetBool(buf[0] != 0)
	return nil
}

func (s *serializer) readSizedInt(r io.Reader, v reflect.Value, code typeCode) error {
	err := expect(r, code)
	if err != nil {
		return err
	}
	l := getIntLen(code)

	return s.readRawInt(r, v, l)
}

func (s *serializer) readInt(r io.Reader, v reflect.Value) error {
	buf := make([]byte, 1)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return err
	}

	switch typeCode(buf[0]) {
	case int32Code:
		return s.readRawInt(r, v, 4)
	case int64Code:
		return s.readRawInt(r, v, 8)
	default:
		return fmt.Errorf("invalid int data")
	}
}

func (s *serializer) readRawInt(r io.Reader, v reflect.Value, l int) error {
	buf := make([]byte, l)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return err
	}

	var val int64
	for i := 0; i < l; i++ {
		val = val << 8
		val |= int64(buf[l-i-1] & 0xff)
	}
	v.SetInt(val)
	return nil
}

func getIntLen(code typeCode) int {
	switch code {
	case int8Code, uint8Code:
		return 1
	case int16Code, uint16Code:
		return 2
	case int32Code, uint32Code:
		return 4
	case int64Code, uint64Code:
		return 8
	default:
		return 0
	}
}

func (s *serializer) readStruct(r io.Reader, v reflect.Value) error {
	err := expect(r, structCode)
	if err != nil {
		return err
	}
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.CanSet() {
			err = s.readValue(r, field)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *serializer) readString(r io.Reader, v reflect.Value) error {
	err := expect(r, stringCode)
	if err != nil {
		return err
	}
	strLen, err := s.readInt32(r)
	if err != nil {
		return fmt.Errorf("could not read string len: %w", err)
	}
	buf := make([]byte, strLen)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return fmt.Errorf("could not read string data: %w", err)
	}
	v.SetString(string(buf))
	return nil
}

func (s *serializer) readFloat64(r io.Reader, v reflect.Value) error {
	err := expect(r, float64Code)
	if err != nil {
		return err
	}
	floatBits, err := s.readInt64(r)
	if err != nil {
		return fmt.Errorf("could not read string len: %w", err)
	}

	v.SetFloat(math.Float64frombits(floatBits))

	return nil
}

func (s *serializer) readInt32(r io.Reader) (int, error) {
	buf := make([]byte, 4)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, fmt.Errorf("could not read int32: %w", err)
	}
	return int(buf[0]) | (int(buf[1]) << 8) | (int(buf[2]) << 16) | (int(buf[3]) << 24), nil
}
func (s *serializer) readInt64(r io.Reader) (uint64, error) {
	buf := make([]byte, 8)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return 0, fmt.Errorf("could not read int64: %w", err)
	}
	return uint64(buf[0]) |
			(uint64(buf[1]) << 8) |
			(uint64(buf[2]) << 16) |
			(uint64(buf[3]) << 24) |
			(uint64(buf[4]) << 32) |
			(uint64(buf[5]) << 40) |
			(uint64(buf[6]) << 48) |
			(uint64(buf[7]) << 56),
		nil
}

func expect(r io.Reader, code typeCode) error {
	buf := []byte{0}
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return fmt.Errorf("could not read type code: %w", err)
	}
	if buf[0] != byte(code) {
		return fmt.Errorf("unexpected type code: expected %v, found %v", code, buf[0])
	}
	return nil
}
