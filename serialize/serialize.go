// Package serialize is a simple package to serialize data.
// It is able to serialize and deserialize interfaces.
// A custom binary format is generated that is compatible to nothing.
package serialize

import (
	"encoding"
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

type Serializer struct {
	typeList []reflect.Type
	typeMap  map[string]uint32
}

// New creates a new serializer
func New() *Serializer {
	return &Serializer{typeMap: map[string]uint32{}}
}

// Register registers a interface for serialization
func (s *Serializer) Register(i any) *Serializer {
	t := reflect.TypeOf(i)
	s.typeMap[t.String()] = uint32(len(s.typeList))
	s.typeList = append(s.typeList, t)
	return s
}

// Write writes the data to the writer
func (s *Serializer) Write(w io.Writer, data any) error {
	return s.writeValue(w, reflect.ValueOf(data), 0)
}

var (
	binaryMarshalerType   = reflect.TypeOf((*encoding.BinaryMarshaler)(nil)).Elem()
	binaryUnmarshalerType = reflect.TypeOf((*encoding.BinaryUnmarshaler)(nil)).Elem()
)

func (s *Serializer) writeValue(w io.Writer, v reflect.Value, ptrDepth int) error {
	if v.IsValid() && v.Type().Implements(binaryMarshalerType) {
		return s.binMarshal(w, v, ptrDepth)
	}

	switch v.Kind() {
	case reflect.Bool:
		return s.writeBool(w, v)
	case reflect.Int8:
		return s.writeIntBytes(w, int8Code, v.Int(), 1)
	case reflect.Uint8:
		return s.writeIntBytes(w, uint8Code, int64(v.Uint()), 1)
	case reflect.Int16:
		return s.writeIntBytes(w, int16Code, v.Int(), 2)
	case reflect.Uint16:
		return s.writeIntBytes(w, uint16Code, int64(v.Uint()), 2)
	case reflect.Int32:
		return s.writeIntBytes(w, int32Code, v.Int(), 4)
	case reflect.Uint32:
		return s.writeIntBytes(w, uint32Code, int64(v.Uint()), 4)
	case reflect.Int64:
		return s.writeIntBytes(w, int64Code, v.Int(), 8)
	case reflect.Uint64:
		return s.writeIntBytes(w, uint64Code, int64(v.Uint()), 8)
	case reflect.Int:
		if bits.UintSize == 32 {
			return s.writeIntBytes(w, int32Code, v.Int(), 4)
		} else {
			return s.writeIntBytes(w, int64Code, v.Int(), 8)
		}
	case reflect.Uint:
		if bits.UintSize == 32 {
			return s.writeIntBytes(w, uint32Code, int64(v.Uint()), 4)
		} else {
			return s.writeIntBytes(w, uint64Code, int64(v.Uint()), 8)
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

func (s *Serializer) binMarshal(w io.Writer, v reflect.Value, depth int) error {
	r := v.MethodByName("MarshalBinary").Call(nil)
	if !(r[1].IsNil()) {
		return fmt.Errorf("error calling MarshalBinary")
	}
	return s.writeValue(w, r[0], depth)
}

func (s *Serializer) writeInterface(w io.Writer, v reflect.Value, depth int) error {
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

func (s *Serializer) writeMap(w io.Writer, v reflect.Value, ptrDepth int) error {
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

func (s *Serializer) writeArray(w io.Writer, v reflect.Value, prtDepth int) error {
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

func (s *Serializer) writeBool(w io.Writer, v reflect.Value) error {
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

func (s *Serializer) writeStruct(w io.Writer, v reflect.Value, ptrDepth int) error {
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

func (s *Serializer) writeString(w io.Writer, str string) error {
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

func (s *Serializer) writeIntBytes(w io.Writer, code typeCode, v int64, n int) error {
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

func (s *Serializer) writeInt32(w io.Writer, i uint32) error {
	return s.writeBytes(w,
		byte(i&0xff),
		byte((i>>8)&0xff),
		byte((i>>16)&0xff),
		byte((i>>24)&0xff),
	)
}

func (s *Serializer) writeTypeCode(w io.Writer, c typeCode) error {
	_, err := w.Write([]byte{byte(c)})
	return err
}

func (s *Serializer) writeBytes(w io.Writer, b ...byte) error {
	_, err := w.Write(b)
	return err
}

// Read reads the data from the reader
func (s *Serializer) Read(r io.Reader, data any) (err error) {
	rv := reflect.ValueOf(data)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return fmt.Errorf("invalid target type: %v", reflect.TypeOf(data))
	}

	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("error during decoding: %v", rec)
		}
	}()

	s.readValue(r, rv)
	return nil
}

func (s *Serializer) readValue(r io.Reader, v reflect.Value) {
	if v.CanAddr() && v.Addr().Type().Implements(binaryUnmarshalerType) {
		s.binUnmarshal(r, v)
		return
	}

	switch v.Kind() {
	case reflect.Struct:
		s.readStruct(r, v)
	case reflect.Bool:
		s.readBool(r, v)
	case reflect.Int, reflect.Uint:
		s.readInt(r, v)
	case reflect.Uint8:
		v.SetUint(s.readSizedInt(r, uint8Code))
	case reflect.Uint16:
		v.SetUint(s.readSizedInt(r, uint16Code))
	case reflect.Uint32:
		v.SetUint(s.readSizedInt(r, uint32Code))
	case reflect.Uint64:
		v.SetUint(s.readSizedInt(r, uint64Code))
	case reflect.Int8:
		v.SetInt(int64(s.readSizedInt(r, int8Code)))
	case reflect.Int16:
		v.SetInt(int64(s.readSizedInt(r, int16Code)))
	case reflect.Int32:
		v.SetInt(int64(s.readSizedInt(r, int32Code)))
	case reflect.Int64:
		v.SetInt(int64(s.readSizedInt(r, int64Code)))
	case reflect.Float64:
		s.readFloat64(r, v)
	case reflect.Float32:
		s.readFloat32(r, v)
	case reflect.String:
		s.readString(r, v)
	case reflect.Pointer:
		if v.IsNil() {
			nv := reflect.New(v.Type().Elem())
			v.Set(nv)
		}
		s.readValue(r, v.Elem())
	case reflect.Slice:
		s.readSlice(r, v)
	case reflect.Array:
		s.readArray(r, v)
	case reflect.Map:
		s.readMap(r, v)
	case reflect.Interface:
		s.readInterface(r, v)
	default:
		panic(fmt.Errorf("unsuported type %v", v.Type()))
	}
}

func (s *Serializer) readInterface(r io.Reader, v reflect.Value) {
	expect(r, interfaceCode)
	ic := s.readInt32(r)

	pointer := ic&pointerMask != 0
	ic &= pointerMask - 1

	intType := s.typeList[ic]

	val := reflect.New(intType)

	s.readValue(r, val)

	if pointer {
		v.Set(val)
	} else {
		v.Set(val.Elem())
	}
}

func (s *Serializer) readMap(r io.Reader, v reflect.Value) {
	expect(r, mapCode)
	l := s.readInt32(r)

	keyType := v.Type().Key()
	valType := v.Type().Elem()

	newMap := reflect.MakeMap(v.Type())
	for i := 0; i < l; i++ {
		key := reflect.New(keyType)
		s.readValue(r, key)
		val := reflect.New(valType)
		s.readValue(r, val)

		newMap.SetMapIndex(key.Elem(), val.Elem())
	}
	v.Set(newMap)
}

func (s *Serializer) readSlice(r io.Reader, v reflect.Value) {
	expect(r, arrayCode)
	l := s.readInt32(r)

	slice := reflect.MakeSlice(v.Type(), l, l)
	for i := 0; i < l; i++ {
		s.readValue(r, slice.Index(i))
	}
	v.Set(slice)
}

func (s *Serializer) readArray(r io.Reader, v reflect.Value) {
	expect(r, arrayCode)
	l := s.readInt32(r)

	for i := 0; i < l; i++ {
		s.readValue(r, v.Index(i))
	}
}

func (s *Serializer) readBool(r io.Reader, v reflect.Value) {
	expect(r, boolCode)
	buf := make([]byte, 1)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		panic(err)
	}
	v.SetBool(buf[0] != 0)
}

func (s *Serializer) readSizedInt(r io.Reader, code typeCode) uint64 {
	expect(r, code)
	l := getIntLen(code)
	return s.readRawInt(r, l)
}

func (s *Serializer) readInt(r io.Reader, v reflect.Value) {
	buf := make([]byte, 1)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		panic(err)
	}

	switch typeCode(buf[0]) {
	case int32Code:
		v.SetInt(int64(s.readRawInt(r, 4)))
	case int64Code:
		v.SetInt(int64(s.readRawInt(r, 8)))
	default:
		panic("invalid int data")
	}
}

func (s *Serializer) readRawInt(r io.Reader, l int) uint64 {
	buf := make([]byte, l)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		panic(err)
	}

	var val uint64
	for i := 0; i < l; i++ {
		val = val << 8
		val |= uint64(buf[l-i-1] & 0xff)
	}
	return val
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

func (s *Serializer) readStruct(r io.Reader, v reflect.Value) {
	expect(r, structCode)
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		if field.CanSet() {
			s.readValue(r, field)
		}
	}
}

func (s *Serializer) readString(r io.Reader, v reflect.Value) {
	expect(r, stringCode)
	strLen := s.readInt32(r)
	buf := make([]byte, strLen)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		panic(fmt.Errorf("could not read string data: %w", err))
	}
	v.SetString(string(buf))
}

func (s *Serializer) readFloat32(r io.Reader, v reflect.Value) {
	expect(r, float32Code)
	floatBits := s.readInt32(r)
	v.SetFloat(float64(math.Float32frombits(uint32(floatBits))))
}

func (s *Serializer) readFloat64(r io.Reader, v reflect.Value) {
	expect(r, float64Code)
	floatBits := s.readInt64(r)
	v.SetFloat(math.Float64frombits(floatBits))
}

func (s *Serializer) readInt32(r io.Reader) int {
	buf := make([]byte, 4)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		panic(fmt.Errorf("could not read int32: %w", err))
	}
	return int(buf[0]) | (int(buf[1]) << 8) | (int(buf[2]) << 16) | (int(buf[3]) << 24)
}
func (s *Serializer) readInt64(r io.Reader) uint64 {
	buf := make([]byte, 8)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		panic(fmt.Errorf("could not read int64: %w", err))
	}
	return uint64(buf[0]) |
		(uint64(buf[1]) << 8) |
		(uint64(buf[2]) << 16) |
		(uint64(buf[3]) << 24) |
		(uint64(buf[4]) << 32) |
		(uint64(buf[5]) << 40) |
		(uint64(buf[6]) << 48) |
		(uint64(buf[7]) << 56)
}

func (s *Serializer) binUnmarshal(r io.Reader, v reflect.Value) {
	b := []byte{}
	ar := reflect.ValueOf(&b).Elem()
	s.readSlice(r, ar)

	method := v.Addr().MethodByName("UnmarshalBinary")
	res := method.Call([]reflect.Value{ar})
	if !(res[0].IsNil()) {
		panic(fmt.Errorf("error calling UnmarshalBinary on %v: %v", v.Type(), res[0]))
	}
}

func expect(r io.Reader, code typeCode) {
	buf := []byte{0}
	_, err := io.ReadFull(r, buf)
	if err != nil {
		panic(fmt.Errorf("could not read type code: %w", err))
	}
	if buf[0] != byte(code) {
		panic(fmt.Errorf("unexpected type code: expected %v, found %v", code, buf[0]))
	}
}
