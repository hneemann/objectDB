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
)

func Write(w io.Writer, data any) error {
	return writeValue(w, reflect.ValueOf(data), 0)
}

func writeValue(w io.Writer, v reflect.Value, ptrDepth int) error {
	switch v.Kind() {
	case reflect.Bool:
		return writeBool(w, v)
	case reflect.Int8:
		return writeIntBytes(w, int8Code, v.Int(), 1)
	case reflect.Uint8:
		return writeIntBytes(w, uint8Code, v.Int(), 1)
	case reflect.Int16:
		return writeIntBytes(w, int16Code, v.Int(), 2)
	case reflect.Uint16:
		return writeIntBytes(w, uint16Code, v.Int(), 2)
	case reflect.Int32:
		return writeIntBytes(w, int32Code, v.Int(), 4)
	case reflect.Uint32:
		return writeIntBytes(w, uint32Code, v.Int(), 4)
	case reflect.Int64:
		return writeIntBytes(w, int64Code, v.Int(), 8)
	case reflect.Uint64:
		return writeIntBytes(w, uint64Code, v.Int(), 8)
	case reflect.Int:
		if bits.UintSize == 32 {
			return writeIntBytes(w, int32Code, v.Int(), 4)
		} else {
			return writeIntBytes(w, int64Code, v.Int(), 8)
		}
	case reflect.Uint:
		if bits.UintSize == 32 {
			return writeIntBytes(w, uint32Code, v.Int(), 4)
		} else {
			return writeIntBytes(w, uint64Code, v.Int(), 8)
		}
	case reflect.Float32:
		return writeIntBytes(w, float32Code, int64(math.Float32bits(float32(v.Float()))), 4)
	case reflect.Float64:
		return writeIntBytes(w, float64Code, int64(math.Float64bits(v.Float())), 8)
	case reflect.String:
		return writeString(w, v.String())
	case reflect.Struct:
		return writeStruct(w, v, ptrDepth)
	case reflect.Pointer:
		return writeValue(w, v.Elem(), ptrDepth+1)
	case reflect.Invalid:
		return writeTypeCode(w, invalidCode)
	case reflect.Slice, reflect.Array:
		return writeArray(w, v, ptrDepth)
	case reflect.Map:
		return writeMap(w, v, ptrDepth)
	}

	return fmt.Errorf("unsuported type %v", v)
}

func writeMap(w io.Writer, v reflect.Value, ptrDepth int) error {
	err := writeTypeCode(w, mapCode)
	if err != nil {
		return err
	}

	err = writeInt32(w, uint32(v.Len()))
	if err != nil {
		return err
	}

	it := v.MapRange()
	for it.Next() {
		err = writeValue(w, it.Key(), ptrDepth)
		if err != nil {
			return err
		}
		err = writeValue(w, it.Value(), ptrDepth)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeArray(w io.Writer, v reflect.Value, prtDepth int) error {
	err := writeTypeCode(w, arrayCode)
	if err != nil {
		return err
	}
	l := v.Len()
	err = writeInt32(w, uint32(l))
	if err != nil {
		return err
	}
	for i := 0; i < l; i++ {
		err = writeValue(w, v.Index(i), prtDepth)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeBool(w io.Writer, v reflect.Value) error {
	err := writeTypeCode(w, boolCode)
	if err != nil {
		return err
	}
	if v.Bool() {
		return writeBytes(w, 1)
	} else {
		return writeBytes(w, 0)
	}
}

func writeStruct(w io.Writer, v reflect.Value, ptrDepth int) error {
	err := writeTypeCode(w, structCode)
	if err != nil {
		return err
	}
	for i := 0; i < v.NumField(); i++ {
		err = writeValue(w, v.Field(i), ptrDepth)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeString(w io.Writer, s string) error {
	err := writeTypeCode(w, stringCode)
	if err != nil {
		return err
	}
	err = writeInt32(w, uint32(len(s)))
	if err != nil {
		return err
	}
	_, err = w.Write([]byte(s))
	return err
}

func writeIntBytes(w io.Writer, code typeCode, v int64, n int) error {
	err := writeTypeCode(w, code)
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		err = writeBytes(w, byte(v&0xff))
		if err != nil {
			return err
		}
		v = v >> 8
	}
	return err
}

func writeInt32(w io.Writer, i uint32) error {
	return writeBytes(w,
		byte(i&0xff),
		byte((i>>8)&0xff),
		byte((i>>16)&0xff),
		byte((i>>24)&0xff),
	)
}

func writeTypeCode(w io.Writer, c typeCode) error {
	_, err := w.Write([]byte{byte(c)})
	return err
}

func writeBytes(w io.Writer, b ...byte) error {
	_, err := w.Write(b)
	return err
}
