package serialize

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

func TestStringWrite(t *testing.T) {
	var w bytes.Buffer

	var a = "Hello"

	err := New().Write(&w, &a)
	assert.NoError(t, err)

	assert.EqualValues(t, []byte{0xc, 0x5, 0x0, 0x0, 0x0, 0x48, 0x65, 0x6c, 0x6c, 0x6f}, w.Bytes())
}

func TestStructWrite(t *testing.T) {
	var w bytes.Buffer

	var a = struct {
		A int32
		B bool
	}{
		A: 1025,
		B: true,
	}

	err := New().Write(&w, &a)
	assert.NoError(t, err)

	assert.EqualValues(t, []byte{0xd, 0x4, 0x1, 0x4, 0x0, 0x0, 0x1, 0x1}, w.Bytes())
}

func TestStructNilWrite(t *testing.T) {
	var w bytes.Buffer

	var a = struct {
		A int32
		B *bool
	}{
		A: 1025,
		B: nil,
	}

	err := New().Write(&w, &a)
	assert.NoError(t, err)

	assert.EqualValues(t, []byte{0xd, 0x4, 0x1, 0x4, 0x0, 0x0, 0x0}, w.Bytes())
}

func TestSlice(t *testing.T) {
	var w bytes.Buffer
	var a = []int16{1, 2, 3, 4}
	err := New().Write(&w, &a)
	assert.NoError(t, err)
	assert.EqualValues(t, []byte{0xe, 0x4, 0x0, 0x0, 0x0, 0x3, 0x1, 0x0, 0x3, 0x2, 0x0, 0x3, 0x3, 0x0, 0x3, 0x4, 0x0}, w.Bytes())
}

func TestArray(t *testing.T) {
	var w bytes.Buffer
	var a = [4]int16{1, 2, 3, 4}
	err := New().Write(&w, &a)
	assert.NoError(t, err)
	assert.EqualValues(t, []byte{0xe, 0x4, 0x0, 0x0, 0x0, 0x3, 0x1, 0x0, 0x3, 0x2, 0x0, 0x3, 0x3, 0x0, 0x3, 0x4, 0x0}, w.Bytes())
}

func TestMap(t *testing.T) {
	var w bytes.Buffer
	var a = map[string]string{
		"a": "A",
		"b": "B",
	}
	err := New().Write(&w, &a)
	assert.NoError(t, err)

	found := w.Bytes()
	e1 := bytes.Equal([]byte{0xf, 0x2, 0x0, 0x0, 0x0, 0xc, 0x1, 0x0, 0x0, 0x0, 0x61, 0xc, 0x1, 0x0, 0x0, 0x0, 0x41, 0xc, 0x1, 0x0, 0x0, 0x0, 0x62, 0xc, 0x1, 0x0, 0x0, 0x0, 0x42}, found)
	e2 := bytes.Equal([]byte{0xf, 0x2, 0x0, 0x0, 0x0, 0xc, 0x1, 0x0, 0x0, 0x0, 0x62, 0xc, 0x1, 0x0, 0x0, 0x0, 0x42, 0xc, 0x1, 0x0, 0x0, 0x0, 0x61, 0xc, 0x1, 0x0, 0x0, 0x0, 0x41}, found)

	assert.True(t, e1 || e2)
}

func TestMapInt(t *testing.T) {
	var w bytes.Buffer
	var a = map[int16]int16{
		1: 16,
		2: 32,
	}
	err := New().Write(&w, &a)
	assert.NoError(t, err)

	found := w.Bytes()
	e1 := bytes.Equal([]byte{0xf, 0x2, 0x0, 0x0, 0x0, 0x3, 0x2, 0x0, 0x3, 0x20, 0x0, 0x3, 0x1, 0x0, 0x3, 0x10, 0x0}, found)
	e2 := bytes.Equal([]byte{0xf, 0x2, 0x0, 0x0, 0x0, 0x3, 0x1, 0x0, 0x3, 0x10, 0x0, 0x3, 0x2, 0x0, 0x3, 0x20, 0x0}, found)

	assert.True(t, e1 || e2)
}

func TestRWStruct(t *testing.T) {
	var b bytes.Buffer

	type st struct {
		A int64
		B int32
		C int16
		D int8
		E string
		F int
	}

	in := st{
		A: 1025,
		B: 1026,
		C: 1027,
		D: 88,
		E: "Hello World",
		F: 32768,
	}

	ser := New()
	err := ser.Write(&b, &in)
	assert.NoError(t, err)

	var out st
	err = ser.Read(&b, &out)
	assert.NoError(t, err)

	assert.EqualValues(t, in, out)
}

func TestRWSlice(t *testing.T) {
	var b bytes.Buffer

	type st struct {
		A int
	}

	var in []st
	for i := 0; i < 10; i++ {
		in = append(in, st{A: i * 10})
	}

	ser := New()
	err := ser.Write(&b, &in)
	assert.NoError(t, err)

	var out []st
	err = ser.Read(&b, &out)
	assert.NoError(t, err)

	assert.EqualValues(t, in, out)
}

func TestRWSlicePointer(t *testing.T) {
	var b bytes.Buffer

	type st struct {
		A int
	}

	var in []*st
	for i := 0; i < 10; i++ {
		in = append(in, &st{A: i * 10})
	}

	ser := New()
	err := ser.Write(&b, &in)
	assert.NoError(t, err)

	var out []*st
	err = ser.Read(&b, &out)
	assert.NoError(t, err)

	assert.EqualValues(t, in, out)
}

func TestRWArray(t *testing.T) {
	var b bytes.Buffer

	type st struct {
		A int
	}

	var in [10]st
	for i := 0; i < 10; i++ {
		in[i] = st{A: i * 10}
	}

	ser := New()
	err := ser.Write(&b, &in)
	assert.NoError(t, err)

	var out [10]st
	err = ser.Read(&b, &out)
	assert.NoError(t, err)

	assert.EqualValues(t, in, out)
}

func TestRWMap(t *testing.T) {
	var b bytes.Buffer

	var in = map[string]string{
		"a": "A",
		"b": "B",
	}

	ser := New()
	err := ser.Write(&b, &in)
	assert.NoError(t, err)

	var out map[string]string
	err = ser.Read(&b, &out)
	assert.NoError(t, err)

	assert.EqualValues(t, in, out)
}

func TestRWMapMix(t *testing.T) {
	var b bytes.Buffer

	var in = map[string]int{
		"a": 1,
		"b": 2,
	}

	ser := New()
	err := ser.Write(&b, &in)
	assert.NoError(t, err)

	var out map[string]int
	err = ser.Read(&b, &out)
	assert.NoError(t, err)

	assert.EqualValues(t, in, out)
}

type KeyStr struct {
	Name string
	Val  int
}

func TestRWMapStruct(t *testing.T) {
	var b bytes.Buffer

	var in = map[KeyStr]int{
		{"a", 1}: 1,
		{"b", 2}: 2,
	}

	ser := New()
	err := ser.Write(&b, &in)
	assert.NoError(t, err)

	var out map[KeyStr]int
	err = ser.Read(&b, &out)
	assert.NoError(t, err)

	assert.EqualValues(t, in, out)
}

func TestRWInt(t *testing.T) {
	var b bytes.Buffer

	type S struct {
		A int
		B int8
		C int16
		D int32
		E int64
		F uint
		G uint8
		H uint16
		I uint32
		J uint64
		K float32
		L float64
	}

	w := S{
		A: 1,
		B: 2,
		C: 3,
		D: -4,
		E: 5,
		F: 6,
		G: 7,
		H: 8,
		I: 9,
		J: 10,
		K: math.Pi,
		L: math.Pi,
	}

	ser := New()
	err := ser.Write(&b, &w)
	assert.NoError(t, err)

	var r S
	err = ser.Read(&b, &r)
	assert.NoError(t, err)

	assert.EqualValues(t, w, r)
}
