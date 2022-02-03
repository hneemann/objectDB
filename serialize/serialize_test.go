package serialize

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestStringWrite(t *testing.T) {
	var w bytes.Buffer

	var a = "Hello"

	err := Write(&w, &a)
	assert.NoError(t, err)

	assert.EqualValues(t, []byte{0xc, 0x5, 0x0, 0x0, 0x0, 0x48, 0x65, 0x6c, 0x6c, 0x6f}, w.Bytes())
}

func TestStructWrite(t *testing.T) {
	var w bytes.Buffer

	var a = struct {
		a int32
		b bool
	}{
		a: 1025,
		b: true,
	}

	err := Write(&w, &a)
	assert.NoError(t, err)

	assert.EqualValues(t, []byte{0xd, 0x4, 0x1, 0x4, 0x0, 0x0, 0x1, 0x1}, w.Bytes())
}

func TestStructNilWrite(t *testing.T) {
	var w bytes.Buffer

	var a = struct {
		a int32
		b *bool
	}{
		a: 1025,
		b: nil,
	}

	err := Write(&w, &a)
	assert.NoError(t, err)

	assert.EqualValues(t, []byte{0xd, 0x4, 0x1, 0x4, 0x0, 0x0, 0x0}, w.Bytes())
}

func TestSlice(t *testing.T) {
	var w bytes.Buffer
	var a = []int16{1, 2, 3, 4}
	err := Write(&w, &a)
	assert.NoError(t, err)
	assert.EqualValues(t, []byte{0xe, 0x4, 0x0, 0x0, 0x0, 0x3, 0x1, 0x0, 0x3, 0x2, 0x0, 0x3, 0x3, 0x0, 0x3, 0x4, 0x0}, w.Bytes())
}

func TestArray(t *testing.T) {
	var w bytes.Buffer
	var a = [4]int16{1, 2, 3, 4}
	err := Write(&w, &a)
	assert.NoError(t, err)
	assert.EqualValues(t, []byte{0xe, 0x4, 0x0, 0x0, 0x0, 0x3, 0x1, 0x0, 0x3, 0x2, 0x0, 0x3, 0x3, 0x0, 0x3, 0x4, 0x0}, w.Bytes())
}

func TestMap(t *testing.T) {
	var w bytes.Buffer
	var a = map[string]string{
		"a": "A",
		"b": "B",
	}
	err := Write(&w, &a)
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
	err := Write(&w, &a)
	assert.NoError(t, err)

	found := w.Bytes()
	e1 := bytes.Equal([]byte{0xf, 0x2, 0x0, 0x0, 0x0, 0x3, 0x2, 0x0, 0x3, 0x20, 0x0, 0x3, 0x1, 0x0, 0x3, 0x10, 0x0}, found)
	e2 := bytes.Equal([]byte{0xf, 0x2, 0x0, 0x0, 0x0, 0x3, 0x1, 0x0, 0x3, 0x10, 0x0, 0x3, 0x2, 0x0, 0x3, 0x20, 0x0}, found)

	assert.True(t, e1 || e2)
}