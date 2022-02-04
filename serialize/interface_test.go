package serialize

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
)

type MyStr struct {
	V string
}

func (ms *MyStr) String() string {
	return ms.V
}

type MyFloat struct {
	V float64
}

func (mf *MyFloat) String() string {
	return fmt.Sprintf("%.5f", mf.V)
}

type MyFloat32 struct {
	V float32
}

func (mf *MyFloat32) String() string {
	return fmt.Sprintf("%.5f", mf.V)
}

func TestInterface(t *testing.T) {
	s := []fmt.Stringer{
		&MyStr{V: "Hello"},
		&MyFloat{V: math.Pi},
		&MyFloat32{V: math.Pi},
	}

	ser := New().
		Register(MyStr{}).
		Register(MyFloat{}).
		Register(MyFloat32{})

	b := bytes.Buffer{}

	err := ser.Write(&b, &s)
	assert.NoError(t, err)

	var r []fmt.Stringer
	err = ser.Read(&b, &r)
	assert.NoError(t, err)

	assert.EqualValues(t, "Hello", r[0].String())
	assert.EqualValues(t, "3.14159", r[1].String())
	assert.EqualValues(t, "3.14159", r[2].String())

}
