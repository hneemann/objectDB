package serialize

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"testing"
	"time"
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

type Test struct {
	T time.Time
}

func TestSerializer(t *testing.T) {
	ti := time.Now()
	s := []Test{
		{T: ti},
		{T: ti.Add(time.Hour)},
		{T: ti.Add(time.Hour * 2)},
	}

	ser := New()

	b := bytes.Buffer{}

	err := ser.Write(&b, &s)
	assert.NoError(t, err)

	var r []Test
	err = ser.Read(&b, &r)
	assert.NoError(t, err)

	assert.True(t, s[0].T.Equal(r[0].T))
	assert.True(t, s[1].T.Equal(r[1].T))
	assert.True(t, s[2].T.Equal(r[2].T))

}
