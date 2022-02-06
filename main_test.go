package objectDB

import (
	"objectDB/serialize"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type Test struct {
	Time time.Time
}

func (t Test) DeepCopy() Test {
	return Test{
		Time: t.Time,
	}
}

func (t Test) GetDate() time.Time {
	return t.Time
}

func (t Test) String() string {
	return t.Time.String()
}

func TestSimple(t *testing.T) {
	table, err := New[Test](Monthly[Test](), PersistJSON[Test]("testdata", "_db.json"))
	assert.NoError(t, err)
	n := time.Now()
	t1 := Test{Time: n}
	table.Insert(Test{Time: n.Add(-time.Hour * 24 * 30)})
	table.Insert(t1)
	table.Insert(Test{Time: n.Add(time.Hour * 24 * 30)})

	a := table.All()
	assert.EqualValues(t, 3, a.Len())
	t1.Time = time.Time{}
	assert.EqualValues(t, n, a.Item(1).Time)

	b := table.Match(func(e *Test) bool { return e.Time == n })
	assert.EqualValues(t, 1, b.Len())

	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
}

func TestStorage(t *testing.T) {
	table, err := New[Test](Monthly[Test](), PersistJSON[Test]("testdata", "_db.json"))
	assert.NoError(t, err)
	n := time.Now()

	table.Insert(Test{Time: n.Add(-time.Hour * 24 * 30)})
	table.Insert(Test{Time: n})
	table.Insert(Test{Time: n.Add(time.Hour * 24 * 30)})

	table2, err := New[Test](Monthly[Test](), PersistJSON[Test]("testdata", "_db.json"))
	a := table2.All()
	assert.EqualValues(t, 3, a.Len())
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
}

func TestStorageSerializer(t *testing.T) {
	table, err := New[Test](Monthly[Test](), PersistSerializer[Test]("testdata", "_db.bin", serialize.New()))
	assert.NoError(t, err)
	n := time.Now()

	table.Insert(Test{Time: n.Add(-time.Hour * 24 * 30)})
	table.Insert(Test{Time: n})
	table.Insert(Test{Time: n.Add(time.Hour * 24 * 30)})

	table2, err := New[Test](Monthly[Test](), PersistSerializer[Test]("testdata", "_db.bin", serialize.New()))
	a := table2.All()
	assert.EqualValues(t, 3, a.Len())
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
}

func TestInsert(t *testing.T) {
	table, err := New[Test](nil, nil)
	table.SetPrimaryOrder(func(a, b *Test) bool { return a.Time.Before(b.Time) })
	assert.NoError(t, err)
	n := time.Now()

	table.Insert(Test{Time: n.Add(time.Hour * 5)})
	table.Insert(Test{Time: n.Add(time.Hour * 8)})
	table.Insert(Test{Time: n.Add(time.Hour * 7)})
	table.Insert(Test{Time: n.Add(time.Hour * 2)})
	table.Insert(Test{Time: n.Add(time.Hour * 1)})
	table.Insert(Test{Time: n.Add(time.Hour * 4)})
	table.Insert(Test{Time: n.Add(time.Hour * 3)})
	table.Insert(Test{Time: n.Add(time.Hour * 9)})
	table.Insert(Test{Time: n.Add(time.Hour * 6)})

	r := table.All()
	assert.EqualValues(t, 9, r.Len())
	for i := 1; i < r.Len(); i++ {
		assert.True(t, r.Item(i-1).Time.Before(r.Item(i).Time))
	}

	r = r.Order(func(a, b *Test) bool { return b.Time.Before(a.Time) })
	for i := 1; i < r.Len(); i++ {
		assert.True(t, r.Item(i-1).Time.After(r.Item(i).Time))
	}
}
