package objectDB

import (
	"objectDB/serialize"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var myMonthly = Monthly[time.Time](func(t *time.Time) time.Time {
	return *t
})

func TestSimple(t *testing.T) {
	table, err := New[time.Time](myMonthly, PersistJSON[time.Time]("testdata", "_db.json"), nil)
	assert.NoError(t, err)
	t1 := time.Now()
	table.Insert(t1.Add(-time.Hour * 24 * 30))
	table.Insert(t1)
	table.Insert(t1.Add(time.Hour * 24 * 30))

	a := table.All()
	assert.EqualValues(t, 3, a.Len())
	assert.EqualValues(t, t1, *Must(a.Item(1)))

	b := table.Match(func(e *time.Time) bool { return *e == t1 })
	assert.EqualValues(t, 1, b.Len())

	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
}

func Must[A any](a A, err error) A {
	if err != nil {
		panic(err)
	}
	return a
}

func TestStorage(t *testing.T) {
	table, err := New[time.Time](myMonthly, PersistJSON[time.Time]("testdata", "_db.json"), nil)
	assert.NoError(t, err)
	n := time.Now()

	table.Insert(n.Add(-time.Hour * 24 * 30))
	table.Insert(n)
	table.Insert(n.Add(time.Hour * 24 * 30))

	table2, err := New[time.Time](myMonthly, PersistJSON[time.Time]("testdata", "_db.json"), nil)
	a := table2.All()
	assert.EqualValues(t, 3, a.Len())
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
}

func TestStorageSerializer(t *testing.T) {
	table, err := New[time.Time](myMonthly, PersistSerializer[time.Time]("testdata", "_db.bin", serialize.New()), nil)
	assert.NoError(t, err)
	n := time.Now()

	table.Insert(n.Add(-time.Hour * 24 * 30))
	table.Insert(n)
	table.Insert(n.Add(time.Hour * 24 * 30))

	table2, err := New[time.Time](myMonthly, PersistSerializer[time.Time]("testdata", "_db.bin", serialize.New()), nil)
	a := table2.All()
	assert.EqualValues(t, 3, a.Len())
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
}

func TestInsert(t *testing.T) {
	table, err := New[time.Time](myMonthly, nil, nil)
	table.SetPrimaryOrder(func(a, b *time.Time) bool { return a.Before(*b) })
	assert.NoError(t, err)
	n := time.Now()

	table.Insert(n.Add(time.Hour * 5))
	table.Insert(n.Add(time.Hour * 8))
	table.Insert(n.Add(time.Hour * 7))
	table.Insert(n.Add(time.Hour * 2))
	table.Insert(n.Add(time.Hour * 1))
	table.Insert(n.Add(time.Hour * 4))
	table.Insert(n.Add(time.Hour * 3))
	table.Insert(n.Add(time.Hour * 9))
	table.Insert(n.Add(time.Hour * 6))

	r := table.All()
	assert.EqualValues(t, 9, r.Len())
	for i := 1; i < r.Len(); i++ {
		assert.True(t, Must(r.Item(i-1)).Before(*Must(r.Item(i))))
	}

	r, err = r.Order(func(a, b *time.Time) bool { return b.Before(*a) })
	assert.NoError(t, err)
	for i := 1; i < r.Len(); i++ {
		assert.True(t, Must(r.Item(i-1)).After(*Must(r.Item(i))))
	}
}
