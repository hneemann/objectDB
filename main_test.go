package objectDB

import (
	"objectDB/serialize"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var myMonthly = Monthly[time.Time]("test", func(t *time.Time) time.Time {
	return *t
})

func fillTable(table *Table[time.Time]) time.Time {
	n := time.Now()
	table.Insert(n.Add(time.Hour * 5))
	table.Insert(n.Add(time.Hour * 8))
	table.Insert(n.Add(time.Hour * 7))
	table.Insert(n.Add(time.Hour * 2))
	table.Insert(n.Add(time.Hour * 1))
	table.Insert(n)
	table.Insert(n.Add(time.Hour * 4))
	table.Insert(n.Add(time.Hour * 3))
	table.Insert(n.Add(time.Hour * 9))
	table.Insert(n.Add(time.Hour * 6))
	return n
}

func TestSimple(t *testing.T) {
	table, err := New[time.Time](myMonthly, PersistJSON[time.Time]("testdata", "_db.json"), nil)
	assert.NoError(t, err)

	t1 := fillTable(table)

	a := table.Match(func(e *time.Time) bool { return true })
	assert.EqualValues(t, 10, a.Size())
	var pick time.Time
	assert.NoError(t, a.Get(&pick, 5))
	assert.EqualValues(t, t1, pick)

	b := table.Match(func(e *time.Time) bool { return *e == t1 })
	assert.EqualValues(t, 1, b.Size())

	for range 10 {
		assert.NoError(t, a.Delete(0))
	}
}

func TestStorage(t *testing.T) {
	table, err := New[time.Time](myMonthly, PersistJSON[time.Time]("testdata", "_db.json"), nil)
	assert.NoError(t, err)
	n := time.Now()

	table.Insert(n.Add(-time.Hour * 24 * 30))
	table.Insert(n)
	table.Insert(n.Add(time.Hour * 24 * 30))

	table2, err := New[time.Time](myMonthly, PersistJSON[time.Time]("testdata", "_db.json"), nil)
	a := table2.Match(func(e *time.Time) bool { return true })
	assert.EqualValues(t, 3, a.Size())
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
	a := table2.Match(func(e *time.Time) bool { return true })
	assert.EqualValues(t, 3, a.Size())
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
}

func TestInsert(t *testing.T) {
	table, err := New[time.Time](myMonthly, nil, nil)
	assert.NoError(t, err)
	table.SetPrimaryOrder(func(a, b *time.Time) bool { return a.Before(*b) })

	n := fillTable(table)

	r := table.Match(func(e *time.Time) bool { return true })
	assert.EqualValues(t, 10, r.Size())
	for i := range r.Size() {
		var pick time.Time
		assert.NoError(t, r.Get(&pick, i))
		assert.EqualValues(t, n.Add(time.Hour*time.Duration(i)), pick)
	}

	var i int
	for it, err := range r.Iter {
		assert.NoError(t, err)
		assert.EqualValues(t, n.Add(time.Hour*time.Duration(i)), *it)
		i++
	}

	r, err = r.Order(func(a, b *time.Time) bool { return b.Before(*a) })
	assert.NoError(t, err)
	for i := range r.Size() {
		var pick time.Time
		assert.NoError(t, r.Get(&pick, i))
		assert.EqualValues(t, n.Add(time.Hour*time.Duration(9-i)), pick)
	}
}

func TestFirst(t *testing.T) {
	table, err := New[time.Time](myMonthly, nil, nil)
	assert.NoError(t, err)
	table.SetPrimaryOrder(func(a, b *time.Time) bool { return a.Before(*b) })

	n := fillTable(table)

	var found time.Time
	assert.True(t, table.First(&found, func(e *time.Time) bool { return true }))
	assert.EqualValues(t, n, found)
}

func TestAll(t *testing.T) {
	table, err := New[time.Time](myMonthly, nil, nil)
	assert.NoError(t, err)
	table.SetPrimaryOrder(func(a, b *time.Time) bool { return a.Before(*b) })

	n := fillTable(table)

	assert.EqualValues(t, 10, table.Size())
	var i int
	for e := range table.All {
		assert.EqualValues(t, n.Add(time.Hour*time.Duration(i)), e)
		i++
	}

}

func TestUpdate(t *testing.T) {
	table, err := New[time.Time](myMonthly, nil, nil)
	assert.NoError(t, err)
	table.SetPrimaryOrder(func(a, b *time.Time) bool { return a.Before(*b) })

	n := fillTable(table)

	r := table.Match(func(e *time.Time) bool { return true })

	assert.EqualValues(t, 10, r.Size())

	n = n.Add(-time.Hour)
	assert.NoError(t, r.Update(0, &n))

	var f time.Time
	assert.True(t, table.First(&f, func(e *time.Time) bool { return true }))

	assert.EqualValues(t, n, f)

	n = n.Add(time.Hour * 5)
	assert.Error(t, r.Update(0, &n))

}
