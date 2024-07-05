package objectDB

import (
	"objectDB/serialize"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var myMonthly = Monthly[time.Time]("test", func(t *time.Time) time.Time {
	return *t
})

func add(ti time.Time, h int) *time.Time {
	t := ti.Add(time.Hour * time.Duration(h))
	return &t
}

func fillTable(table *Table[time.Time]) time.Time {
	n := time.Now()
	table.Insert(add(n, 5))
	table.Insert(add(n, 8))
	table.Insert(add(n, 7))
	table.Insert(add(n, 2))
	table.Insert(add(n, 1))
	table.Insert(add(n, 0))
	table.Insert(add(n, 4))
	table.Insert(add(n, 3))
	table.Insert(add(n, 9))
	table.Insert(add(n, 6))
	return n
}

func TestSimple(t *testing.T) {
	table, err := New[time.Time](myMonthly, PersistJSON[time.Time]("testdata", "_db.json"), nil, nil)
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
	table, err := New[time.Time](myMonthly, PersistJSON[time.Time]("testdata", "_db.json"), nil, nil)
	assert.NoError(t, err)
	n := time.Now()

	table.Insert(add(n, -24*30))
	table.Insert(add(n, 0))
	table.Insert(add(n, 24*30))

	table2, err := New[time.Time](myMonthly, PersistJSON[time.Time]("testdata", "_db.json"), nil, nil)
	a := table2.Match(func(e *time.Time) bool { return true })
	assert.EqualValues(t, 3, a.Size())
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
}

func TestStorageSerializer(t *testing.T) {
	table, err := New[time.Time](myMonthly, PersistSerializer[time.Time]("testdata", "_db.bin", serialize.New()), nil, nil)
	assert.NoError(t, err)
	n := time.Now()

	table.Insert(add(n, -24*30))
	table.Insert(add(n, 0))
	table.Insert(add(n, 24*30))

	table2, err := New[time.Time](myMonthly, PersistSerializer[time.Time]("testdata", "_db.bin", serialize.New()), nil, nil)
	a := table2.Match(func(e *time.Time) bool { return true })
	assert.EqualValues(t, 3, a.Size())
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
}

func TestInsert(t *testing.T) {
	table, err := New[time.Time](myMonthly, nil, nil, func(a, b *time.Time) bool { return a.Before(*b) })
	assert.NoError(t, err)

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
	table, err := New[time.Time](myMonthly, nil, nil, func(a, b *time.Time) bool { return a.Before(*b) })
	assert.NoError(t, err)

	n := fillTable(table)

	var found time.Time
	assert.True(t, table.First(&found, func(e *time.Time) bool { return true }))
	assert.EqualValues(t, n, found)
}

func TestAll(t *testing.T) {
	table, err := New[time.Time](myMonthly, nil, nil, func(a, b *time.Time) bool { return a.Before(*b) })
	assert.NoError(t, err)

	n := fillTable(table)

	assert.EqualValues(t, 10, table.Size())
	var i int
	for e := range table.All {
		assert.EqualValues(t, n.Add(time.Hour*time.Duration(i)), *e)
		i++
	}

}

func TestUpdate(t *testing.T) {
	table, err := New[time.Time](myMonthly, nil, nil, func(a, b *time.Time) bool { return a.Before(*b) })
	assert.NoError(t, err)

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

func TestStorageSerializerDelay(t *testing.T) {
	table, err := New[time.Time](myMonthly, PersistSerializer[time.Time]("testdata", "_db.bin", serialize.New()), nil, nil)
	assert.NoError(t, err)
	table.SetWriteDelay(2)

	// add some vales
	n := time.Now()
	table.Insert(add(n, 0))
	table.Insert(add(n, 1))
	table.Insert(add(n, 2))

	// folder still empty
	files, err := os.ReadDir("testdata")
	assert.NoError(t, err)
	assert.EqualValues(t, 0, len(files))

	// wait
	time.Sleep(5 * time.Second)

	// folder contains a file
	files, err = os.ReadDir("testdata")
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(files))

	// delete entries
	a := table.Match(func(e *time.Time) bool { return true })
	assert.EqualValues(t, 3, a.Size())
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))

	// folder contains still a file
	files, err = os.ReadDir("testdata")
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(files))

	// wait again
	time.Sleep(5 * time.Second)

	// folder empty
	files, err = os.ReadDir("testdata")
	assert.NoError(t, err)
	assert.EqualValues(t, 0, len(files))
}

func TestStorageSerializerDelayShutdown(t *testing.T) {
	table, err := New[time.Time](myMonthly, PersistSerializer[time.Time]("testdata", "_db.bin", serialize.New()), nil, nil)
	assert.NoError(t, err)
	table.SetWriteDelay(2)

	// add some vales
	n := time.Now()
	table.Insert(add(n, 0))
	table.Insert(add(n, 1))
	table.Insert(add(n, 2))

	table.Shutdown()

	// folder contains a file
	files, err := os.ReadDir("testdata")
	assert.NoError(t, err)
	assert.EqualValues(t, 1, len(files))

	// delete entries
	a := table.Match(func(e *time.Time) bool { return true })
	assert.EqualValues(t, 3, a.Size())
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))
	assert.NoError(t, a.Delete(0))

	// folder empty
	files, err = os.ReadDir("testdata")
	assert.NoError(t, err)
	assert.EqualValues(t, 0, len(files))
}
