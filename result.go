package objectDB

import (
	"fmt"
	"sort"
)

type Result[E any] struct {
	table      *Table[E]
	tableIndex []int
	copyAvail  []bool
	deepCopy   []E
	version    int
}

func newResult[E any](tableIndex []int, table *Table[E]) *Result[E] {
	dc := make([]E, len(tableIndex))
	av := make([]bool, len(tableIndex))
	return &Result[E]{
		table:      table,
		tableIndex: tableIndex,
		copyAvail:  av,
		deepCopy:   dc,
		version:    table.version,
	}
}

func (r *Result[E]) Len() int {
	return len(r.tableIndex)
}

func (r *Result[E]) Item(n int) (*E, error) {
	if r.copyAvail[n] {
		return &r.deepCopy[n], nil
	}
	if r.table.version != r.version {
		return nil, fmt.Errorf("item: table has changed")
	}
	r.table.deepCopy(&(r.deepCopy[n]), r.table.data[r.tableIndex[n]])
	r.copyAvail[n] = true
	return &r.deepCopy[n], nil
}

func (r *Result[E]) Delete(n int) error {
	tableIndex := r.tableIndex[n]
	err := r.table.delete(tableIndex, r.version)
	if err == nil {
		r.version++
		copy(r.tableIndex[n:], r.tableIndex[n+1:])
		r.tableIndex = r.tableIndex[:len(r.tableIndex)-1]
		for i := range r.tableIndex {
			if r.tableIndex[i] > tableIndex {
				r.tableIndex[i]--
			}
		}
	}
	return err
}

func (r *Result[E]) Update(n int, e *E) error {
	return r.table.update(r.tableIndex[n], r.version, e)
}

func (r *Result[E]) Order(less func(e1, e2 *E) bool) (*Result[E], error) {
	if r.table.version != r.version {
		return nil, fmt.Errorf("order: table has changed")
	}

	so := make([]int, len(r.tableIndex))
	copy(so, r.tableIndex)
	sort.Slice(so, func(i, j int) bool {
		return less(r.table.data[so[i]], r.table.data[so[j]])
	})
	return newResult(so, r.table), nil
}
