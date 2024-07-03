package objectDB

import (
	"fmt"
)

type Result[E any] struct {
	table      *Table[E]
	tableIndex []int
	version    int
}

func newResult[E any](tableIndex []int, table *Table[E]) Result[E] {
	return Result[E]{
		table:      table,
		tableIndex: tableIndex,
		version:    table.version,
	}
}

func (r *Result[E]) Size() int {
	return len(r.tableIndex)
}

func (r *Result[E]) Iter(yield func(*E, error) bool) {
	var err error
	var e E
	for _, n := range r.tableIndex {
		err = r.table.copy(&e, n, r.version)
		if !yield(&e, err) {
			break
		}
		if err != nil {
			break
		}
	}
}

func (r *Result[E]) Get(dst *E, n int) error {
	if n < 0 || n >= len(r.tableIndex) {
		return fmt.Errorf("item: index out of range")
	}

	return r.table.copy(dst, r.tableIndex[n], r.version)
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

func (r *Result[E]) Order(less func(e1, e2 *E) bool) (Result[E], error) {
	so, err := r.table.order(r.tableIndex, less, r.version)
	if err != nil {
		return Result[E]{}, err
	}
	return Result[E]{
		table:      r.table,
		tableIndex: so,
		version:    r.version,
	}, nil
}
