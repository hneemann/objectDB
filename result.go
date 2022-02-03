package objectDB

import "sort"

type Result[E Entity[E]] struct {
	table     *Table[E]
	data      []*E
	copyAvail []bool
	deepCopy  []E
}

func newResult[E Entity[E]](data []*E, table *Table[E]) *Result[E] {
	dc := make([]E, len(data))
	av := make([]bool, len(data))
	return &Result[E]{
		table:     table,
		data:      data,
		copyAvail: av,
		deepCopy:  dc,
	}
}

func (a *Result[E]) Len() int {
	return len(a.data)
}

func (a *Result[E]) Item(n int) E {
	if a.copyAvail[n] {
		return a.deepCopy[n]
	}
	e := (*a.data[n]).DeepCopy()
	a.deepCopy[n] = e
	a.copyAvail[n] = true
	return e
}

func (a *Result[E]) Delete(n int) error {
	removed, err := a.table.delete(a.data[n])
	if removed {
		copy(a.data[n:], a.data[n+1:])
		a.data[len(a.data)-1] = nil
		a.data = a.data[:len(a.data)-1]
	}
	return err
}

func (r *Result[E]) Order(less func(e1, e2 *E) bool) *Result[E] {
	so := make([]*E, len(r.data))
	copy(so, r.data)
	sort.Slice(so, func(i, j int) bool {
		return less(so[i], so[j])
	})
	return newResult(so, r.table)
}
