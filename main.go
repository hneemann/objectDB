package objectDB

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

type Table[E any] struct {
	m            sync.Mutex
	nameProvider NameProvider[E]
	persist      Persist[E]
	orderLess    func(e1, e2 *E) bool
	deepCopy     func(dst *E, src *E)
	data         []*E
	version      int
}

func (t *Table[E]) SetPrimaryOrder(less func(e1, e2 *E) bool) {
	t.m.Lock()
	defer t.m.Unlock()

	sort.Slice(t.data, func(i, j int) bool {
		return less(t.data[i], t.data[j])
	})
	t.orderLess = less
	t.version++
}
func (t *Table[E]) Size() int {
	t.m.Lock()
	defer t.m.Unlock()

	return len(t.data)
}

func (t *Table[E]) Insert(e E) error {
	t.m.Lock()
	defer t.m.Unlock()

	var deepCopy E
	t.deepCopy(&deepCopy, &e)
	if t.orderLess == nil || len(t.data) == 0 || t.orderLess(t.data[len(t.data)-1], &deepCopy) {
		t.data = append(t.data, &deepCopy)
		t.version++
		return t.persistItem(&e)
	}

	for i, en := range t.data {
		if t.orderLess(&deepCopy, en) {
			t.data = append(t.data, &deepCopy)
			copy(t.data[i+1:], t.data[i:])
			t.data[i] = &deepCopy
			t.version++
			return t.persistItem(&deepCopy)
		}
	}

	return errors.New("impossible insert state")
}

func (t *Table[E]) delete(index int, version int) error {
	t.m.Lock()
	defer t.m.Unlock()

	if t.version != version {
		return fmt.Errorf("delete: table has changed")
	}

	e := t.data[index]
	copy(t.data[index:], t.data[index+1:])
	t.data[len(t.data)-1] = nil
	t.data = t.data[:len(t.data)-1]
	t.version++
	return t.persistItem(e)
}

func (t *Table[E]) update(index int, version int, e *E) error {
	t.m.Lock()
	defer t.m.Unlock()

	if t.version != version {
		return fmt.Errorf("update: table has changed")
	}

	if t.orderLess != nil {
		ok1 := index == 0 || t.orderLess(t.data[index-1], e)
		ok2 := index == len(t.data)-1 || t.orderLess(e, t.data[index+1])
		if !ok1 || !ok2 {
			return fmt.Errorf("update: order violation")
		}
	}
	t.deepCopy(t.data[index], e)

	return t.persistItem(e)
}

func (t *Table[E]) All(yield func(E) bool) {
	t.m.Lock()
	defer t.m.Unlock()

	for _, en := range t.data {
		var e E
		t.deepCopy(&e, en)
		if !yield(e) {
			break
		}
	}
}

func (t *Table[E]) Match(accept func(*E) bool) Result[E] {
	t.m.Lock()
	defer t.m.Unlock()

	var m []int
	for i, en := range t.data {
		if accept(en) {
			m = append(m, i)
		}
	}
	return newResult(m, t)
}

func (t *Table[E]) First(dst *E, accept func(*E) bool) bool {
	t.m.Lock()
	defer t.m.Unlock()

	for _, en := range t.data {
		if accept(en) {
			t.deepCopy(dst, en)
			return true
		}
	}
	return false
}

func (t *Table[E]) copy(dest *E, n, version int) error {
	t.m.Lock()
	defer t.m.Unlock()

	if n < 0 || n >= len(t.data) {
		return fmt.Errorf("copy: index out of range")
	}

	if t.version != version {
		return fmt.Errorf("copy: table has changed")
	}

	t.deepCopy(dest, t.data[n])

	return nil
}

func (t *Table[E]) persistItem(e *E) error {
	if t.persist == nil {
		return nil
	}

	var p []*E
	for _, en := range t.data {
		if t.nameProvider.SameFile(en, e) {
			p = append(p, en)
		}
	}
	name := t.nameProvider.ToFile(e)
	return t.persist.Persist(name, p)
}

func New[E any](nameProvider NameProvider[E], persist Persist[E], deepCopy func(dst *E, src *E)) (*Table[E], error) {
	if deepCopy == nil {
		deepCopy = func(dst *E, src *E) {
			*dst = *src
		}
	}

	var e []*E
	if persist != nil {
		var err error
		e, err = persist.Restore()
		if err != nil {
			return nil, fmt.Errorf("could not restore db: %w", err)
		}
	}
	return &Table[E]{
		nameProvider: nameProvider,
		persist:      persist,
		deepCopy:     deepCopy,
		data:         e,
	}, nil
}
