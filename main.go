package objectDB

import (
	"fmt"
	"time"
)

type Filter[E any] interface {
	Accept(e *E) bool
}

type Entity[E any] interface {
	DeepCopy() E
}

type DateEntity[E any] interface {
	Entity[E]
	GetDate() time.Time
}

type Table[E Entity[E]] struct {
	nameProvider NameProvider[E]
	persist      Persist[E]
	orderLess    func(e1, e2 *E) bool
	data         []*E
}

func (t *Table[E]) SetPrimaryOrder(less func(e1, e2 *E) bool) {
	t.orderLess = less
}

func (t *Table[E]) Insert(e E) error {
	deepCopy := e.DeepCopy()
	if t.orderLess == nil || len(t.data) == 0 || t.orderLess(t.data[len(t.data)-1], &deepCopy) {
		t.data = append(t.data, &deepCopy)
		return t.persistItem(&e)
	}

	for i, en := range t.data {
		if t.orderLess(&deepCopy, en) {
			t.data = append(t.data, &deepCopy)
			copy(t.data[i+1:], t.data[i:])
			t.data[i] = &deepCopy
			return t.persistItem(&deepCopy)
		}
	}

	panic("impossible insert state")
}

func (t *Table[E]) delete(e *E) (bool, error) {
	for i, en := range t.data {
		if e == en {
			copy(t.data[i:], t.data[i+1:])
			t.data[len(t.data)-1] = nil
			t.data = t.data[:len(t.data)-1]
			return true, t.persistItem(e)
		}
	}
	return false, fmt.Errorf("could not delete item: not found %v", *e)
}

func (t *Table[E]) All() *Result[E] {
	c := make([]*E, len(t.data))
	copy(c, t.data)
	return newResult(c, t)
}

func (t *Table[E]) Match(accept func(*E) bool) *Result[E] {
	var m []*E
	for _, en := range t.data {
		if accept(en) {
			m = append(m, en)
		}
	}
	return newResult(m, t)
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
	err := t.persist.Persist(name, p)
	if err != nil {
		return fmt.Errorf("could not persist entity: %w", err)
	}
	return nil
}

func New[E Entity[E]](nameProvider NameProvider[E], persist Persist[E]) (*Table[E], error) {
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
		data:         e,
	}, nil
}
