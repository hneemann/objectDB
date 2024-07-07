package objectDB

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"
)

type Table[E any] struct {
	m            sync.Mutex
	nameProvider NameProvider[E]
	persist      Persist[E]
	orderLess    func(e1, e2 *E) bool
	deepCopy     func(dst *E, src *E)
	data         []*E
	version      int
	delayedWrite *delayHandler[E]
}

// Size returns the number of elements in the table.
func (t *Table[E]) Size() int {
	t.m.Lock()
	defer t.m.Unlock()

	return len(t.data)
}

// Insert adds a new element to the table.
func (t *Table[E]) Insert(e *E) error {
	t.m.Lock()
	defer t.m.Unlock()

	var deepCopy E
	t.deepCopy(&deepCopy, e)
	if t.orderLess == nil || len(t.data) == 0 || (t.orderLess != nil && t.orderLess(t.data[len(t.data)-1], &deepCopy)) {
		t.data = append(t.data, &deepCopy)
		t.version++
		return t.persistItem(&deepCopy)
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

// All calls the yield function for each element in the table. No long-running
// operations should be done in the yield function, as the table is locked during
// the call. The elements are deep copied before the yield function is called.
func (t *Table[E]) All(yield func(*E) bool) {
	t.m.Lock()
	defer t.m.Unlock()

	for _, en := range t.data {
		var e E
		t.deepCopy(&e, en)
		if !yield(&e) {
			break
		}
	}
}

// Match returns a Result that contains all elements that match the accept
// function. For performance reasons, the accept function is called with the not
// yet deep copied elements. So the accept function is not allowed to modify the
// elements. No long-running operations should be done in the accept function,
// because the table is locked during the call.
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

// First returns the first element that matches the accept function. For
// performance reasons, the accept function is called with the not yet deep
// copied elements. So the accept function is not allowed to modify the elements.
// No long-running operations should be done in the accept function, because the
// table is locked during the call.
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

	if t.delayedWrite == nil {
		var p []*E
		for _, en := range t.data {
			if t.nameProvider.SameFile(en, e) {
				p = append(p, en)
			}
		}
		name := t.nameProvider.ToFile(e)
		return t.persist.Persist(name, p)
	} else {
		return t.delayedWrite.modified(t.nameProvider.ToFile(e))
	}
}

func (t *Table[E]) order(tableIndex []int, less func(e1, e2 *E) bool, version int) ([]int, error) {
	t.m.Lock()
	defer t.m.Unlock()

	if t.version != version {
		return nil, fmt.Errorf("order: table has changed")
	}

	so := make([]int, len(tableIndex))
	copy(so, tableIndex)
	sort.Slice(so, func(i, j int) bool {
		return less(t.data[so[i]], t.data[so[j]])
	})
	return so, nil
}

// SetWriteDelay sets the delay in seconds for persisting changes to disk. If sec
// is 0, changes are written immediately. This is the default. If sec is greater
// than 0, changes are written after sec seconds of inactivity. If this method is
// called, the Shutdown method must be called before the program exits, otherwise
// changes may be lost.
func (t *Table[E]) SetWriteDelay(sec int) {
	t.m.Lock()
	defer t.m.Unlock()

	if t.delayedWrite != nil {
		t.delayedWrite.shutdown()
		t.delayedWrite = nil
	}

	if sec > 0 {
		t.delayedWrite = newDelayHandler[E](t, sec)
	}
}

func (t *Table[E]) writeFiles(name string) error {
	t.m.Lock()
	defer t.m.Unlock()

	list := make([]*E, 0)
	for _, en := range t.data {
		if t.nameProvider.ToFile(en) == name {
			list = append(list, en)
		}
	}
	return t.persist.Persist(name, list)
}

// Shutdown must be called before the program exits, if write delay was used,
// otherwise changes may be lost. It waits until all changes are written to disk.
// If the write delay was not used, this method does nothing. After this method
// is called, the table is still usable, but changes are written immediately.
func (t *Table[E]) Shutdown() {
	log.Println("shutdown table")
	t.m.Lock()
	dw := t.delayedWrite
	t.delayedWrite = nil
	t.m.Unlock()

	if dw != nil {
		dw.shutdown()
	}
	log.Println("table shutdown completed")
}

type delayHandler[E any] struct {
	m         sync.Mutex
	table     *Table[E]
	sec       int
	nameMap   map[string]time.Time
	lastError error
	done      chan struct{}
	ack       chan struct{}
}

func newDelayHandler[E any](table *Table[E], sec int) *delayHandler[E] {
	done := make(chan struct{})
	ack := make(chan struct{})
	dh := &delayHandler[E]{
		table:   table,
		sec:     sec,
		nameMap: make(map[string]time.Time),
		done:    done,
		ack:     ack,
	}
	go func() {
		for {
			select {
			case <-time.After(time.Second * time.Duration(sec)):
				names := dh.getModifiedNameList()
				for _, name := range names {
					err := dh.table.writeFiles(name)
					dh.written(name, err)
				}
			case <-done:
				close(ack)
				return
			}
		}
	}()

	return dh
}

func (h *delayHandler[E]) modified(file string) error {
	h.m.Lock()
	defer h.m.Unlock()

	h.nameMap[file] = time.Now().Add(time.Second * time.Duration(h.sec))
	if h.lastError != nil {
		err := h.lastError
		h.lastError = nil
		return err
	}
	return nil
}

func (h *delayHandler[E]) getModifiedNameList() []string {
	h.m.Lock()
	defer h.m.Unlock()

	now := time.Now()
	var names []string
	for name, t := range h.nameMap {
		if now.After(t) {
			names = append(names, name)
		}
	}
	return names
}

func (h *delayHandler[E]) written(name string, err error) {
	h.m.Lock()
	defer h.m.Unlock()

	if err != nil {
		h.lastError = err
	} else {
		delete(h.nameMap, name)
	}
}

func (h *delayHandler[E]) shutdown() {
	close(h.done)
	<-h.ack

	for name := range h.nameMap {
		err := h.table.writeFiles(name)
		if err != nil {
			log.Println(err)
		}
	}
}

// New creates a new Table. The nameProvider is used to create a file name for
// each element. The persist parameter is used to store the data on disk. The
// deepCopy function is used to create a deep copy of an element. If nil, a
// simple copy is used. The less function is used to sort the elements. If nil,
// no sorting is done.
func New[E any](nameProvider NameProvider[E], persist Persist[E], deepCopy func(dst *E, src *E), less func(e1, e2 *E) bool) (*Table[E], error) {
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
	if less != nil {
		sort.Slice(e, func(i, j int) bool {
			return less(e[i], e[j])
		})
	}

	return &Table[E]{
		nameProvider: nameProvider,
		persist:      persist,
		deepCopy:     deepCopy,
		orderLess:    less,
		data:         e,
	}, nil
}
