package objectDB

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/hneemann/objectDB/serialize"
	"io"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

// NameProvider is an interface to provide a name for a file based on the object.
type NameProvider[E any] interface {
	// SameFile returns true if the two objects should be stored in the same file.
	SameFile(e1, e2 *E) bool
	// ToFile returns the name of the file where the object should be stored.
	// If SameFile(e1, e2) is true, ToFile(e1) has to return the same as ToFile(e2).
	ToFile(e *E) string
}

// Monthly returns a NameProvider that stores objects in monthly files.
// The prefix is added to the file name.
func Monthly[E any](prefix string, dateFunc func(*E) time.Time) NameProvider[E] {
	if prefix != "" {
		prefix += "_"
	}
	return monthly[E]{dateFunc: dateFunc, prefix: prefix}
}

type monthly[E any] struct {
	dateFunc func(*E) time.Time
	prefix   string
}

func (m monthly[E]) SameFile(e1, e2 *E) bool {
	d1 := m.dateFunc(e1)
	d2 := m.dateFunc(e2)
	return (d1.Year() == d2.Year()) && (d1.Month() == d2.Month())
}

func (m monthly[E]) ToFile(e *E) string {
	d := m.dateFunc(e)
	mo := int(d.Month())
	if mo < 10 {
		return m.prefix + strconv.Itoa(d.Year()) + "_0" + strconv.Itoa(mo)
	}
	return m.prefix + strconv.Itoa(d.Year()) + "_" + strconv.Itoa(mo)
}

// SingleFile returns a NameProvider that stores all objects in the same file.
func SingleFile[E any](filename string) NameProvider[E] {
	return singleFile[E]{filename: filename}
}

type singleFile[E any] struct {
	filename string
}

func (s singleFile[E]) SameFile(*E, *E) bool {
	return true
}

func (s singleFile[E]) ToFile(*E) string {
	return s.filename
}

// Persist is an interface to persist and restore objects.
type Persist[E any] interface {
	// Persist stores the objects in a file.
	Persist(name string, items []*E) error
	// Restore reads all available objects
	Restore() ([]*E, error)
}

// PersistJSON returns a Persist that stores objects in JSON format.
func PersistJSON[E any](baseFolder, suffix string) Persist[E] {
	return persistJson[E]{
		baseFolder: baseFolder,
		suffix:     suffix,
	}
}

type persistJson[E any] struct {
	baseFolder string
	suffix     string
}

func (p persistJson[E]) Persist(dbFile string, items []*E) error {
	log.Println("persist", dbFile)
	filePath := path.Join(p.baseFolder, dbFile+p.suffix)
	if len(items) == 0 {
		err := os.Remove(filePath)
		if err != nil {
			return fmt.Errorf("could not remove json file: %w", err)
		}
	} else {
		b, err := json.Marshal(items)
		if err != nil {
			return fmt.Errorf("could not marshal json: %w", err)
		}
		err = os.WriteFile(filePath, b, 0644)
		if err != nil {
			return fmt.Errorf("could not write file: %w", err)
		}
	}
	return nil
}

func (p persistJson[E]) Restore() ([]*E, error) {
	dir, err := os.Open(p.baseFolder)
	if err != nil {
		return nil, fmt.Errorf("could not open base folder: %w", err)
	}
	names, err := dir.ReadDir(-1)
	if err != nil {
		return nil, fmt.Errorf("could not scan base folder: %w", err)
	}
	err = dir.Close()
	if err != nil {
		return nil, fmt.Errorf("could not close base folder: %w", err)
	}

	var allItems []*E
	for _, n := range names {
		name := n.Name()
		if strings.HasSuffix(name, p.suffix) {
			items, err2 := p.readFile(name)
			if err2 != nil {
				return nil, err2
			}
			allItems = append(allItems, items...)
		}
	}

	return allItems, nil
}

func LogClose(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Println("could not close:", err)
	}
}

func (p persistJson[E]) readFile(name string) ([]*E, error) {
	jsonFile := path.Join(p.baseFolder, name)
	log.Println("read", name)
	f, err := os.Open(jsonFile)
	if err != nil {
		return nil, fmt.Errorf("could not open json file: %w", err)
	}
	defer LogClose(f)

	b, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("could not read json file: %w", err)
	}
	var items []*E
	err = json.Unmarshal(b, &items)
	if err != nil {
		return nil, fmt.Errorf("could not unmarshal json file: %w", err)
	}

	return items, nil
}

// PersistSerializer returns a Persist that stores objects in binary format. It
// is able to persist and restore interfaces. To do that the interface has to be
// registered with serialize.Register.
func PersistSerializer[E any](baseFolder, suffix string, serializer *serialize.Serializer) Persist[E] {
	return &persistSerializer[E]{
		baseFolder: baseFolder,
		suffix:     suffix,
		serializer: serializer,
	}
}

type persistSerializer[E any] struct {
	baseFolder string
	suffix     string
	serializer *serialize.Serializer
}

func (p *persistSerializer[E]) Persist(dbFile string, items []*E) error {
	log.Println("persist", dbFile)
	filePath := path.Join(p.baseFolder, dbFile+p.suffix)
	if len(items) == 0 {
		err := os.Remove(filePath)
		if err != nil {
			return fmt.Errorf("could not remove bin file: %w", err)
		}
	} else {
		f, err := os.Create(filePath)
		if err != nil {
			return fmt.Errorf("could not create file: %w", err)
		}
		defer LogClose(f)
		buf := bufio.NewWriter(f)
		defer buf.Flush()
		err = p.serializer.Write(buf, items)
		if err != nil {
			return fmt.Errorf("could not serialize data: %w", err)
		}
	}
	return nil
}

func (p *persistSerializer[E]) Restore() ([]*E, error) {
	dir, err := os.Open(p.baseFolder)
	if err != nil {
		return nil, fmt.Errorf("could not open base folder: %w", err)
	}
	names, err := dir.ReadDir(-1)
	if err != nil {
		return nil, fmt.Errorf("could not scan base folder: %w", err)
	}
	err = dir.Close()
	if err != nil {
		return nil, fmt.Errorf("could not close base folder: %w", err)
	}

	var allItems []*E

	for _, n := range names {
		name := n.Name()
		if strings.HasSuffix(name, p.suffix) {
			items, err := p.readFile(name)
			if err != nil {
				return nil, err
			}

			allItems = append(allItems, items...)
		}
	}

	return allItems, nil
}

func (p *persistSerializer[E]) readFile(name string) ([]*E, error) {
	binFile := path.Join(p.baseFolder, name)
	log.Println("read", name)

	f, err := os.Open(binFile)
	if err != nil {
		return nil, fmt.Errorf("could not open bin file: %w", err)
	}
	defer LogClose(f)

	var items []*E
	err = p.serializer.Read(bufio.NewReader(f), &items)
	if err != nil {
		return nil, fmt.Errorf("could not read bin file: %w", err)
	}
	return items, nil
}
