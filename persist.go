package objectDB

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"objectDB/serialize"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

type NameProvider[E any] interface {
	SameFile(e1, e2 *E) bool
	ToFile(e *E) string
}

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

func SingleFile[E any](filename string) NameProvider[E] {
	return singleFile[E]{filename: filename}
}

type singleFile[E any] struct {
	filename string
}

func (s singleFile[E]) SameFile(e1, e2 *E) bool {
	return true
}

func (s singleFile[E]) ToFile(e *E) string {
	return s.filename
}

type Persist[E any] interface {
	Persist(name string, items []*E) error
	Restore() ([]*E, error)
}

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
		err = ioutil.WriteFile(filePath, b, 0644)
		if err != nil {
			return fmt.Errorf("could not write file: %w", err)
		}
	}
	return nil
}

func (p persistJson[E]) Restore() ([]*E, error) {
	dir, err := os.Open(p.baseFolder)
	if err != nil {
		return []*E{}, fmt.Errorf("could not open base folder: %w", err)
	}
	names, err := dir.ReadDir(-1)
	if err != nil {
		return []*E{}, fmt.Errorf("could not scan base folder: %w", err)
	}
	err = dir.Close()
	if err != nil {
		return []*E{}, fmt.Errorf("could not close base folder: %w", err)
	}

	var allItems []*E

	for _, n := range names {
		if strings.HasSuffix(n.Name(), p.suffix) {
			jsonFile := path.Join(p.baseFolder, n.Name())
			log.Println("read " + jsonFile)

			f, err := os.Open(jsonFile)
			if err == nil {
				defer f.Close()

				b, err := ioutil.ReadAll(f)
				if err != nil {
					return nil, fmt.Errorf("could not open json file: %w", err)
				}
				var items []*E
				err = json.Unmarshal(b, &items)
				if err != nil {
					log.Println("could not unmarshal json file")
					return nil, fmt.Errorf("could not unmarshal json file: %w", err)
				}

				allItems = append(allItems, items...)
			}
		}
	}

	return allItems, nil
}

func PersistSerializer[E any](baseFolder, suffix string, serializer *serialize.Serializer) Persist[E] {
	return persistSerializer[E]{
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

func (p persistSerializer[E]) Persist(dbFile string, items []*E) error {
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
		defer f.Close()
		buf := bufio.NewWriter(f)
		defer buf.Flush()
		err = p.serializer.Write(buf, items)
		if err != nil {
			return fmt.Errorf("could not serialize data: %w", err)
		}
	}
	return nil
}

func (p persistSerializer[E]) Restore() ([]*E, error) {
	dir, err := os.Open(p.baseFolder)
	if err != nil {
		return []*E{}, fmt.Errorf("could not open base folder: %w", err)
	}
	names, err := dir.ReadDir(-1)
	if err != nil {
		return []*E{}, fmt.Errorf("could not scan base folder: %w", err)
	}
	err = dir.Close()
	if err != nil {
		return []*E{}, fmt.Errorf("could not close base folder: %w", err)
	}

	var allItems []*E

	for _, n := range names {
		if strings.HasSuffix(n.Name(), p.suffix) {
			binFile := path.Join(p.baseFolder, n.Name())
			log.Println("read " + binFile)

			f, err := os.Open(binFile)
			if err == nil {
				defer f.Close()

				var items []*E
				err := p.serializer.Read(bufio.NewReader(f), &items)
				if err != nil {
					return nil, fmt.Errorf("could not read bin file: %w", err)
				}

				allItems = append(allItems, items...)
			}
		}
	}

	return allItems, nil
}
