package objectDB

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
)

type NameProvider[E Entity[E]] interface {
	SameFile(e1, e2 *E) bool
	ToFile(e *E) string
}

func Monthly[E DateEntity[E]]() NameProvider[E] {
	return monthly[E]{}
}

type monthly[E DateEntity[E]] struct {
}

func (m monthly[E]) SameFile(e1, e2 *E) bool {
	d1 := (*e1).GetDate()
	d2 := (*e2).GetDate()
	return (d1.Year() == d2.Year()) && (d1.Month() == d2.Month())
}

func (m monthly[E]) ToFile(e *E) string {
	d := (*e).GetDate()
	return strconv.Itoa(d.Year()) + "_" + strconv.Itoa(int(d.Month()))
}

func SingleFile[E Entity[E]](filename string) NameProvider[E] {
	return singleFile[E]{filename: filename}
}

type singleFile[E Entity[E]] struct {
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
			return fmt.Errorf("could not remove jdon file: %w", err)
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
					log.Println("could not open json file")
					return nil, err
				}
				var items []*E
				err = json.Unmarshal(b, &items)
				if err != nil {
					log.Println("could not unmarshal json file")
					return nil, err
				}

				allItems = append(allItems, items...)
			}
		}
	}

	return allItems, nil
}
