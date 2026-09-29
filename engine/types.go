package engine

import (
	"fmt"
	"sort"
)

type Type string

const (
	TypeNull   Type = "NULL"
	TypeInt    Type = "INT"
	TypeFloat  Type = "FLOAT"
	TypeString Type = "STRING"
	TypeBool   Type = "BOOL"
)

type Column struct {
	Name string
	Type Type
}

type Schema []Column

func (s Schema) Index(name string) (int, bool) {
	for i := range s {
		if s[i].Name == name {
			return i, true
		}
	}
	return 0, false
}

func concatSchema(left, right Schema) Schema {
	out := make(Schema, 0, len(left)+len(right))
	out = append(out, left...)
	out = append(out, right...)
	return out
}

type Row []any

type Table struct {
	Name    string
	Columns Schema
	Rows    []Row
}

func (t Table) validate() error {
	for rowIndex, row := range t.Rows {
		if len(row) != len(t.Columns) {
			return fmt.Errorf("table %s row %d has %d values, schema has %d columns", t.Name, rowIndex, len(row), len(t.Columns))
		}
		for i, value := range row {
			if value == nil {
				continue
			}
			switch t.Columns[i].Type {
			case TypeInt:
				if _, ok := value.(int64); !ok {
					return fmt.Errorf("table %s row %d column %s expects INT", t.Name, rowIndex, t.Columns[i].Name)
				}
			case TypeFloat:
				if _, ok := value.(float64); !ok {
					return fmt.Errorf("table %s row %d column %s expects FLOAT", t.Name, rowIndex, t.Columns[i].Name)
				}
			case TypeString:
				if _, ok := value.(string); !ok {
					return fmt.Errorf("table %s row %d column %s expects STRING", t.Name, rowIndex, t.Columns[i].Name)
				}
			case TypeBool:
				if _, ok := value.(bool); !ok {
					return fmt.Errorf("table %s row %d column %s expects BOOL", t.Name, rowIndex, t.Columns[i].Name)
				}
			}
		}
	}
	return nil
}

type Database map[string]Table

func (d Database) Table(name string) (Table, error) {
	table, ok := d[name]
	if !ok {
		return Table{}, fmt.Errorf("unknown table %q", name)
	}
	return table, nil
}

func sortRows(rows []Row) {
	sort.SliceStable(rows, func(i, j int) bool { return compareValues(rows[i], rows[j]) < 0 })
}

func SortRows(rows []Row) { sortRows(rows) }

func compareValues(left, right any) int {
	ls := valueSortKey(left)
	rs := valueSortKey(right)
	if ls != rs {
		if ls < rs {
			return -1
		}
		return 1
	}
	return 0
}

func valueSortKey(value any) string {
	if value == nil {
		return "0:"
	}
	switch value := value.(type) {
	case bool:
		if value {
			return "1:true"
		}
		return "1:false"
	case int64:
		return fmt.Sprintf("2:%d", value)
	case float64:
		return fmt.Sprintf("3:%.17g", value)
	case string:
		return "4:" + value
	default:
		return "5:" + fmt.Sprint(value)
	}
}
