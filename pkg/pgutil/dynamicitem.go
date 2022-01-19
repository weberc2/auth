package pgutil

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Value interface {
	CompareValue(Value) error
	Pointer() interface{}
	Value() interface{}
}

type ValueType int

const (
	ValueTypeInvalid ValueType = -1
	ValueTypeString  ValueType = iota
	ValueTypeInteger
	ValueTypeTime
)

func ValueTypeFromColumnType(columnType string) (ValueType, error) {
	switch columnType {
	case "TEXT":
		return ValueTypeString, nil
	case "INTEGER":
		return ValueTypeInteger, nil
	case "TIMESTAMP", "TIMESTAMPTZ":
		return ValueTypeTime, nil
	default:
		if _, err := parseVarChar(columnType); err == nil {
			return ValueTypeString, nil
		}
		return ValueTypeInvalid, fmt.Errorf("unsupported column type: %s", columnType)
	}
}

type String string

func (s *String) Value() interface{} {
	if s == nil {
		return nil
	}
	return string(*s)
}

func (s *String) Pointer() interface{} { return (*string)(s) }
func (s *String) CompareValue(found Value) error {
	if found, ok := found.(*String); ok {
		if s == found {
			return nil
		}
		if s != nil && found == nil {
			return fmt.Errorf("wanted `%s`; found `nil`", *s)
		}
		if s == nil && found != nil {
			return fmt.Errorf("wanted `nil`; found `%s`", *found)
		}
		if *s != *found {
			return fmt.Errorf("wanted `%s`; found `%s`", *s, *found)
		}
		return nil
	}
	return fmt.Errorf("wanted type `String`; found `%T`", found)
}

type Integer int

func (i *Integer) Value() interface{} {
	if i == nil {
		return nil
	}
	return int(*i)
}

func (i *Integer) Pointer() interface{} { return (*int)(i) }
func (i *Integer) CompareValue(found Value) error {
	if found, ok := found.(*Integer); ok {
		if i == found {
			return nil
		}
		if i != nil && found == nil {
			return fmt.Errorf("wanted `%d`; found `nil`", *i)
		}
		if i == nil && found != nil {
			return fmt.Errorf("wanted `nil`; found `%d`", *found)
		}
		if *i != *found {
			return fmt.Errorf("wanted `%d`; found `%d`", *i, *found)
		}
		return nil
	}
	return fmt.Errorf("wanted type `Integer`; found `%T`", found)
}

type Time time.Time

func (t *Time) Value() interface{} {
	if t == nil {
		return nil
	}
	return time.Time(*t)
}

func (t *Time) Pointer() interface{} { return (*time.Time)(t) }
func (t *Time) CompareValue(found Value) error {
	if found, ok := found.(*Time); ok {
		if t == found {
			return nil
		}
		if t != nil && found == nil {
			return fmt.Errorf("wanted `%v`; found `nil`", *t)
		}
		if t == nil && found != nil {
			return fmt.Errorf("wanted `nil`; found `%v`", *found)
		}
		if !time.Time(*t).Equal(time.Time(*found)) {
			return fmt.Errorf("wanted `%v`; found `%v`", *t, *found)
		}
		return nil
	}
	return fmt.Errorf("wanted type `Time`; found `%T`", found)
}

func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time.Format(time.Time(t), time.RFC3339))
}

func NewString(s string) *String { return (*String)(&s) }
func NewInteger(i int) *Integer  { return (*Integer)(&i) }
func NewTime(t time.Time) *Time  { return (*Time)(&t) }
func NilString() Value           { return new(String) }
func NilInteger() Value          { return new(Integer) }
func NilTime() Value             { return new(Time) }

func DynamicItemFactory(values ...func() Value) func() DynamicItem {
	return func() DynamicItem {
		item := make(DynamicItem, len(values))
		for i := range values {
			item[i] = values[i]()
		}
		return item
	}
}

func NilValueFuncFromColumnType(columnType string) (func() Value, error) {
	valueType, err := ValueTypeFromColumnType(columnType)
	if err != nil {
		return nil, err
	}
	switch valueType {
	case ValueTypeString:
		return NilString, nil
	case ValueTypeInteger:
		return NilInteger, nil
	case ValueTypeTime:
		return NilTime, nil
	default:
		panic(fmt.Sprintf("invalid value type: %d", valueType))
	}
}

func EmptyDynamicItemFromColumns(columns []Column) (DynamicItem, error) {
	item := make(DynamicItem, len(columns))
	for i, c := range columns {
		f, err := NilValueFuncFromColumnType(c.Type)
		if err != nil {
			return nil, fmt.Errorf("column `%s`: %w", c.Name, err)
		}
		item[i] = f()
	}
	return item, nil
}

func DynamicItemFactoryFromColumns(
	columns ...Column,
) (func() DynamicItem, error) {
	valueFuncs := make([]func() Value, len(columns))
	for i, c := range columns {
		f, err := NilValueFuncFromColumnType(c.Type)
		if err != nil {
			return nil, fmt.Errorf("column `%s`: %w", c.Name, err)
		}
		valueFuncs[i] = f
	}
	return DynamicItemFactory(valueFuncs...), nil
}

func parseVarChar(s string) (int, error) {
	if strings.HasPrefix(s, "VARCHAR(") && s[len(s)-1] == ')' {
		if i, err := strconv.Atoi(s[len("VARCHAR(") : len(s)-1]); err == nil {
			return i, nil
		}
	}
	return 0, fmt.Errorf("wanted `VARCHAR(<int>)`; found `%s`", s)
}

type DynamicItem []Value

func (di DynamicItem) Scan(pointers []interface{}) {
	for i := range di {
		if di[i] != nil {
			pointers[i] = di[i].Pointer()
		}
	}
}

func (di DynamicItem) Values(values []interface{}) {
	for i := range di {
		if di[i] != nil {
			values[i] = di[i].Value()
		}
	}
}

func (di DynamicItem) ID() interface{} { return di[idColumnPosition] }

func (wanted DynamicItem) Compare(found DynamicItem) error {
	if len(wanted) != len(found) {
		return fmt.Errorf(
			"len(DynamicItem): wanted `%d`; found `%d`",
			len(wanted),
			len(found),
		)
	}
	for i := range wanted {
		if err := wanted[i].CompareValue(found[i]); err != nil {
			return fmt.Errorf("column %d: %w", i, err)
		}
	}
	return nil
}

func CompareDynamicItems(wanted, found []DynamicItem) error {
	if len(wanted) != len(found) {
		return fmt.Errorf(
			"len([]DynamicItem): wanted `%d`; found `%d`",
			len(wanted),
			len(found),
		)
	}
	for i := range wanted {
		if err := wanted[i].Compare(found[i]); err != nil {
			return fmt.Errorf("row %d: %w", i, err)
		}
	}
	return nil
}

func (r *Result) ToDynamicItems(
	newItem func() DynamicItem,
) ([]DynamicItem, error) {
	var items []DynamicItem
	for r.Next() {
		item := newItem()
		if err := r.Scan(item); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}
