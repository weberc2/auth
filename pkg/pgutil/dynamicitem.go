package pgutil

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Value represents a scalar value in a table row.
type Value interface {
	// CompareValue compares the current value with another value. It returns
	// an error if the values are not the same, otherwise `nil`.
	CompareValue(Value) error

	// Pointer returns a pointer to the value which allows reflect-based
	// utilities to write to it.
	Pointer() interface{}

	// Value returns the underlying value as a type which can be understood by
	// `Exec()`, `Query()`, etc functions in the `database/sql` package (e.g.,
	// `int`, `string`, `time.Time`, etc).
	Value() interface{}
}

// ValueType denotes the type of a given `Value`.
type ValueType int

const (
	// ValueTypeInvalid represents an invalid value type. It's typically only
	// used when a function (with a `(ValueType, error)` return type) must
	// return an error.
	ValueTypeInvalid ValueType = -1

	// ValueTypeString is the ValueType for String values.
	ValueTypeString ValueType = iota

	// ValueTypeInteger is the ValueType for Integer values.
	ValueTypeInteger

	// ValueTypeTime is the ValueType for Time values.
	ValueTypeTime
)

// ValueTypeFromColumnType returns the ValueType which corresponds to a given
// column type. If the column type isn't supported, an error is returned.
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
		return ValueTypeInvalid, fmt.Errorf(
			"unsupported column type: %s",
			columnType,
		)
	}
}

// String represents a string Value.
type String string

// Value returns the underlying data as a `string`.
func (s *String) Value() interface{} {
	if s == nil {
		return nil
	}
	return string(*s)
}

// Pointer returns a pointer to the underlying `string`-typed data.
func (s *String) Pointer() interface{} { return (*string)(s) }

// CompareValue compares the `String` with other values. If the other value is
// not a `*String` with the same value, an error is returned.
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

// Integer represents an integer value.
type Integer int

// Value returns the underlying data as an `int`.
func (i *Integer) Value() interface{} {
	if i == nil {
		return nil
	}
	return int(*i)
}

// Pointer returns a pointer to the underlying `int`-typed data.
func (i *Integer) Pointer() interface{} { return (*int)(i) }

// CompareValue compares the `Integer` with other values. If the other value
// is not an `*Integer` with the same value, an error is returned.
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

// Time represents a datetime value.
type Time time.Time

// Value returns the underlying data as a `time.Time`.
func (t *Time) Value() interface{} {
	if t == nil {
		return nil
	}
	return time.Time(*t)
}

// Pointer returns a pointer to the underlying `time.Time`-typed data.
func (t *Time) Pointer() interface{} { return (*time.Time)(t) }

// CompareValue compares the `Time` with other values. If the other value is
// not a `*Time` with the same value, an error is returned.
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

// MarshalJSON marshals a `Time` as JSON. It implements the
// `encoding/json.Marshaler` interface.
func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Time.Format(time.Time(t), time.RFC3339))
}

// NewString creates a new `*String` object.
func NewString(s string) *String { return (*String)(&s) }

// NewInteger creates a new `*Integer` object.
func NewInteger(i int) *Integer { return (*Integer)(&i) }

// NewTime creates a new `*Time` object.
func NewTime(t time.Time) *Time { return (*Time)(&t) }

// NilString creates a `Value` which is implemented by a nil `*String`.
func NilString() Value { return new(String) }

// NilInteger creates a `Value` which is implemented by a nil `*Integer`.
func NilInteger() Value { return new(Integer) }

// NilTime creates a `Value` which is implemented by a nil `*Time`.
func NilTime() Value { return new(Time) }

// DynamicItemFactory returns a factory function which returns `DynamicItem`s
// on each call. The item's values are generated by the provided `values`
// functions. For example, `DynamicItemFactory(NilString, NilInteger)` will
// return a function which creates items with type (String, Integer) and whose
// values are empty.
func DynamicItemFactory(values ...func() Value) func() DynamicItem {
	return func() DynamicItem {
		item := make(DynamicItem, len(values))
		for i := range values {
			item[i] = values[i]()
		}
		return item
	}
}

// NilValueFuncFromColumnType returns a function which returns "typed-nil"
// values for a given column type string, if the column type is supported. For
// example, if the column type is `INTEGER`, this function will return
// `NilInteger, nil`.
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

// EmptyDynamicItemFromColumns takes a list of columns and generates an "empty"
// item whose values are "typed-nil"s (the types of the nil values corresponds
// to the column types). If any of the column types aren't supported, an error
// is returned.
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

// DynamicItemFactoryFromColumns returns a DynamicItem factory function based
// on a list of columns. See `DynamicItemFactory` for details about factory
// functions. See `NilValueFuncFromColumnType` for details about how column
// types are matched to value types, see `ValueTypeFromColumnType`.
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

// DynamicItem represents a row in a table whose type isn't known at compile
// time. It implements the `pgutil.Item` interface.
type DynamicItem []Value

// Scan implements the `pgutil.Item` interface's `Scan()` method.
func (di DynamicItem) Scan(pointers []interface{}) {
	for i := range di {
		if di[i] != nil {
			pointers[i] = di[i].Pointer()
		}
	}
}

// Scan implements the `pgutil.Item` interface's `Values()` method.
func (di DynamicItem) Values(values []interface{}) {
	for i := range di {
		if di[i] != nil {
			values[i] = di[i].Value()
		}
	}
}

// Scan implements the `pgutil.Item` interface's `ID()` method.
func (di DynamicItem) ID() interface{} { return di[idColumnPosition] }

// Compare compares two `DynamicItem`s. If the items differ in length, type, or
// value, an error is returned. Otherwise, `nil`.
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

// CompareDynamicItems compares two slices of `DynamicItem`s. If the slices
// differ in length, order, or value, an error is returned. Otherwise nil.
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

// ToDynamicItems converts a `Result` into a slice of `DynamicItem`s. If any
// `Result.Scan()` operation fails, an error is returned.
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
