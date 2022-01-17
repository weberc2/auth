package cli

import (
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/gosimple/slug"
	"github.com/urfave/cli/v2"
	"github.com/weberc2/auth/pkg/pgutil"
)

func New(table *pgutil.Table) (*cli.App, error) {
	flags, err := tableFlags(table)
	if err != nil {
		return nil, err
	}

	return &cli.App{
		Name: table.Name,
		Description: fmt.Sprintf(
			"a CLI for the `%s` Postgres table",
			table.Name,
		),
		Commands: []*cli.Command{{
			Name:        "table",
			Description: "commands for managing the Postgres table",
			Subcommands: []*cli.Command{{
				Name:        "ensure",
				Aliases:     []string{"make", "create"},
				Description: "create the postgres table if it doesn't exist",
				Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
					return table.Ensure(db)
				}),
			}, {
				Name:        "drop",
				Aliases:     []string{"delete", "destroy"},
				Description: "drop the postgres table",
				Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
					return table.Drop(db)
				}),
			}, {
				Name:        "clear",
				Aliases:     []string{"truncate", "trunc"},
				Description: "truncate the postgres table",
				Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
					return table.Clear(db)
				}),
			}, {
				Name:        "reset",
				Aliases:     []string{"recreate"},
				Description: "drop and recreate the postgres table",
				Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
					return table.Reset(db)
				}),
			}},
		}, {
			Name:        "insert",
			Aliases:     []string{"add", "create", "put"},
			Description: "put an item into the postgres table",
			Flags:       flags.insert,
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				return table.Insert(db, rowFromFlags(flags, ctx))
			}),
		}, {
			Name:        "upsert",
			Aliases:     []string{"add", "create", "put"},
			Description: "insert or update an item in the postgres table",
			Flags:       flags.upsert,
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				return table.Upsert(db, rowFromFlags(flags, ctx))
			}),
		}, {
			Name:        "get",
			Aliases:     []string{"fetch"},
			Description: "put an item into the postgres table",
			Flags:       []cli.Flag{flags.id},
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				row := make(Row, len(flags.types))
				for i, t := range flags.types {
					row[i] = t.zero()
				}
				if err := table.Get(
					db,
					ctx.Generic(flags.idName),
					row,
				); err != nil {
					return err
				}
				tmp := make(map[string]interface{}, len(table.Columns))
				for i, c := range table.Columns {
					tmp[c.Name] = row[i]
				}
				return jsonPrint(tmp)
			}),
		}, {
			Name:        "delete",
			Aliases:     []string{"remove", "rm", "del", "drop"},
			Description: "remove an item from the postgres table",
			Flags:       []cli.Flag{flags.id},
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				if err := table.Delete(
					db,
					ctx.Generic(flags.idName),
				); err != nil {
					return err
				}
				return nil
			}),
		}, {
			Name:        "list",
			Description: "list all items in the postgres table",
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				result, err := table.List(db)
				if err != nil {
					return err
				}
				newRow, err := RowFuncFromColumns(table.Columns...)
				if err != nil {
					return err
				}

				columnNames := make([]string, len(table.Columns))
				for i, c := range table.Columns {
					columnNames[i] = c.Name
				}

				var rows []map[string]interface{}
				for result.Next() {
					row := newRow()
					result.Scan(row)
					rows = append(rows, rowToMap(columnNames, row))
				}
				return jsonPrint(rows)
			}),
		}},
	}, nil
}

func rowFromFlags(flags *flags, ctx *cli.Context) Row {
	row := make(Row, len(flags.names))
	for i := range flags.upsert {
		row[i] = ctx.Generic(flags.names[i])
		// as a special case, we have to convert `timeFlag` flag values from
		// `tyme` back to `time.Time`. The former is necessary to implement the
		// flag.Value interface, but the latter is necessary for SQL.
		if _, ok := flags.upsert[i].(*timeFlag); ok {
			row[i] = (*time.Time)(ctx.Generic(flags.names[i]).(*tyme))
		}
	}
	return row
}

func rowToMap(headers []string, row Row) map[string]interface{} {
	m := make(map[string]interface{}, len(headers))
	for i := range headers {
		m[headers[i]] = row[i]
	}
	return m
}

func jsonPrint(v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	_, err = fmt.Printf("%s\n", data)
	return err
}

func withConn(f func(db *sql.DB, ctx *cli.Context) error) cli.ActionFunc {
	return func(ctx *cli.Context) error {
		db, err := pgutil.OpenEnvPing()
		if err != nil {
			return err
		}

		return f(db, ctx)
	}
}

func parseVarChar(s string) (int, error) {
	if strings.HasPrefix(s, "VARCHAR(") && s[len(s)-1] == ')' {
		if i, err := strconv.Atoi(s[len("VARCHAR(") : len(s)-1]); err == nil {
			return i, nil
		}
	}
	return 0, fmt.Errorf("wanted `VARCHAR(<int>)`; found `%s`", s)
}

type flagType int

func (ft flagType) zero() interface{} {
	switch ft {
	case flagTypeInteger:
		return new(int)
	case flagTypeString:
		return new(string)
	case flagTypeTimestamp:
		return new(time.Time)
	default:
		panic(fmt.Sprintf("invalid flagType: %d", ft))
	}
}

const (
	flagTypeInteger flagType = iota
	flagTypeString
	flagTypeTimestamp
)

type flags struct {
	id     cli.Flag
	idName string
	names  []string
	types  []flagType
	insert []cli.Flag
	upsert []cli.Flag
}

func tableFlags(t *pgutil.Table) (*flags, error) {
	var (
		insertFlags = make([]cli.Flag, len(t.Columns))
		upsertFlags = make([]cli.Flag, len(t.Columns))
		names       = make([]string, len(t.Columns))
		types       = make([]flagType, len(t.Columns))
		idColumn    = t.IDColumn()
	)

	for i, c := range t.Columns {
		flagType, flagFunc, err := flagFunc(c.Type)
		if err != nil {
			return nil, fmt.Errorf("column `%s`: %w", c.Name, err)
		}
		flagName := slug.Make(c.Name)
		insertFlags[i] = flagFunc(flagName, idColumn.Name == c.Name || !c.Null)
		upsertFlags[i] = flagFunc(flagName, idColumn.Name == c.Name)
		types[i] = flagType
		names[i] = flagName
	}

	_, idColumnFlagFunc, err := flagFunc(idColumn.Type)
	if err != nil {
		return nil, fmt.Errorf("column `%s`: %w", idColumn.Name, err)
	}
	idName := slug.Make(idColumn.Name)

	return &flags{
		id:     idColumnFlagFunc(idName, true),
		idName: idName,
		names:  names,
		types:  types,
		insert: insertFlags,
		upsert: upsertFlags,
	}, nil
}

func intFlag(name string, required bool) cli.Flag {
	return &cli.IntFlag{Name: name, Required: required}
}

func stringFlag(name string, required bool) cli.Flag {
	return &cli.StringFlag{Name: name, Required: required}
}

type timeFlag struct {
	Name       string
	Usage      string
	Required   bool
	HasBeenSet bool
	Layout     string
	Value      time.Time
}

func (tf *timeFlag) String() string {
	return cli.FlagStringer(tf)
}

func (tf *timeFlag) IsRequired() bool { return tf.Required }

func (tf *timeFlag) GetValue() string { return tf.Value.Format(time.RFC3339) }

func (tf *timeFlag) GetUsage() string { return tf.Usage }

func (tf *timeFlag) TakesValue() bool { return true }

type tyme time.Time

func (t *tyme) Scan(v interface{}) error {
	switch x := v.(type) {
	case string:
		return t.Set(x)
	case time.Time:
		*t = tyme(x)
		return nil
	default:
		return fmt.Errorf("unsupported time value: %v (%T)", v, v)
	}
}

func (t *tyme) String() string { return (time.Time)(*t).Format(time.RFC3339) }

func (t *tyme) Set(s string) error {
	tv, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return err
	}
	*t = tyme(tv)
	return nil
}

func (tf *timeFlag) Apply(fs *flag.FlagSet) error {
	fs.Var((*tyme)(&tf.Value), tf.Name, tf.Usage)
	return nil
}

func (tf *timeFlag) Names() []string { return []string{tf.Name} }

func (tf *timeFlag) IsSet() bool { return tf.HasBeenSet }

func timestampFlag(name string, required bool) cli.Flag {
	return &timeFlag{
		Name:     name,
		Required: required,
		Layout:   time.RFC3339,
	}
}

func flagFunc(typ string) (flagType, func(string, bool) cli.Flag, error) {
	switch typ {
	case "TIMESTAMP", "TIMESTAMPTZ":
		return flagTypeTimestamp, timestampFlag, nil
	case "INTEGER":
		return flagTypeInteger, intFlag, nil
	case "TEXT":
		return flagTypeString, stringFlag, nil
	default:
		if _, err := parseVarChar(typ); err == nil {
			return flagTypeString, stringFlag, nil
		}
		return -1, nil, fmt.Errorf("unsupported column type: %s", typ)
	}
}
