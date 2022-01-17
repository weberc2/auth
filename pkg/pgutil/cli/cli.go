package cli

import (
	"database/sql"
	"encoding/json"
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
				return table.Insert(db, itemFromFlags(flags, ctx))
			}),
		}, {
			Name:        "upsert",
			Aliases:     []string{"add", "create", "put"},
			Description: "insert or update an item in the postgres table",
			Flags:       flags.insert,
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				return table.Upsert(db, itemFromFlags(flags, ctx))
			}),
		}, {
			Name:        "get",
			Aliases:     []string{"fetch"},
			Description: "put an item into the postgres table",
			Flags:       []cli.Flag{flags.id},
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				item := newDynamicItem(flags.types)
				if err := table.Get(
					db,
					ctx.Generic(flags.idName),
					item,
				); err != nil {
					return err
				}
				tmp := make(map[string]interface{}, len(table.Columns))
				for i, c := range table.Columns {
					tmp[c.Name] = item[i]
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
				newItem, err := pgutil.DynamicItemFactoryFromColumns(
					table.Columns...,
				)
				if err != nil {
					return err
				}

				columnNames := make([]string, len(table.Columns))
				for i, c := range table.Columns {
					columnNames[i] = c.Name
				}

				var items []map[string]interface{}
				for result.Next() {
					item := newItem()
					result.Scan(item)
					items = append(items, itemToMap(columnNames, item))
				}
				return jsonPrint(items)
			}),
		}},
	}, nil
}

func itemFromFlags(flags *flags, ctx *cli.Context) pgutil.DynamicItem {
	item := make(pgutil.DynamicItem, len(flags.names))
	for i := range flags.insert {
		switch flags.types[i] {
		case flagTypeInteger:
			item[i] = pgutil.NewInteger(ctx.Int(flags.names[i]))
		case flagTypeString:
			item[i] = pgutil.NewString(ctx.String(flags.names[i]))
		case flagTypeTimestamp:
			if t := ctx.Timestamp(flags.names[i]); t != nil {
				item[i] = pgutil.NewTime(*t)
			}
		default:
			panic(fmt.Sprintf("unsupported flag type: %d", flags.types[i]))
		}
	}
	return item
}

func itemToMap(
	headers []string,
	item pgutil.DynamicItem,
) map[string]interface{} {
	m := make(map[string]interface{}, len(headers))
	for i := range headers {
		m[headers[i]] = item[i].Pointer()
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

func (ft flagType) zero() pgutil.Value {
	switch ft {
	case flagTypeInteger:
		return pgutil.NilInteger()
	case flagTypeString:
		return pgutil.NilString()
	case flagTypeTimestamp:
		return pgutil.NilTime()
	default:
		panic(fmt.Sprintf("invalid flagType: %d", ft))
	}
}

func newDynamicItem(types []flagType) pgutil.DynamicItem {
	item := make(pgutil.DynamicItem, len(types))
	for i, t := range types {
		item[i] = t.zero()
	}
	return item
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
}

func tableFlags(t *pgutil.Table) (*flags, error) {
	var (
		insertFlags = make([]cli.Flag, len(t.Columns))
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
	}, nil
}

func intFlag(name string, required bool) cli.Flag {
	return &cli.IntFlag{Name: name, Required: required}
}

func stringFlag(name string, required bool) cli.Flag {
	return &cli.StringFlag{Name: name, Required: required}
}

func timestampFlag(name string, required bool) cli.Flag {
	return &cli.TimestampFlag{
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
