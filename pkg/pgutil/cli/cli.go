package cli

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/gosimple/slug"
	"github.com/urfave/cli/v2"
	"github.com/weberc2/auth/pkg/pgutil"
)

// New creates a new CLI app for a given `pgutil.Table` schema.
func New(table *pgutil.Table) (*cli.App, error) {
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
			Flags:       insertFlags(table),
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				item, err := itemFromFlags(table, ctx)
				if err != nil {
					return fmt.Errorf(
						"building insertion item for table `%s`: %w",
						table.Name,
						err,
					)
				}
				return table.Insert(db, item)
			}),
		}, {
			Name:        "upsert",
			Aliases:     []string{"add", "create", "put"},
			Description: "insert or update an item in the postgres table",
			Flags:       insertFlags(table),
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				item, err := itemFromFlags(table, ctx)
				if err != nil {
					return fmt.Errorf(
						"building insertion item for table `%s`: %w",
						table.Name,
						err,
					)
				}
				return table.Upsert(db, item)
			}),
		}, {
			Name:        "get",
			Aliases:     []string{"fetch"},
			Description: "put an item into the postgres table",
			Flags:       []cli.Flag{requiredColumnFlag(table.IDColumn())},
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				item, err := pgutil.EmptyDynamicItemFromColumns(table.Columns)
				if err != nil {
					return fmt.Errorf(
						"building allocating return item for table `%s`: %w",
						table.Name,
						err,
					)
				}
				idCol := table.IDColumn()
				val, err := flagValue(idCol.Type, ctx, slug.Make(idCol.Name))
				if err != nil {
					return err
				}
				if err := table.Get(db, val, item); err != nil {
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
			Flags:       []cli.Flag{requiredColumnFlag(table.IDColumn())},
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				idCol := table.IDColumn()
				val, err := flagValue(idCol.Type, ctx, slug.Make(idCol.Name))
				if err != nil {
					return err
				}
				if err := table.Delete(db, val); err != nil {
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
					if err := result.Scan(item); err != nil {
						return err
					}
					m := make(map[string]interface{}, len(columnNames))
					for i := range columnNames {
						m[columnNames[i]] = item[i].Pointer()
					}
					items = append(items, m)
				}
				return jsonPrint(items)
			}),
		}, {
			Name:        "update",
			Description: "update an item in the postgres table",
			Flags:       updateFlags(table),
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				item, err := itemFromFlags(table, ctx)
				if err != nil {
					return err
				}
				return table.Update(db, item)
			}),
		}},
	}, nil
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

func jsonPrint(v interface{}) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		panic(err)
	}
	_, err = fmt.Printf("%s\n", data)
	return err
}
