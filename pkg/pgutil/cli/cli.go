package cli

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/urfave/cli/v2"
	"github.com/weberc2/auth/pkg/pgutil"
)

// New creates a new CLI app for a given `pgutil.Table` schema.
func New(t *pgutil.Table) (*cli.App, error) {
	return &cli.App{
		Name: t.Name,
		Description: fmt.Sprintf(
			"a CLI for the `%s` Postgres table",
			t.Name,
		),
		Usage: fmt.Sprintf(
			"a CLI for the `%s` Postgres table",
			t.Name,
		),
		Commands: []*cli.Command{{
			Name: "table",
			Description: fmt.Sprintf(
				"commands for managing the `%s` table",
				t.Name,
			),
			Usage: fmt.Sprintf(
				"commands for managing the `%s` table",
				t.Name,
			),
			Subcommands: []*cli.Command{{
				Name:    "ensure",
				Aliases: []string{"make", "create"},
				Description: fmt.Sprintf(
					"create the `%s` table if it doesn't exist",
					t.Name,
				),
				Usage: fmt.Sprintf(
					"create the `%s` table if it doesn't exist",
					t.Name,
				),
				Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
					return t.Ensure(db)
				}),
			}, {
				Name:        "drop",
				Aliases:     []string{"delete", "destroy"},
				Description: fmt.Sprintf("drop the `%s` table", t.Name),
				Usage:       fmt.Sprintf("drop the `%s` table", t.Name),
				Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
					return t.Drop(db)
				}),
			}, {
				Name:    "clear",
				Aliases: []string{"truncate", "trunc"},
				Description: fmt.Sprintf(
					"truncate the `%s` table",
					t.Name,
				),
				Usage: fmt.Sprintf(
					"truncate the `%s` table",
					t.Name,
				),
				Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
					return t.Clear(db)
				}),
			}, {
				Name:    "reset",
				Aliases: []string{"recreate"},
				Description: fmt.Sprintf(
					"drop and recreate the `%s` table",
					t.Name,
				),
				Usage: fmt.Sprintf(
					"drop and recreate the `%s` table",
					t.Name,
				),
				Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
					return t.Reset(db)
				}),
			}},
		}, {
			Name:    "insert",
			Aliases: []string{"add", "create", "put"},
			Description: fmt.Sprintf(
				"put an item into the `%s` table",
				t.Name,
			),
			Usage: fmt.Sprintf(
				"put an item into the `%s` table",
				t.Name,
			),
			Flags: insertFlags(t),
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				item, err := itemFromFlags(t, ctx)
				if err != nil {
					return fmt.Errorf(
						"building insertion item for table `%s`: %w",
						t.Name,
						err,
					)
				}
				return t.Insert(db, item)
			}),
		}, {
			Name:    "upsert",
			Aliases: []string{"add", "create", "put"},
			Description: fmt.Sprintf(
				"insert or update an item in the `%s` table",
				t.Name,
			),
			Usage: fmt.Sprintf(
				"insert or update an item in the `%s` table",
				t.Name,
			),
			Flags: insertFlags(t),
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				item, err := itemFromFlags(t, ctx)
				if err != nil {
					return fmt.Errorf(
						"building insertion item for table `%s`: %w",
						t.Name,
						err,
					)
				}
				return t.Upsert(db, item)
			}),
		}, {
			Name:    "get",
			Aliases: []string{"fetch"},
			Description: fmt.Sprintf(
				"put an item into the `%s` table",
				t.Name,
			),
			Usage: fmt.Sprintf(
				"put an item into the `%s` table",
				t.Name,
			),
			Flags: requiredColumnFlags(t.PrimaryKeys),
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				result, err := pgutil.EmptyDynamicItemFromTable(t)
				if err != nil {
					return fmt.Errorf(
						"building allocating return item for table `%s`: %w",
						t.Name,
						err,
					)
				}
				id, err := itemFromFlags(t, ctx)
				if err != nil {
					return err
				}
				if err := t.Get(db, id, result); err != nil {
					return err
				}
				columns := t.Columns()
				tmp := make(map[string]interface{}, len(columns))
				for i, c := range columns {
					tmp[c.Name] = result[i]
				}
				return jsonPrint(tmp)
			}),
		}, {
			Name:    "delete",
			Aliases: []string{"remove", "rm", "del", "drop"},
			Description: fmt.Sprintf(
				"remove an item from the `%s` table",
				t.Name,
			),
			Usage: fmt.Sprintf(
				"remove an item from the `%s` table",
				t.Name,
			),
			Flags: requiredColumnFlags(t.PrimaryKeys),
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				id, err := itemFromFlags(t, ctx)
				if err != nil {
					return err
				}
				if err := t.Delete(db, id); err != nil {
					return err
				}
				return nil
			}),
		}, {
			Name: "list",
			Description: fmt.Sprintf(
				"list all items in the `%s` table",
				t.Name,
			),
			Usage: fmt.Sprintf(
				"list all items in the `%s` table",
				t.Name,
			),
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				result, err := t.List(db)
				if err != nil {
					return err
				}
				newItem, err := pgutil.ZeroedDynamicItemFactoryFromTable(t)
				if err != nil {
					return err
				}

				columns := t.Columns()
				columnNames := make([]string, len(columns))
				for i, c := range columns {
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
			Name: "update",
			Description: fmt.Sprintf(
				"update an item in the `%s` table",
				t.Name,
			),
			Usage: fmt.Sprintf(
				"update an item in the `%s` table",
				t.Name,
			),
			Flags: updateFlags(t),
			Action: withConn(func(db *sql.DB, ctx *cli.Context) error {
				item, err := itemFromFlags(t, ctx)
				if err != nil {
					return err
				}
				return t.Update(db, item)
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
