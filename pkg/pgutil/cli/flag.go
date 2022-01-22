package cli

import (
	"fmt"
	"time"

	"github.com/gosimple/slug"
	"github.com/urfave/cli/v2"
	"github.com/weberc2/auth/pkg/pgutil"
)

func insertFlags(t *pgutil.Table) []cli.Flag {
	columns := t.Columns()
	out := make([]cli.Flag, len(columns))
	for i, c := range columns {
		flag, err := newFlag(c.Type, slug.Make(c.Name), true)
		if err != nil {
			panic(fmt.Errorf("column `%s`: %w", c.Name, err))
		}
		out[i] = flag
	}
	return out
}

func updateFlags(t *pgutil.Table) []cli.Flag {
	columns := t.Columns()
	out := make([]cli.Flag, len(columns))
	for i, c := range columns {
		flag, err := newFlag(c.Type, slug.Make(c.Name), true)
		if err != nil {
			panic(fmt.Errorf("column `%s`: %w", c.Name, err))
		}
		out[i] = flag
	}
	return out
}

func newFlag(columnType string, flag string, required bool) (cli.Flag, error) {
	valueType, err := pgutil.ValueTypeFromColumnType(columnType)
	if err != nil {
		return nil, err
	}
	switch valueType {
	case pgutil.ValueTypeString:
		return &cli.StringFlag{Name: flag, Required: required}, nil
	case pgutil.ValueTypeInteger:
		return &cli.IntFlag{Name: flag, Required: required}, nil
	case pgutil.ValueTypeTime:
		return &cli.TimestampFlag{
			Name:     flag,
			Required: required,
			Layout:   time.RFC3339,
		}, nil
	default:
		panic(fmt.Sprintf("invalid value type: %d", valueType))
	}
}

func requiredColumnFlags(columns []pgutil.Column) []cli.Flag {
	out := make([]cli.Flag, len(columns))
	for i, c := range columns {
		f, err := newFlag(c.Type, slug.Make(c.Name), true)
		if err != nil {
			panic(fmt.Sprintf("column `%s`: %v", c.Name, err))
		}
		out[i] = f
	}
	return out
}

func itemFromFlags(
	t *pgutil.Table,
	ctx *cli.Context,
) (pgutil.DynamicItem, error) {
	columns := t.Columns()
	item := make(pgutil.DynamicItem, len(columns))
	for i, c := range columns {
		value, err := flagValue(c.Type, ctx, slug.Make(c.Name))
		if err != nil {
			return nil, fmt.Errorf("column `%s`: %w", c.Name, err)
		}
		item[i] = value
	}
	return item, nil
}

func flagValue(
	columnType string,
	ctx *cli.Context,
	flag string,
) (pgutil.Value, error) {
	valueType, err := pgutil.ValueTypeFromColumnType(columnType)
	if err != nil {
		return nil, err
	}
	switch valueType {
	case pgutil.ValueTypeString:
		return pgutil.NewString(ctx.String(flag)), nil
	case pgutil.ValueTypeInteger:
		return pgutil.NewInteger(ctx.Int(flag)), nil
	case pgutil.ValueTypeTime:
		if t := ctx.Timestamp(flag); t != nil {
			return pgutil.NewTime(*t), nil
		}
		return nil, nil
	default:
		panic(fmt.Sprintf("invalid value type: %d", valueType))
	}
}
