package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/gosimple/slug"
	"github.com/urfave/cli/v2"
	"github.com/weberc2/auth/pkg/pgutil"
)

func insertFlags(t *pgutil.Table) []cli.Flag {
	columns := t.Columns()
	out := make([]cli.Flag, len(columns))
	for i, c := range columns {
		flag, err := flagFromColumn(&columns[i], false)
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
		flag, err := flagFromColumn(&columns[i], true)
		if err != nil {
			panic(fmt.Errorf("column `%s`: %w", c.Name, err))
		}
		out[i] = flag
	}
	return out
}

func flagFromColumn(c *pgutil.Column, update bool) (cli.Flag, error) {
	flag := slug.Make(c.Name)
	optional := update || c.Null || c.Default != nil
	valueType, err := pgutil.ValueTypeFromColumnType(c.Type)
	if err != nil {
		return nil, err
	}
	deftext := "<none>"
	if c.Default != nil {
		var sb strings.Builder
		sb.WriteByte('`')
		c.Default.SQL(&sb)
		sb.WriteByte('`')
		deftext = sb.String()
	}
	switch valueType {
	case pgutil.ValueTypeBoolean:
		return &cli.BoolFlag{
			Name:        flag,
			DefaultText: deftext,
			Required:    !optional,
		}, nil
	case pgutil.ValueTypeString:
		return &cli.StringFlag{
			Name:        flag,
			DefaultText: deftext,
			Required:    !optional,
		}, nil
	case pgutil.ValueTypeInteger:
		return &cli.IntFlag{
			Name:        flag,
			DefaultText: deftext,
			Required:    !optional,
		}, nil
	case pgutil.ValueTypeTime:
		return &cli.TimestampFlag{
			Name:        flag,
			DefaultText: deftext,
			Required:    !optional,
			Layout:      time.RFC3339,
		}, nil
	default:
		panic(fmt.Sprintf("invalid value type: %d", valueType))
	}
}

func requiredColumnFlags(columns []pgutil.Column) []cli.Flag {
	out := make([]cli.Flag, len(columns))
	for i, c := range columns {
		f, err := flagFromColumn(&columns[i], false)
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
	case pgutil.ValueTypeBoolean:
		if ctx.IsSet(flag) {
			return pgutil.NewBoolean(ctx.Bool(flag)), nil
		}
		return pgutil.NilBoolean(), nil
	case pgutil.ValueTypeString:
		if ctx.IsSet(flag) {
			return pgutil.NewString(ctx.String(flag)), nil
		}
		return pgutil.NilString(), nil
	case pgutil.ValueTypeInteger:
		if ctx.IsSet(flag) {
			return pgutil.NewInteger(ctx.Int(flag)), nil
		}
		return pgutil.NilInteger(), nil
	case pgutil.ValueTypeTime:
		if ctx.IsSet(flag) {
			if t := ctx.Timestamp(flag); t != nil {
				return pgutil.NewTime(*t), nil
			}
		}
		return pgutil.NilTime(), nil
	default:
		panic(fmt.Sprintf("invalid value type: %d", valueType))
	}
}
