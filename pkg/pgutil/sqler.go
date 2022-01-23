package pgutil

import (
	"strconv"
	"strings"
	"time"
)

type SQLer interface {
	SQL(*strings.Builder)
}

func (b *Boolean) SQL(sb *strings.Builder) {
	if b == nil {
		sb.WriteString("NULL")
		return
	}

	if *b {
		sb.WriteString("true")
	} else {
		sb.WriteString("false")
	}
}

func (s *String) SQL(sb *strings.Builder) {
	if s == nil {
		sb.WriteString("NULL")
		return
	}

	sb.WriteByte('\'')
	sb.WriteString(string(*s))
	sb.WriteByte('\'')
}

func (i *Integer) SQL(sb *strings.Builder) {
	if i == nil {
		sb.WriteString("NULL")
		return
	}

	sb.WriteString(strconv.Itoa(int(*i)))
}

func (t *Time) SQL(sb *strings.Builder) {
	if t == nil {
		sb.WriteString("NULL")
		return
	}

	sb.WriteByte('\'')
	sb.WriteString(time.Time(*t).Format(time.RFC3339))
	sb.WriteByte('\'')
}

type SQL string

func (s SQL) SQL(sb *strings.Builder) { sb.WriteString(string(s)) }
