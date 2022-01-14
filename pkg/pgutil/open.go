package pgutil

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

func OpenEnv() (*sql.DB, error) {
	db, err := sql.Open(
		"postgres",
		fmt.Sprintf(
			"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
			getEnv("PG_HOST", "localhost"),
			getEnv("PG_PORT", "5432"),
			getEnv("PG_USER", "postgres"),
			getEnv("PG_PASS", ""),
			getEnv("PG_DB_NAME", "postgres"),
			getEnv("PG_SSL_MODE", "disable"),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("opening postgres database: %w", err)
	}
	return db, nil
}

func OpenEnvPing() (*sql.DB, error) {
	db, err := OpenEnv()
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("pinging postgres database: %w", err)
	}

	return db, nil
}

func getEnv(env, def string) string {
	x := os.Getenv(env)
	if x == "" {
		return def
	}
	return x
}
