package pgutil

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

// OpenEnv opens a database connection based on environment variables,
// `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASS`, `PG_DB_NAME`, and `PG_SSL_MODE`.
// These environment variables have default values, `localhost`, `5432`,
// `postgres`, <empty string>, `postgres`, and `disable`, respectively.
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

// OpenEnvPing opens a database connection based on environment variables (per
// `OpenEnv`) *and* it pings the database, returning an error if the connection
// doesn't work.
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
