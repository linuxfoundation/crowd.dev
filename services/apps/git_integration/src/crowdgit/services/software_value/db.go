package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// InsightsDB holds the connection pool and configuration for the Insights database.
type InsightsDB struct {
	pool   *pgxpool.Pool
	config DBConfig
}

// newDBConnection establishes a new database connection pool.
// This is a helper function used by NewInsightsDB.
func newDBConnection(ctx context.Context, config DBConfig) (*pgxpool.Pool, error) {
	const dbConnectionStringTemplate = "user=%s password=%s host=%s port=%d dbname=%s sslmode=%s pool_max_conns=%d"
	dbConnectionString := fmt.Sprintf(dbConnectionStringTemplate,
		config.User,
		config.Password,
		config.Host,
		config.Port,
		config.DBName,
		config.SSLMode,
		config.PoolMax,
	)

	poolConfig, err := pgxpool.ParseConfig(dbConnectionString)
	if err != nil {
		return nil, fmt.Errorf("error parsing connection string: %w", err)
	}

	if config.ReadOnly {
		poolConfig.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
			_, execErr := conn.Exec(ctx, "SET SESSION default_transaction_read_only = 'on'")
			if execErr != nil {
				log.Printf("Failed to set read-only mode for new connection to %s: %v", config.DBName, execErr)
			} else {
				log.Printf("Connection to %s for user %s set to read-only mode.", config.DBName, config.User)
			}
			return execErr // If error occurs, connection won't be added to pool or pool creation might fail
		}
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	// Check if the connection is actually alive and close the pool if not.
	err = pool.Ping(ctx)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("error pinging database: %w", err)
	}
	log.Printf("Database connection successful for %s.", config.DBName)
	return pool, nil
}

// NewInsightsDB creates a new InsightsDB instance.
func NewInsightsDB(ctx context.Context, config DBConfig) (*InsightsDB, error) {
	pool, err := newDBConnection(ctx, config)
	if err != nil {
		return nil, err
	}
	return &InsightsDB{
		pool:   pool,
		config: config,
	}, nil
}

// Close closes the database connection pool.
func (db *InsightsDB) Close() {
	if db.pool != nil {
		db.pool.Close()
	}
}

// saveProjectCost upserts the project cost into the database table.
func (db *InsightsDB) saveProjectCost(ctx context.Context, repository Repository, estimatedCost float64) error {
	// From PostgreSQL 15 and newer, there's another way to do this: https://www.postgresql.org/docs/15/sql-merge.html
	// But since we currently use PostgreSQL 14 for CM, and despite me having created a database for this using 15,
	// I'll keep this compatible with 14, just in case.
	sqlStatement := `
			INSERT INTO project_costs (repo_url, estimated_cost, updated_at)
			VALUES ($1, $2, $3)
			ON CONFLICT (repo_url) DO UPDATE
			SET estimated_cost = EXCLUDED.estimated_cost,
				updated_at = NOW();
		`

	_, err := db.pool.Exec(ctx, sqlStatement, repository.URL, estimatedCost, time.Now())
	if err != nil {
		return fmt.Errorf("failed to execute upsert for project '%s': %w", repository.Path, err)
	}
	return nil
}

// saveLanguageStats upserts the language stats into the database table.
func (db *InsightsDB) saveLanguageStats(ctx context.Context, repository Repository, langStats []LanguageStats) error {
	if len(langStats) == 0 {
		return nil // No stats to save
	}

	var valueStrings []string
	var args []interface{}
	argCounter := 1

	for _, stat := range langStats {
		valueStrings = append(valueStrings, fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			argCounter, argCounter+1, argCounter+2, argCounter+3, argCounter+4, argCounter+5,
			argCounter+6, argCounter+7, argCounter+8, argCounter+9, argCounter+10, argCounter+11))
		args = append(args, repository.URL, stat.LanguageName, stat.Bytes, stat.CodeBytes, stat.Lines, stat.Code,
			stat.Comment, stat.Blank, stat.Complexity, stat.Count, stat.WeightedComplexity, time.Now())
		argCounter += 12
	}

	sqlStatement := fmt.Sprintf(`
	 INSERT INTO language_stats (repo_url, language_name, bytes, code_bytes, lines, code, comment, blank, complexity, count, weighted_complexity, updated_at)
	 VALUES %s
	 ON CONFLICT (repo_url, language_name) DO UPDATE
	 SET lines = EXCLUDED.lines,
	  bytes = EXCLUDED.bytes,
	  code_bytes = EXCLUDED.code_bytes,
	  code = EXCLUDED.code,
	  comment = EXCLUDED.comment,
	  blank = EXCLUDED.blank,
	  complexity = EXCLUDED.complexity,
	  count = EXCLUDED.count,
	  weighted_complexity = EXCLUDED.weighted_complexity,
	  updated_at = NOW();`, strings.Join(valueStrings, ", "))

	// Execute the batch insert
	_, err := db.pool.Exec(ctx, sqlStatement, args...)
	if err != nil {
		return fmt.Errorf("failed to execute batch upsert for project '%s': %w", repository.URL, err)
	}

	return nil
}
