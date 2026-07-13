package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/chenyme/grok2api/backend/internal/infra/persistence/relational"
)

const postgresDSNEnvironment = "GROK2API_POSTGRES_DSN"

func main() {
	sqlitePath := flag.String("sqlite", "", "source SQLite database path")
	snapshotPath := flag.String("snapshot", "", "new SQLite snapshot path")
	batchSize := flag.Int("batch-size", 500, "rows copied per PostgreSQL insert batch")
	flag.Parse()

	dsn := strings.TrimSpace(os.Getenv(postgresDSNEnvironment))
	if strings.TrimSpace(*sqlitePath) == "" || strings.TrimSpace(*snapshotPath) == "" || dsn == "" {
		log.Fatalf("-sqlite, -snapshot, and %s are required", postgresDSNEnvironment)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	_, err := relational.MigrateSQLiteToPostgres(ctx, relational.MigrationOptions{
		SQLitePath:   *sqlitePath,
		SnapshotPath: *snapshotPath,
		PostgresDSN:  dsn,
		BatchSize:    *batchSize,
		LogTable: func(report relational.MigrationTableReport) {
			fmt.Printf("table=%s rows=%d sha256=%s\n", report.Table, report.Rows, report.SHA256)
		},
	})
	if err != nil {
		log.Fatal(err)
	}
}
