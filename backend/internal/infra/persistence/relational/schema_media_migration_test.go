package relational

import (
	"context"
	"path/filepath"
	"testing"
)

func TestWidenMediaJobSecondsConstraintPreservesExistingSQLiteRows(t *testing.T) {
	ctx := context.Background()
	database, err := OpenSQLite(ctx, filepath.Join(t.TempDir(), "media-constraint.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := database.db.Exec(`CREATE TABLE media_jobs (
		id TEXT PRIMARY KEY,
		seconds INTEGER NOT NULL CONSTRAINT chk_media_jobs_seconds CHECK (seconds BETWEEN 1 AND 15)
	)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.db.Exec(`INSERT INTO media_jobs(id, seconds) VALUES ('existing', 15)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := database.widenMediaJobSecondsConstraint(ctx); err != nil {
		t.Fatal(err)
	}
	if err := database.db.Exec(`INSERT INTO media_jobs(id, seconds) VALUES ('legacy-20', 20)`).Error; err != nil {
		var ddl string
		_ = database.db.Raw("SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'media_jobs'").Scan(&ddl).Error
		t.Fatalf("20-second job rejected after migration: %v ddl=%s", err, ddl)
	}
	if err := database.db.Exec(`INSERT INTO media_jobs(id, seconds) VALUES ('invalid-21', 21)`).Error; err == nil {
		t.Fatal("21-second job accepted after migration")
	}
	var count int64
	if err := database.db.Table("media_jobs").Where("id = ?", "existing").Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("existing row count=%d err=%v", count, err)
	}
}

func TestPostgresMediaSecondsConstraintMigrationIsIdempotent(t *testing.T) {
	for _, definition := range []string{
		"CHECK (((seconds >= 1) AND (seconds <= 20)))",
		"CHECK (seconds BETWEEN 1 AND 20)",
		`CHECK ((("seconds" >= (1)::bigint) AND ("seconds" <= (20)::bigint)))`,
	} {
		if postgresMediaSecondsConstraintNeedsMigration(definition) {
			t.Fatalf("current definition marked stale: %s", definition)
		}
	}
	for _, definition := range []string{
		"",
		"CHECK (((seconds >= 1) AND (seconds <= 15)))",
		"CHECK (seconds BETWEEN 1 AND 15)",
	} {
		if !postgresMediaSecondsConstraintNeedsMigration(definition) {
			t.Fatalf("stale definition marked current: %s", definition)
		}
	}
}
