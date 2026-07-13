package relational

import (
	"context"
	"path/filepath"
	"sort"
	"testing"
)

func TestCreateSQLiteSnapshotIncludesCommittedWALRows(t *testing.T) {
	ctx := context.Background()
	sourcePath := filepath.Join(t.TempDir(), "source.db")
	source, err := OpenSQLite(ctx, sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if err := source.db.Exec("CREATE TABLE snapshot_probe (id INTEGER PRIMARY KEY, value TEXT NOT NULL)").Error; err != nil {
		t.Fatal(err)
	}
	if err := source.db.Exec("INSERT INTO snapshot_probe(id, value) VALUES (1, 'visible')").Error; err != nil {
		t.Fatal(err)
	}

	snapshotPath := filepath.Join(t.TempDir(), "snapshot.db")
	if err := CreateSQLiteSnapshot(ctx, sourcePath, snapshotPath); err != nil {
		t.Fatal(err)
	}
	snapshot, err := OpenSQLite(ctx, snapshotPath)
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	var value string
	if err := snapshot.db.Raw("SELECT value FROM snapshot_probe WHERE id = 1").Scan(&value).Error; err != nil {
		t.Fatal(err)
	}
	if value != "visible" {
		t.Fatalf("snapshot value = %q", value)
	}
}

func TestMigrationPlanCoversEverySchemaModelOnce(t *testing.T) {
	type tableNamer interface{ TableName() string }
	expected := make([]string, 0, len(schemaModels))
	for _, value := range schemaModels {
		namer, ok := value.(tableNamer)
		if !ok {
			t.Fatalf("schema model %T has no table name", value)
		}
		expected = append(expected, namer.TableName())
	}
	actual := migrationTableNames()
	sort.Strings(expected)
	sort.Strings(actual)
	if len(actual) != len(expected) {
		t.Fatalf("migration tables = %d, schema tables = %d\nactual=%v\nexpected=%v", len(actual), len(expected), actual, expected)
	}
	for index := range expected {
		if actual[index] != expected[index] {
			t.Fatalf("migration tables differ\nactual=%v\nexpected=%v", actual, expected)
		}
		if index > 0 && actual[index] == actual[index-1] {
			t.Fatalf("duplicate migration table %q", actual[index])
		}
	}
}
