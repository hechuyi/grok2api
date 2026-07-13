package relational

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"hash"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	glebarezsqlite "github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

const defaultMigrationBatchSize = 500

var timeValueType = reflect.TypeFor[time.Time]()

// MigrationOptions defines one complete SQLite-to-PostgreSQL migration. The
// PostgreSQL DSN is deliberately supplied by the caller so the command can
// enforce an environment-only secret boundary.
type MigrationOptions struct {
	SQLitePath   string
	SnapshotPath string
	PostgresDSN  string
	BatchSize    int
	LogTable     func(MigrationTableReport)
}

// MigrationTableReport contains only non-sensitive verification metadata.
type MigrationTableReport struct {
	Table  string
	Rows   int64
	SHA256 string
}

// MigrationReport is returned only after the PostgreSQL transaction and
// post-load ANALYZE both complete successfully.
type MigrationReport struct {
	Tables []MigrationTableReport
}

type migrationTable struct {
	name         string
	order        string
	autoSequence bool
	copy         func(context.Context, *gorm.DB, *gorm.DB, int) (MigrationTableReport, error)
}

// CreateSQLiteSnapshot uses VACUUM INTO from a read-only source connection.
// SQLite performs the copy from one consistent read transaction, including
// committed rows that are still resident in the WAL.
func CreateSQLiteSnapshot(ctx context.Context, sourcePath, snapshotPath string) error {
	sourcePath, err := filepath.Abs(strings.TrimSpace(sourcePath))
	if err != nil || strings.TrimSpace(sourcePath) == "" {
		return fmt.Errorf("SQLite source path is invalid")
	}
	snapshotPath, err = filepath.Abs(strings.TrimSpace(snapshotPath))
	if err != nil || strings.TrimSpace(snapshotPath) == "" {
		return fmt.Errorf("SQLite snapshot path is invalid")
	}
	if sourcePath == snapshotPath {
		return fmt.Errorf("SQLite source and snapshot paths must differ")
	}
	info, err := os.Stat(sourcePath)
	if err != nil {
		return fmt.Errorf("inspect SQLite source: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("SQLite source is not a regular file")
	}
	if _, err := os.Stat(snapshotPath); err == nil {
		return fmt.Errorf("SQLite snapshot already exists")
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect SQLite snapshot destination: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(snapshotPath), 0o700); err != nil {
		return fmt.Errorf("create SQLite snapshot directory: %w", err)
	}

	source, err := openSQLiteReadOnly(ctx, sourcePath)
	if err != nil {
		return err
	}
	defer source.Close()
	if err := source.db.WithContext(ctx).Exec("VACUUM INTO ?", snapshotPath).Error; err != nil {
		return fmt.Errorf("create SQLite snapshot: %w", err)
	}
	if err := os.Chmod(snapshotPath, 0o600); err != nil {
		return fmt.Errorf("protect SQLite snapshot: %w", err)
	}
	if err := validateSQLiteSnapshot(ctx, snapshotPath); err != nil {
		return err
	}
	return nil
}

// MigrateSQLiteToPostgres snapshots and validates SQLite, upgrades only that
// snapshot to the current schema, and copies every relational table in one
// PostgreSQL transaction. The original SQLite database is never modified.
func MigrateSQLiteToPostgres(ctx context.Context, options MigrationOptions) (MigrationReport, error) {
	options.SQLitePath = strings.TrimSpace(options.SQLitePath)
	options.SnapshotPath = strings.TrimSpace(options.SnapshotPath)
	options.PostgresDSN = strings.TrimSpace(options.PostgresDSN)
	if options.SQLitePath == "" || options.SnapshotPath == "" || options.PostgresDSN == "" {
		return MigrationReport{}, fmt.Errorf("SQLite source, snapshot, and PostgreSQL DSN are required")
	}
	if options.BatchSize <= 0 || options.BatchSize > 2000 {
		options.BatchSize = defaultMigrationBatchSize
	}
	if err := CreateSQLiteSnapshot(ctx, options.SQLitePath, options.SnapshotPath); err != nil {
		return MigrationReport{}, err
	}

	beforeAlignment, err := sqliteSnapshotTableCounts(ctx, options.SnapshotPath)
	if err != nil {
		return MigrationReport{}, err
	}
	// Schema evolution is applied to the disposable snapshot, never to the
	// source database. This also creates the singleton model runtime row.
	snapshotWriter, err := OpenSQLite(ctx, options.SnapshotPath)
	if err != nil {
		return MigrationReport{}, fmt.Errorf("open SQLite snapshot for schema alignment: %w", err)
	}
	if err := snapshotWriter.InitializeSchema(ctx); err != nil {
		_ = snapshotWriter.Close()
		return MigrationReport{}, fmt.Errorf("align SQLite snapshot schema: %w", err)
	}
	if err := snapshotWriter.db.WithContext(ctx).Exec("PRAGMA wal_checkpoint(TRUNCATE)").Error; err != nil {
		_ = snapshotWriter.Close()
		return MigrationReport{}, fmt.Errorf("checkpoint aligned SQLite snapshot: %w", err)
	}
	if err := snapshotWriter.Close(); err != nil {
		return MigrationReport{}, fmt.Errorf("close aligned SQLite snapshot: %w", err)
	}
	if err := validateSQLiteSnapshot(ctx, options.SnapshotPath); err != nil {
		return MigrationReport{}, err
	}
	afterAlignment, err := sqliteSnapshotTableCounts(ctx, options.SnapshotPath)
	if err != nil {
		return MigrationReport{}, err
	}
	for table, count := range beforeAlignment {
		if table != "model_runtime_state" && afterAlignment[table] != count {
			return MigrationReport{}, fmt.Errorf("SQLite schema alignment changed row count for %s", table)
		}
	}

	source, err := openSQLiteReadOnly(ctx, options.SnapshotPath)
	if err != nil {
		return MigrationReport{}, err
	}
	defer source.Close()
	target, err := OpenPostgres(ctx, options.PostgresDSN, 4, 2)
	if err != nil {
		return MigrationReport{}, err
	}
	defer target.Close()
	if err := requirePostgres18(ctx, target.db); err != nil {
		return MigrationReport{}, err
	}

	plan := migrationTables()
	if err := requireEmptyPostgresTarget(ctx, target.db, plan); err != nil {
		return MigrationReport{}, err
	}
	if err := target.InitializeSchema(ctx); err != nil {
		return MigrationReport{}, fmt.Errorf("initialize PostgreSQL schema: %w", err)
	}
	if err := requireEmptyPostgresTarget(ctx, target.db, plan); err != nil {
		return MigrationReport{}, err
	}

	reports := make([]MigrationTableReport, 0, len(plan))
	err = target.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// InitializeSchema intentionally seeds this singleton. The source row is
		// authoritative and is copied with the other 23 tables below.
		if err := tx.Exec("DELETE FROM model_runtime_state").Error; err != nil {
			return fmt.Errorf("prepare model_runtime_state: %w", err)
		}
		for _, table := range plan {
			report, err := table.copy(ctx, source.db, tx, options.BatchSize)
			if err != nil {
				return err
			}
			reports = append(reports, report)
		}
		for _, table := range plan {
			if table.autoSequence {
				if err := resetPostgresSequence(tx, table.name); err != nil {
					return err
				}
			}
		}
		if err := verifyPostgresConstraints(tx, migrationTableNames()); err != nil {
			return err
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return MigrationReport{}, err
	}
	for _, table := range plan {
		if err := target.db.WithContext(ctx).Exec("ANALYZE " + quoteIdentifier(table.name)).Error; err != nil {
			return MigrationReport{}, fmt.Errorf("analyze %s: %w", table.name, err)
		}
	}
	if options.LogTable != nil {
		for _, report := range reports {
			options.LogTable(report)
		}
	}
	return MigrationReport{Tables: reports}, nil
}

func migrationTables() []migrationTable {
	return []migrationTable{
		newMigrationTable(adminModel{}, "id ASC", true),
		newMigrationTable(accountModel{}, "id ASC", true),
		newMigrationTable(modelRouteModel{}, "id ASC", true),
		newMigrationTable(clientKeyModel{}, "id ASC", true),
		newMigrationTable(mediaAssetModel{}, "id ASC", false),
		newMigrationTable(runtimeSettingsModel{}, "key ASC", false),
		newMigrationTable(egressNodeModel{}, "id ASC", true),
		newMigrationTable(requestAuditModel{}, "id ASC", true),
		newMigrationTable(modelRuntimeStateModel{}, "id ASC", false),

		newMigrationTable(adminSessionModel{}, "id ASC", true),
		newMigrationTable(accountCredentialModel{}, "account_id ASC", false),
		newMigrationTable(accountProviderLinkModel{}, "web_account_id ASC", false),
		newMigrationTable(webAccountProfileModel{}, "account_id ASC", false),
		newMigrationTable(quotaWindowModel{}, "account_id ASC, mode ASC", false),
		newMigrationTable(billingModel{}, "account_id ASC", false),
		newMigrationTable(quotaRecoveryModel{}, "account_id ASC", false),
		newMigrationTable(accountModelCapabilityModel{}, "account_id ASC, upstream_model ASC", false),
		newMigrationTable(accountModelSyncStateModel{}, "account_id ASC", false),
		newMigrationTable(modelRouteAccountModel{}, "model_route_id ASC, account_id ASC", false),
		newMigrationTable(clientKeyModelPermission{}, "client_key_id ASC, model_route_id ASC", false),
		newMigrationTable(billingReservationModel{}, "event_id ASC", false),
		newMigrationTable(responseOwnershipModel{}, "response_id ASC", false),
		newMigrationTable(webResponseStateModel{}, "response_id ASC", false),
		newMigrationTable(mediaJobModel{}, "id ASC", false),
	}
}

func migrationTableNames() []string {
	tables := migrationTables()
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.name)
	}
	return names
}

func newMigrationTable[T any](model T, order string, autoSequence bool) migrationTable {
	namer, ok := any(model).(interface{ TableName() string })
	if !ok {
		panic(fmt.Sprintf("migration model %T has no table name", model))
	}
	name := namer.TableName()
	return migrationTable{
		name:         name,
		order:        order,
		autoSequence: autoSequence,
		copy: func(ctx context.Context, source, target *gorm.DB, batchSize int) (MigrationTableReport, error) {
			return copyTypedTable[T](ctx, source, target, name, order, batchSize)
		},
	}
}

func copyTypedTable[T any](ctx context.Context, source, target *gorm.DB, table, order string, batchSize int) (MigrationTableReport, error) {
	sourceDigest := sha256.New()
	statement := &gorm.Statement{DB: target}
	if err := statement.Parse(new(T)); err != nil {
		return MigrationTableReport{}, fmt.Errorf("inspect PostgreSQL model for %s: %w", table, err)
	}
	columns := append([]string(nil), statement.Schema.DBNames...)
	rows, err := source.WithContext(ctx).Model(new(T)).Order(order).Rows()
	if err != nil {
		return MigrationTableReport{}, fmt.Errorf("read %s from SQLite snapshot: %w", table, err)
	}
	defer rows.Close()
	batch := make([]map[string]any, 0, batchSize)
	var sourceCount int64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := target.WithContext(ctx).Session(&gorm.Session{SkipHooks: true}).Table(table).CreateInBatches(&batch, len(batch)).Error; err != nil {
			return fmt.Errorf("write %s to PostgreSQL: %w", table, err)
		}
		batch = batch[:0]
		return nil
	}
	for rows.Next() {
		var value T
		if err := source.ScanRows(rows, &value); err != nil {
			return MigrationTableReport{}, fmt.Errorf("decode %s from SQLite snapshot: %w", table, err)
		}
		if err := validatePostgresIntegerRange(reflect.ValueOf(value), table); err != nil {
			return MigrationTableReport{}, err
		}
		writeCanonicalRow(sourceDigest, reflect.ValueOf(value))
		sourceCount++
		row := make(map[string]any, len(columns))
		reflected := reflect.ValueOf(value)
		for _, column := range columns {
			field := statement.Schema.FieldsByDBName[column]
			fieldValue, _ := field.ValueOf(ctx, reflected)
			row[column] = postgresParameterValue(fieldValue)
		}
		batch = append(batch, row)
		if len(batch) == batchSize {
			if err := flush(); err != nil {
				return MigrationTableReport{}, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return MigrationTableReport{}, fmt.Errorf("iterate %s from SQLite snapshot: %w", table, err)
	}
	if err := flush(); err != nil {
		return MigrationTableReport{}, err
	}

	targetCount, targetDigest, err := digestTypedTable[T](ctx, target, table, order)
	if err != nil {
		return MigrationTableReport{}, err
	}
	sourceSum := hex.EncodeToString(sourceDigest.Sum(nil))
	if targetCount != sourceCount || targetDigest != sourceSum {
		return MigrationTableReport{}, fmt.Errorf("verification failed for %s: source rows=%d sha256=%s target rows=%d sha256=%s", table, sourceCount, sourceSum, targetCount, targetDigest)
	}
	return MigrationTableReport{Table: table, Rows: sourceCount, SHA256: sourceSum}, nil
}

func digestTypedTable[T any](ctx context.Context, database *gorm.DB, table, order string) (int64, string, error) {
	digest := sha256.New()
	rows, err := database.WithContext(ctx).Model(new(T)).Order(order).Rows()
	if err != nil {
		return 0, "", fmt.Errorf("read %s from PostgreSQL: %w", table, err)
	}
	defer rows.Close()
	var count int64
	for rows.Next() {
		var value T
		if err := database.ScanRows(rows, &value); err != nil {
			return 0, "", fmt.Errorf("decode %s from PostgreSQL: %w", table, err)
		}
		writeCanonicalRow(digest, reflect.ValueOf(value))
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, "", fmt.Errorf("iterate %s from PostgreSQL: %w", table, err)
	}
	return count, hex.EncodeToString(digest.Sum(nil)), nil
}

func openSQLiteReadOnly(ctx context.Context, path string) (*Database, error) {
	absolute, err := filepath.Abs(strings.TrimSpace(path))
	if err != nil || strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("SQLite path is invalid")
	}
	uri := (&url.URL{Scheme: "file", Path: absolute}).String()
	dsn := uri + "?mode=ro&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)"
	db, err := gorm.Open(glebarezsqlite.Open(dsn), gormConfig())
	if err != nil {
		return nil, fmt.Errorf("open SQLite read-only: %w", err)
	}
	return configureDatabase(ctx, db, "sqlite", 1, 1)
}

func validateSQLiteSnapshot(ctx context.Context, path string) error {
	database, err := openSQLiteReadOnly(ctx, path)
	if err != nil {
		return err
	}
	defer database.Close()
	sqlDB, err := database.db.DB()
	if err != nil {
		return err
	}
	quickRows, err := sqlDB.QueryContext(ctx, "PRAGMA quick_check")
	if err != nil {
		return fmt.Errorf("run SQLite quick_check: %w", err)
	}
	ok := false
	for quickRows.Next() {
		var result string
		if err := quickRows.Scan(&result); err != nil {
			_ = quickRows.Close()
			return fmt.Errorf("read SQLite quick_check: %w", err)
		}
		if result != "ok" {
			_ = quickRows.Close()
			return fmt.Errorf("SQLite quick_check failed")
		}
		ok = true
	}
	if err := quickRows.Close(); err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("SQLite quick_check returned no result")
	}

	foreignRows, err := sqlDB.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("run SQLite foreign_key_check: %w", err)
	}
	defer foreignRows.Close()
	if foreignRows.Next() {
		return fmt.Errorf("SQLite foreign_key_check failed")
	}
	if err := foreignRows.Err(); err != nil {
		return fmt.Errorf("read SQLite foreign_key_check: %w", err)
	}
	return nil
}

func sqliteSnapshotTableCounts(ctx context.Context, path string) (map[string]int64, error) {
	database, err := openSQLiteReadOnly(ctx, path)
	if err != nil {
		return nil, err
	}
	defer database.Close()
	counts := make(map[string]int64, len(schemaModels))
	for _, table := range migrationTableNames() {
		if !database.db.Migrator().HasTable(table) {
			counts[table] = 0
			continue
		}
		var count int64
		if err := database.db.WithContext(ctx).Table(table).Count(&count).Error; err != nil {
			return nil, fmt.Errorf("count SQLite snapshot table %s: %w", table, err)
		}
		counts[table] = count
	}
	return counts, nil
}

func requirePostgres18(ctx context.Context, database *gorm.DB) error {
	var version int
	if err := database.WithContext(ctx).Raw("SHOW server_version_num").Scan(&version).Error; err != nil {
		return fmt.Errorf("read PostgreSQL server version: %w", err)
	}
	if version/10000 != 18 {
		return fmt.Errorf("PostgreSQL 18 is required")
	}
	return nil
}

func requireEmptyPostgresTarget(ctx context.Context, database *gorm.DB, plan []migrationTable) error {
	known := make(map[string]bool, len(plan))
	for _, table := range plan {
		known[table.name] = true
	}
	var existing []string
	if err := database.WithContext(ctx).Raw(`
		SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = current_schema() AND table_type = 'BASE TABLE'
		ORDER BY table_name
	`).Scan(&existing).Error; err != nil {
		return fmt.Errorf("inspect PostgreSQL target schema: %w", err)
	}
	for _, table := range existing {
		if !known[table] {
			return fmt.Errorf("PostgreSQL target contains unknown table %s", table)
		}
	}
	for _, table := range plan {
		if !database.Migrator().HasTable(table.name) {
			continue
		}
		var count int64
		if err := database.WithContext(ctx).Table(table.name).Count(&count).Error; err != nil {
			return fmt.Errorf("count PostgreSQL target table %s: %w", table.name, err)
		}
		if table.name == "model_runtime_state" && count == 1 {
			var valid int64
			if err := database.WithContext(ctx).Table(table.name).Where("id = ?", 1).Count(&valid).Error; err != nil || valid != 1 {
				return fmt.Errorf("PostgreSQL model runtime singleton is invalid")
			}
			continue
		}
		if count != 0 {
			return fmt.Errorf("PostgreSQL target table %s is not empty", table.name)
		}
	}
	return nil
}

func resetPostgresSequence(tx *gorm.DB, table string) error {
	var sequence sql.NullString
	if err := tx.Raw("SELECT pg_get_serial_sequence(?, 'id')", table).Scan(&sequence).Error; err != nil {
		return fmt.Errorf("resolve PostgreSQL sequence for %s: %w", table, err)
	}
	if !sequence.Valid || sequence.String == "" {
		return fmt.Errorf("PostgreSQL sequence for %s is missing", table)
	}
	var maximum sql.NullInt64
	if err := tx.Raw("SELECT MAX(id) FROM " + quoteIdentifier(table)).Scan(&maximum).Error; err != nil {
		return fmt.Errorf("read PostgreSQL maximum id for %s: %w", table, err)
	}
	if maximum.Valid {
		if err := tx.Exec("SELECT setval(?::regclass, ?, true)", sequence.String, maximum.Int64).Error; err != nil {
			return fmt.Errorf("reset PostgreSQL sequence for %s: %w", table, err)
		}
		return nil
	}
	if err := tx.Exec("SELECT setval(?::regclass, 1, false)", sequence.String).Error; err != nil {
		return fmt.Errorf("reset empty PostgreSQL sequence for %s: %w", table, err)
	}
	return nil
}

func verifyPostgresConstraints(tx *gorm.DB, tables []string) error {
	type invalidConstraint struct {
		TableName string
		Name      string
	}
	var invalid []invalidConstraint
	if err := tx.Raw(`
		SELECT relation.relname AS table_name, constraint_record.conname AS name
		FROM pg_constraint constraint_record
		JOIN pg_class relation ON relation.oid = constraint_record.conrelid
		JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace
		WHERE namespace.nspname = current_schema()
			AND relation.relname IN ?
			AND constraint_record.convalidated = FALSE
		ORDER BY relation.relname, constraint_record.conname
	`, tables).Scan(&invalid).Error; err != nil {
		return fmt.Errorf("verify PostgreSQL constraints: %w", err)
	}
	if len(invalid) > 0 {
		return fmt.Errorf("PostgreSQL constraint %s on %s is not validated", invalid[0].Name, invalid[0].TableName)
	}
	return nil
}

func validatePostgresIntegerRange(value reflect.Value, table string) error {
	value = indirectValue(value)
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return fmt.Errorf("migration row for %s is invalid", table)
	}
	typeValue := value.Type()
	for index := 0; index < value.NumField(); index++ {
		fieldType := typeValue.Field(index)
		if isAssociationField(fieldType) {
			continue
		}
		field := indirectValue(value.Field(index))
		if !field.IsValid() {
			continue
		}
		switch field.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			if field.Uint() > math.MaxInt64 {
				return fmt.Errorf("unsigned integer in %s.%s exceeds PostgreSQL BIGINT", table, fieldType.Name)
			}
		}
	}
	return nil
}

func postgresParameterValue(value any) any {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() {
		return nil
	}
	if reflected.Kind() == reflect.Pointer {
		if reflected.IsNil() {
			return nil
		}
		if reflected.Elem().Kind() >= reflect.Uint && reflected.Elem().Kind() <= reflect.Uint64 {
			return int64(reflected.Elem().Uint())
		}
		return value
	}
	if reflected.Kind() >= reflect.Uint && reflected.Kind() <= reflect.Uint64 {
		return int64(reflected.Uint())
	}
	return value
}

func writeCanonicalRow(digest hash.Hash, value reflect.Value) {
	value = indirectValue(value)
	typeValue := value.Type()
	writeDigestString(digest, typeValue.Name())
	for index := 0; index < value.NumField(); index++ {
		fieldType := typeValue.Field(index)
		if isAssociationField(fieldType) {
			continue
		}
		writeDigestString(digest, fieldType.Name)
		writeCanonicalValue(digest, value.Field(index))
	}
	digest.Write([]byte{0xff})
}

func writeCanonicalValue(digest hash.Hash, value reflect.Value) {
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			digest.Write([]byte{0})
			return
		}
		digest.Write([]byte{1})
		writeCanonicalValue(digest, value.Elem())
		return
	}
	if value.Type() == timeValueType {
		instant := value.Interface().(time.Time).UTC().Truncate(time.Microsecond)
		writeDigestString(digest, instant.Format(time.RFC3339Nano))
		return
	}
	switch value.Kind() {
	case reflect.String:
		writeDigestString(digest, value.String())
	case reflect.Bool:
		if value.Bool() {
			digest.Write([]byte{1})
		} else {
			digest.Write([]byte{0})
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		writeDigestString(digest, strconv.FormatInt(value.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		writeDigestString(digest, strconv.FormatUint(value.Uint(), 10))
	case reflect.Float32:
		var encoded [4]byte
		binary.BigEndian.PutUint32(encoded[:], math.Float32bits(float32(value.Float())))
		digest.Write(encoded[:])
	case reflect.Float64:
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], math.Float64bits(value.Float()))
		digest.Write(encoded[:])
	default:
		panic(fmt.Sprintf("unsupported migration digest type %s", value.Type()))
	}
}

func writeDigestString(digest hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	digest.Write(length[:])
	digest.Write([]byte(value))
}

func indirectValue(value reflect.Value) reflect.Value {
	for value.IsValid() && (value.Kind() == reflect.Pointer || value.Kind() == reflect.Interface) {
		if value.IsNil() {
			return reflect.Value{}
		}
		value = value.Elem()
	}
	return value
}

func isAssociationField(field reflect.StructField) bool {
	return field.Type.Kind() == reflect.Pointer && field.Type.Elem().Kind() == reflect.Struct && field.Type.Elem() != timeValueType
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
