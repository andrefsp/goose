package goose

import (
	"database/sql"
	"log"
)

// SkipTo migrates up to a specific version.
func SkipTo(db *sql.DB, dir string, version int64) error {
	migrations, err := CollectMigrations(dir, minVersion, version)
	if err != nil {
		return err
	}

	for {
		current, err := GetDBVersion(db)
		if err != nil {
			return err
		}

		next, err := migrations.Next(current)
		if err != nil {
			if err == ErrNoNextVersion {
				log.Printf("goose: no migrations to run. current version: %d\n", current)
				return nil
			}
			return err
		}

		if err = next.Skip(db); err != nil {
			return err
		}
	}
}

// Skip applies all available migrations.
func Skip(db *sql.DB, dir string) error {
	return SkipTo(db, dir, maxVersion)
}

// SkipOne migrates up by a single version.
func SkipOne(db *sql.DB, dir string) error {
	migrations, err := CollectMigrations(dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

	currentVersion, err := GetDBVersion(db)
	if err != nil {
		return err
	}

	next, err := migrations.Next(currentVersion)
	if err != nil {
		if err == ErrNoNextVersion {
			log.Printf("goose: no migrations to run. current version: %d\n", currentVersion)
		}
		return err
	}

	if err = next.Skip(db); err != nil {
		return err
	}

	return nil
}
