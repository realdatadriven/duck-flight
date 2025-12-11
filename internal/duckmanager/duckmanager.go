package duckmanager

import (
	"database/sql"
	"fmt"
	"log"
	"strings"

	// duckdb driver registers itself with database/sql
	_ "github.com/duckdb/duckdb-go/v2"

	"github.com/realdatadriven/duck-flight/internal/config"
)

// DuckManager manages a single DuckDB instance and executes lifecycle SQL for each schema
type DuckManager struct {
	db  *sql.DB
	cfg *config.ServerConfig
}

// NewDuckManager opens an in-memory DuckDB instance (or file-backed if you provide DSN in cfg later).
// For now it opens an in-memory DB (empty DSN).
func NewDuckManager(cfg *config.ServerConfig) (*DuckManager, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}

	// sanity ping
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &DuckManager{db: db, cfg: cfg}, nil
}

// ExecMulti executes semicolon separated SQL statements in sequence
func (m *DuckManager) ExecMulti(raw string) error {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	// naive split by semicolon -- this is fine for simple DDL/DML snippets.
	parts := strings.Split(raw, ";")
	for _, p := range parts {
		stmt := strings.TrimSpace(p)
		if stmt == "" {
			continue
		}
		if _, err := m.db.Exec(stmt); err != nil {
			return fmt.Errorf("exec failed for statement '%s': %w", stmt, err)
		}
	}
	return nil
}

// Startup runs before_sql and main_sql for every schema
func (m *DuckManager) Startup() error {
	for _, s := range m.cfg.Schemas {
		log.Printf("[duckmanager] setting up schema: %s", s.Name)
		if err := m.ExecMulti(s.BeforeSQL); err != nil {
			return fmt.Errorf("before_sql failed for %s: %w", s.Name, err)
		}
		// fmt.Println("Executing main_sql for schema:", s.Name, s.MainSQL)
		if err := m.ExecMulti(s.MainSQL); err != nil {
			return fmt.Errorf("main_sql failed for %s: %w", s.Name, err)
		}
	}
	return nil
}

// Shutdown runs after_sql for every schema and closes the DB
func (m *DuckManager) Shutdown() error {
	for _, s := range m.cfg.Schemas {
		log.Printf("[duckmanager] tearing down schema: %s", s.Name)
		if err := m.ExecMulti(s.AfterSQL); err != nil {
			log.Printf("warning: after_sql failed for %s: %v", s.Name, err)
			// continue executing other teardowns
		}
	}
	if err := m.db.Close(); err != nil {
		return err
	}
	return nil
}

// DB returns the underlying *sql.DB for advanced usage (discovery, queries)
func (m *DuckManager) DB() *sql.DB { return m.db }

// Config exposes the loaded ServerConfig (schemas etc).
func (m *DuckManager) Config() *config.ServerConfig { return m.cfg }
