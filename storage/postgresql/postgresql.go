package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"read-adviser-bot/lib/e"
	"read-adviser-bot/storage"
)

type Storage struct {
	db *sql.DB
}

// New creates new PostgreSQL storage.
func New(dbInfo string) (*Storage, error) {
	db, err := sql.Open("postgres", dbInfo)
	if err != nil {
		return nil, e.Wrap("can't open database", err)
	}

	if err := db.Ping(); err != nil {
		return nil, e.Wrap("can't connect to the database", err)
	}

	return &Storage{db: db}, nil
}

// Save saves page to the storage.
func (s *Storage) Save(ctx context.Context, p *storage.Page) error {
	q := `INSERT INTO pages (user_name, url) VALUES ($1, $2);`

	if _, err := s.db.ExecContext(ctx, q, p.UserName, p.URL); err != nil {
		return e.Wrap("can't save page", err)
	}

	return nil
}

// PickRandom picks random page from the storage.
func (s *Storage) PickRandom(ctx context.Context, userName string) (*storage.Page, error) {
	q := `SELECT url FROM pages WHERE user_name = $1 ORDER BY RANDOM() LIMIT 1;`

	var url string
	err := s.db.QueryRowContext(ctx, q, userName).Scan(&url)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, storage.ErrNoSavedPages
	}
	if err != nil {
		return nil, e.Wrap("can't pick random page", err)
	}

	return &storage.Page{
		URL:      url,
		UserName: userName,
	}, nil
}

// Remove removes page from the storage.
func (s *Storage) Remove(ctx context.Context, p *storage.Page) error {
	q := `DELETE FROM pages WHERE user_name = $1 AND url = $2;`

	if _, err := s.db.ExecContext(ctx, q, p.UserName, p.URL); err != nil {
		return e.Wrap("can't remove page", err)
	}

	return nil
}

// IsExists checks if page exists in the storage.
func (s *Storage) IsExists(ctx context.Context, p *storage.Page) (bool, error) {
	q := `SELECT COUNT(*) FROM pages WHERE user_name = $1 AND url = $2;`

	var count int
	if err := s.db.QueryRowContext(ctx, q, p.UserName, p.URL).Scan(&count); err != nil {
		return false, e.Wrap("can't check if page exists", err)
	}

	return count > 0, nil
}

func (s *Storage) Init(ctx context.Context) error {
	q := `CREATE TABLE IF NOT EXISTS pages (user_name TEXT, url TEXT);`

	if _, err := s.db.ExecContext(ctx, q); err != nil {
		return e.Wrap("can't create table", err)
	}

	return nil
}
