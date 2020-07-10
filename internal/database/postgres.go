package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	sq "github.com/Masterminds/squirrel"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/holmes89/book-organizer/internal/common"
	"github.com/holmes89/book-organizer/internal/documents"
	_ "github.com/lib/pq" // Used for specifying the type client we are creating
	"github.com/sirupsen/logrus"
	"go.uber.org/fx"
	"strings"
	"time"
)

type PostgresDatabase struct {
	conn *sql.DB
}

func NewPostgresDatabase(lc fx.Lifecycle, config common.PostgresDatabaseConfig) documents.DocumentRepository {
	logrus.Info("connecting to postgres")
	db, err := retryPostgres(3, 10*time.Second, func() (db *sql.DB, e error) {
		return sql.Open("postgres", config.ConnectionString)
	})
	if err != nil {
		logrus.WithError(err).Fatal("unable to connect to postgres")
	}
	logrus.Info("connected to postgres")
	psqldb := &PostgresDatabase{db}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			logrus.Info("closing connection for postgres")
			return db.Close()
		},
	})

	migrateDB(config)

	return psqldb
}

func migrateDB(config common.PostgresDatabaseConfig) {
	db, err := sql.Open("postgres", config.ConnectionString)
	if err != nil {
		logrus.WithError(err).Fatal("unable to connect to postgres to migrate")
	}
	driver, err := postgres.WithInstance(db, &postgres.Config{})
	if err != nil {
		logrus.WithError(err).Fatal("unable to get driver to migrate")
	}

	m, err := migrate.NewWithDatabaseInstance(
		"file://migrations",
		"mind", driver)
	if err != nil {
		logrus.WithError(err).Fatal("unable to create migration instance")
	}
	if err := m.Up(); err != nil {
		if err != migrate.ErrNoChange {
			logrus.WithError(err).Fatal("unable to migrate")
		}
		logrus.Info("no migrations to run")
	}
}

func retryPostgres(attempts int, sleep time.Duration, callback func() (*sql.DB, error)) (*sql.DB, error) {
	for i := 0; i <= attempts; i++ {
		conn, err := callback()
		if err == nil {
			return conn, nil
		}
		time.Sleep(sleep)

		logrus.WithError(err).Error("error connecting to postgres, retrying")
	}
	return nil, fmt.Errorf("after %d attempts, connection failed", attempts)
}

func (r *PostgresDatabase) FindAll(ctx context.Context, filter map[string]interface{}) (docs []*documents.Document, err error) {
	docs = []*documents.Document{}
	ps := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	rows, err := ps.Select("documents.id", "description", "display_name", "name", "type", "path", "COALESCE(string_agg(tagged_resources.id::character varying, ','), '')", "created", "updated").
		From("documents").
		LeftJoin("tagged_resources ON documents.id=tagged_resources.resource_id").
		Suffix("GROUP BY documents.id ORDER BY display_name ASC").
		Where(filter).RunWith(r.conn).Query()

	if err != nil {
		logrus.WithError(err).Error("unable to fetch results")
		return nil, errors.New("unable to fetch results")
	}
	for rows.Next() {
		doc := &documents.Document{}
		var tagList string
		doc.Tags = []string{}
		if err := rows.Scan(&doc.ID, &doc.Description, &doc.DisplayName, &doc.Name, &doc.Type, &doc.Path, &tagList, &doc.Created, &doc.Updated); err != nil {
			logrus.WithError(err).Warn("unable to scan doc results")
		}
		if tagList != "" {
			doc.Tags = append(doc.Tags, strings.Split(tagList, ",")...)
		}
		docs = append(docs, doc)
	}
	return docs, nil
}

func (r *PostgresDatabase) FindByID(ctx context.Context, id string) (*documents.Document, error) {
	ps := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	row := ps.Select("documents.id", "description", "display_name", "name", "type", "path", "COALESCE(string_agg(tagged_resources.id::character varying, ','), '')", "created", "updated").
		From("documents").
		LeftJoin("tagged_resources ON documents.id=tagged_resources.resource_id").
		Suffix("GROUP BY documents.id").
		Where(sq.Eq{"documents.id": id}).RunWith(r.conn).QueryRow()
	doc := &documents.Document{}
	var tagList string
	doc.Tags = []string{}
	if err := row.Scan(&doc.ID, &doc.Description, &doc.DisplayName, &doc.Name, &doc.Type, &doc.Path, &tagList, &doc.Created, &doc.Updated); err != nil {
		logrus.WithError(err).Warn("unable to scan doc results")
	}
	if tagList != "" {
		doc.Tags = append(doc.Tags, strings.Split(tagList, ",")...)
	}

	return doc, nil
}

func (r *PostgresDatabase) UpdateDocument(_ context.Context, doc documents.Document) (result documents.Document, err error) {
	ps := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	_, err = ps.Update("documents").SetMap(
		map[string]interface{}{
			"description":  doc.Description,
			"display_name": doc.DisplayName,
			"type":         doc.Type,
			"updated":      time.Now()}).
		Where(sq.Eq{"id": doc.ID}).RunWith(r.conn).Exec()

	if err != nil {
		logrus.WithError(err).Error("unable to update doc")
		return result, errors.New("unable to update doc")
	}

	return doc, nil
}

func (r *PostgresDatabase) existsByPath(ctx context.Context, path string) (bool, error) {
	ps := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	row := ps.Select("count(id)").
		From("documents").Where(sq.Eq{"path": path}).RunWith(r.conn).QueryRow()
	var count int
	if err := row.Scan(&count); err != nil {
		logrus.WithError(err).Warn("unable to scan doc results")
	}

	return count > 0, nil
}

func (r *PostgresDatabase) Insert(ctx context.Context, doc *documents.Document) error {
	ps := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	if _, err := ps.Insert("documents").Columns("id", "description", "display_name", "name", "type", "path").
		Values(doc.ID, doc.Description, doc.DisplayName, doc.Name, doc.Type, doc.Path).
		RunWith(r.conn).
		Exec(); err != nil {
		logrus.WithError(err).Warn("unable to insert doc")
		return errors.New("unable to insert doc metadata")
	}
	return nil
}

func (r *PostgresDatabase) UpsertStream(ctx context.Context, input <-chan *documents.Document) error {
	count := 0
	for doc := range input {
		bctx := context.Background()
		if exists, _ := r.existsByPath(bctx, doc.Path); exists {
			continue
		}
		if err := r.Insert(bctx, doc); err != nil {
			logrus.WithError(err).Info("unable to upsert document")
			return errors.New("unable to upsert document")
		}
		count++
	}
	logrus.WithField("count", count).Info("documents added")
	return nil
}

func (r *PostgresDatabase) Delete(ctx context.Context, id string) error {
	ps := sq.StatementBuilder.PlaceholderFormat(sq.Dollar)
	if _, err := ps.Delete("documents").Where(sq.Eq{"id": id}).RunWith(r.conn).Exec(); err != nil {
		logrus.WithError(err).Warn("unable to scan doc results")
		return errors.New("unable to delete")
	}

	return nil
}
