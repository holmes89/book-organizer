package books

import (
	"context"
	"github.com/holmes89/book-organizer/internal/documents"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"mime/multipart"
)

type BookService interface {
	FindAll(ctx context.Context) ([]*documents.Document, error)
	FindByID(ctx context.Context, id string) (*documents.Document, error)
	Add(ctx context.Context, file multipart.File, book *documents.Document) error
}

type service struct {
	docService documents.DocumentService
}

func NewBookService(docService documents.DocumentService) BookService {
	return &service{
		docService: docService,
	}
}

func (s *service) FindAll(ctx context.Context) ([]*documents.Document, error) {
	m := map[string]interface{}{"type": "book"}
	entities, err := s.docService.FindAll(ctx, m)
	if err != nil {
		logrus.WithError(err).Error("unable to fetch books from repository")
		return nil, errors.Wrap(err, "unable to fetch from repository")
	}
	return entities, nil
}

func (s *service) FindByID(ctx context.Context, id string) (*documents.Document, error) {
	entity, err := s.docService.FindByID(ctx, id)
	if err != nil {
		logrus.WithError(err).WithField("id", id).Error("unable to fetch book from repository")
		return nil, errors.Wrap(err, "unable to fetch from repository")
	}
	return entity, nil
}

func (s *service) Add(ctx context.Context, file multipart.File, book *documents.Document) error {
	book.Type = "book"

	if err := s.docService.Add(ctx, file, book); err != nil {
		logrus.WithError(err).Error("unable to save to repo")
		return errors.Wrap(err, "failed to store data in repo")
	}

	return nil
}
