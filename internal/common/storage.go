package common

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	_ "gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
	"io"
	"net/url"
	"time"
)

type DocumentSave interface {
	Save(ctx context.Context, fileName string, reader io.Reader) (path string, err error)
}

type BackupSave interface {
	Save(ctx context.Context, fileName string, reader io.Reader) (path string, err error)
}

type DocumentGet interface {
	Get(ctx context.Context, path string) (string, error)
	List(ctx context.Context) <-chan string
}

type BackupReader interface {
	Reader(ctx context.Context, path string) (io.ReadCloser, error)
}

type DocumentStorage interface {
	DocumentSave
	DocumentGet
}

type BackupStorage interface {
	BackupSave
	BackupReader
}

type BucketStorage struct {
	Bucket *blob.Bucket
}

func NewBucketStorage(config BucketConfig) *BucketStorage {
	bucket, err := blob.OpenBucket(context.Background(), config.ConnectionString)
	if err != nil {
		logrus.WithError(err).Fatal("unable to connect to bucket")
	}
	return &BucketStorage{
		Bucket: bucket,
	}
}

// TODO redo this to pass in variables
func NewGCPBucketStorage(config BucketConfig) *BucketStorage {
	ctx := context.Background()

	urlString := config.ConnectionString
	urlParts, _ := url.Parse(urlString)
	// Your GCP credentials.
	// See https://cloud.google.com/docs/authentication/production
	// for more info on alternatives.
	creds, err := gcp.DefaultCredentials(ctx)
	if err != nil {
		logrus.Fatal(err)
	}

	accessID := config.AccessID
	accessKey := config.AccessKey

	if accessID == "" || accessKey == "" {
		logrus.Warn("unable to find access information using default credentials")
		credsMap := make(map[string]string)
		json.Unmarshal(creds.JSON, &credsMap)
		accessID = credsMap["client_id"]
		accessKey = credsMap["private_key"]
	}

	opts := &gcsblob.Options{
		GoogleAccessID: accessID,
		PrivateKey:     []byte(accessKey),
	}
	// Create an HTTP client.
	// This example uses the default HTTP transport and the credentials
	// created above.
	client, err := gcp.NewHTTPClient(
		gcp.DefaultTransport(),
		gcp.CredentialsTokenSource(creds))
	if err != nil {
		logrus.Fatal(err)
	}

	// Create a *blob.Bucket.
	bucket, err := gcsblob.OpenBucket(ctx, client, urlParts.Host, opts)
	if err != nil {
		logrus.Fatal(err)
	}
	return &BucketStorage{
		Bucket: bucket,
	}
}

func NewBucketDocumentStorage(storage *BucketStorage) DocumentStorage {
	return storage
}

func NewBackupStorage(storage *BucketStorage) BackupStorage {
	return storage
}

func (s *BucketStorage) Save(ctx context.Context, fileName string, reader io.Reader) (path string, err error) {

	w, err := s.Bucket.NewWriter(ctx, fileName, nil)
	if err != nil {
		logrus.WithError(err).Error("unable to create upload writer")
		return "", errors.Wrap(err, "unable to creat upload writer")
	}

	defer w.Close()

	if _, err := io.Copy(w, reader); err != nil {
		logrus.WithError(err).Error("failed to upload file")
		return "", errors.Wrap(err, "failed to upload file")
	}

	return fileName, err //TODO allow for custom directory?
}

func (s *BucketStorage) Get(ctx context.Context, path string) (string, error) {
	opts := &blob.SignedURLOptions{
		Expiry: 15 * time.Hour,
		Method: "GET",
	}
	return s.Bucket.SignedURL(ctx, path, opts)
}

func (s *BucketStorage) Reader(ctx context.Context, path string) (io.ReadCloser, error) {
	return s.Bucket.NewReader(ctx, path, nil)
}

func (s *BucketStorage) List(ctx context.Context) <-chan string {
	opts := &blob.ListOptions{}
	iter := s.Bucket.List(opts)
	out := make(chan string)
	go func() {
		defer close(out)
		bgctx := context.Background()
		for {
			for {
				obj, err := iter.Next(bgctx)
				if err == io.EOF {
					logrus.Info("file search complete")
					return
				}
				if err != nil {
					logrus.WithError(err).Error("unable to fetch object")
					return
				}
				if obj.IsDir {
					logrus.WithField("name", obj.Key).Debug("directory found")
					continue
				}
				out <- obj.Key
			}
		}
	}()
	return out
}
