package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"

	"imagelib-s3-dynamic/internal/config"
)

// ObjectNamer maps object keys to S3 object keys and public URLs. It is shared by
// the proxy (which only needs URLs) and the uploader.
type ObjectNamer struct {
	prefix    string
	publicURL string
}

func NewObjectNamer(cfg config.StorageConfig) ObjectNamer {
	return ObjectNamer{prefix: cfg.Prefix, publicURL: cfg.PublicURL}
}

// ObjectKey returns the bucket key for an object key.
func (n ObjectNamer) ObjectKey(key string) string {
	if n.prefix == "" {
		return key
	}
	return n.prefix + "/" + key
}

// PublicURL returns the public URL for an object key, escaping each path segment.
func (n ObjectNamer) PublicURL(key string) string {
	segs := strings.Split(n.ObjectKey(key), "/")
	for i, s := range segs {
		segs[i] = url.PathEscape(s)
	}
	return n.publicURL + "/" + strings.Join(segs, "/")
}

// S3 uploads and deletes objects in the hot tier bucket.
type S3 struct {
	ObjectNamer
	client *s3.Client
	bucket string
}

func NewS3(cfg config.StorageConfig) *S3 {
	// Bound every phase except the (size-dependent) body transfer, so a hung bucket can't
	// stall list/delete calls indefinitely. Uploads get their own size-based context timeout.
	httpClient := awshttp.NewBuildableClient().
		WithDialerOptions(func(d *net.Dialer) { d.Timeout = 5 * time.Second }).
		WithTransportOptions(func(t *http.Transport) { t.ResponseHeaderTimeout = 30 * time.Second })
	client := s3.New(s3.Options{
		HTTPClient:   httpClient,
		BaseEndpoint: aws.String(cfg.Endpoint),
		Region:       cfg.Region,
		Credentials:  credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		UsePathStyle: cfg.ForcePathStyle,
		// S3-compatible providers (B2, MinIO, R2...) don't all support the newer default
		// CRC checksums; only send them when an operation requires it.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})
	return &S3{
		ObjectNamer: NewObjectNamer(cfg),
		client:      client,
		bucket:      cfg.Bucket,
	}
}

// Put uploads exactly size bytes from body.
func (s *S3) Put(ctx context.Context, key string, body io.ReaderAt, size int64, contentType string) error {
	in := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(s.ObjectKey(key)),
		Body:          io.NewSectionReader(body, 0, size),
		ContentLength: aws.Int64(size),
		ContentType:   aws.String(contentType),
	}
	if _, err := s.client.PutObject(ctx, in); err != nil {
		return fmt.Errorf("s3 put %s: %w", key, err)
	}
	return nil
}

// List calls fn for every object under the prefix, with the prefix stripped from keys.
func (s *S3) List(ctx context.Context, fn func(key string, size int64, modified time.Time) error) error {
	prefix := ""
	if s.prefix != "" {
		prefix = s.prefix + "/"
	}
	p := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(prefix),
	})
	for p.HasMorePages() {
		page, err := p.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("s3 list: %w", err)
		}
		for _, o := range page.Contents {
			key := strings.TrimPrefix(aws.ToString(o.Key), prefix)
			if key == "" {
				continue
			}
			if err := fn(key, aws.ToInt64(o.Size), aws.ToTime(o.LastModified)); err != nil {
				return err
			}
		}
	}
	return nil
}

// Delete removes an object. Missing objects are not an error.
func (s *S3) Delete(ctx context.Context, key string) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(s.ObjectKey(key)),
	})
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && (apiErr.ErrorCode() == "NoSuchKey" || apiErr.ErrorCode() == "NotFound") {
		return nil
	}
	if err != nil {
		return fmt.Errorf("s3 delete %s: %w", key, err)
	}
	return nil
}
