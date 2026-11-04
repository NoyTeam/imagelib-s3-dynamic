package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
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
	client       *s3.Client
	bucket       string
	cacheControl string
}

func NewS3(cfg config.StorageConfig) *S3 {
	client := s3.New(s3.Options{
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
		ObjectNamer:  NewObjectNamer(cfg),
		client:       client,
		bucket:       cfg.Bucket,
		cacheControl: cfg.CacheControl,
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
	if s.cacheControl != "" {
		in.CacheControl = aws.String(s.cacheControl)
	}
	if _, err := s.client.PutObject(ctx, in); err != nil {
		return fmt.Errorf("s3 put %s: %w", key, err)
	}
	return nil
}

// Delete removes an object. Missing objects are not an error.
func (s *S3) Delete(ctx context.Context, key string) error {
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
