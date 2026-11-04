package config

import (
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	Addr     string
	Database string
	Redis    RedisConfig
	Storage  StorageConfig
}

type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

type StorageConfig struct {
	Endpoint  string // e.g. https://s3.us-west-004.backblazeb2.com
	Region    string // e.g. us-west-004
	Bucket    string
	AccessKey string
	SecretKey string
	Prefix    string // Option
	PublicURL string
}

func Load() Config {
	_ = godotenv.Load()

	return Config{
		Addr:     getEnv("ADDR", ":8080"),
		Database: getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/imagelib?sslmode=disable"),
		Redis: RedisConfig{
			Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
			Password: os.Getenv("REDIS_PASSWORD"),
		},
		Storage: StorageConfig{
			Endpoint:  getEnv("S3_ENDPOINT", "https://s3.us-west-004.backblazeb2.com"),
			Region:    getEnv("S3_REGION", "us-west-004"),
			Bucket:    os.Getenv("S3_BUCKET"),
			AccessKey: os.Getenv("B2_APPLICATION_KEY_ID"),
			SecretKey: os.Getenv("B2_APPLICATION_KEY"),
			Prefix:    os.Getenv("S3_PREFIX"),
			PublicURL: os.Getenv("S3_PUBLIC_URL"),
		},
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
