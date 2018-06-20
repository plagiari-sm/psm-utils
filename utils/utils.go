package utils

import (
	"os"
)

// GetEnv Get Environment Variable with Fallback Value
func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}
