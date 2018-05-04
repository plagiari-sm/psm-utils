package utils

import (
	"os"
	"testing"
)

func TestFallbackGetEnv(t *testing.T) {
	env := GetEnv("port", "8080")
	expectedValue := "8080"
	if env != expectedValue {
		t.Errorf("Incorrect key value, got: %s, expected: %s.", env, expectedValue)
	}
}

func TestGetEnv(t *testing.T) {
	var value string
	key := "port"
	expectedValue := "8081"
	os.Setenv(key, expectedValue)
	env := GetEnv(key, "8080")
	if env != expectedValue {
		t.Errorf("Incorrect value, got: %s, expected: %s.", env, expectedValue)
	}
	if value = os.Getenv(key); value != "" {
		os.Unsetenv(key)
		defer func() { os.Setenv(key, value) }()
	}
}
