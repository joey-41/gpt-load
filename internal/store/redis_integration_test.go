package store

import (
	"context"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisStoreSelectRotatingKey(t *testing.T) {
	dsn := os.Getenv("REDIS_TEST_DSN")
	if dsn == "" {
		t.Skip("REDIS_TEST_DSN is not set")
	}

	opts, err := redis.ParseURL(dsn)
	if err != nil {
		t.Fatalf("parse redis dsn: %v", err)
	}

	client := redis.NewClient(opts)
	t.Cleanup(func() {
		_ = client.Close()
	})

	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("ping redis: %v", err)
	}

	store := NewRedisStore(client)
	keyPrefix := "test:key-rotation:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	listKey := keyPrefix + ":list"
	stateKey := keyPrefix + ":state"
	t.Cleanup(func() {
		_ = store.Del(listKey, stateKey)
	})

	if err := store.RPush(listKey, 1, 2, 3); err != nil {
		t.Fatalf("seed list: %v", err)
	}

	now := time.Now().Unix()
	first, err := store.SelectRotatingKey(listKey, stateKey, 300, now, false)
	if err != nil {
		t.Fatalf("first select: %v", err)
	}
	if first != "3" {
		t.Fatalf("expected first rotated key 3, got %s", first)
	}

	second, err := store.SelectRotatingKey(listKey, stateKey, 300, now+1, false)
	if err != nil {
		t.Fatalf("second select: %v", err)
	}
	if second != first {
		t.Fatalf("expected second select to reuse %s, got %s", first, second)
	}

	if err := store.HSet(stateKey, map[string]any{
		"current_key_id": first,
		"rotated_at":     now - 301,
	}); err != nil {
		t.Fatalf("expire state: %v", err)
	}

	third, err := store.SelectRotatingKey(listKey, stateKey, 300, now+2, false)
	if err != nil {
		t.Fatalf("third select: %v", err)
	}
	if third != "2" {
		t.Fatalf("expected third select to rotate to 2, got %s", third)
	}

	forced, err := store.SelectRotatingKey(listKey, stateKey, 300, now+3, true)
	if err != nil {
		t.Fatalf("force rotate select: %v", err)
	}
	if forced != "1" {
		t.Fatalf("expected forced rotation to pick 1, got %s", forced)
	}
}
