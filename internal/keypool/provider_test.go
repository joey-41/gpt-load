package keypool

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"gpt-load/internal/encryption"
	"gpt-load/internal/models"
	"gpt-load/internal/store"
	"gpt-load/internal/types"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

const testGroupID = uint(7)

func TestSelectKeyIntervalZeroRotatesEveryRequest(t *testing.T) {
	provider, memStore := newTestProvider(t, nil)
	seedActiveKeys(t, provider, memStore, testGroupID, 1, 2, 3)

	first := mustSelectKey(t, provider, testGroupID, 0, false)
	second := mustSelectKey(t, provider, testGroupID, 0, false)
	third := mustSelectKey(t, provider, testGroupID, 0, false)

	if first.ID != 3 || second.ID != 2 || third.ID != 1 {
		t.Fatalf("unexpected rotation order: got [%d %d %d], want [3 2 1]", first.ID, second.ID, third.ID)
	}
}

func TestSelectKeyIntervalPinsAndExpires(t *testing.T) {
	provider, memStore := newTestProvider(t, nil)
	seedActiveKeys(t, provider, memStore, testGroupID, 1, 2, 3)

	first := mustSelectKey(t, provider, testGroupID, 5, false)
	second := mustSelectKey(t, provider, testGroupID, 5, false)
	if second.ID != first.ID {
		t.Fatalf("expected same key within interval, got %d then %d", first.ID, second.ID)
	}

	expireRotationState(t, memStore, testGroupID, first.ID, 301)

	third := mustSelectKey(t, provider, testGroupID, 5, false)
	if third.ID != 2 {
		t.Fatalf("expected rotation after interval expiry, got %d", third.ID)
	}
}

func TestSelectKeyForceRotateBypassesInterval(t *testing.T) {
	provider, memStore := newTestProvider(t, nil)
	seedActiveKeys(t, provider, memStore, testGroupID, 1, 2, 3)

	first := mustSelectKey(t, provider, testGroupID, 5, false)
	expireRotationState(t, memStore, testGroupID, first.ID, 1)

	second := mustSelectKey(t, provider, testGroupID, 5, true)
	if second.ID == first.ID {
		t.Fatalf("expected force rotate to change key, still got %d", second.ID)
	}
	if second.ID != 2 {
		t.Fatalf("expected force rotate to select next key 2, got %d", second.ID)
	}
}

func TestAddKeyDoesNotInterruptPinnedKeyWithinInterval(t *testing.T) {
	provider, memStore := newTestProvider(t, nil)
	seedActiveKeys(t, provider, memStore, testGroupID, 1, 2, 3)

	current := mustSelectKey(t, provider, testGroupID, 5, false)
	if err := provider.addKeyToStore(&models.APIKey{
		ID:        4,
		GroupID:   testGroupID,
		KeyValue:  "key-4",
		Status:    models.KeyStatusActive,
		CreatedAt: time.Unix(4, 0),
	}); err != nil {
		t.Fatalf("add key to store: %v", err)
	}

	reused := mustSelectKey(t, provider, testGroupID, 5, false)
	if reused.ID != current.ID {
		t.Fatalf("expected pinned key %d to remain active within interval, got %d", current.ID, reused.ID)
	}

	expireRotationState(t, memStore, testGroupID, current.ID, 301)
	next := mustSelectKey(t, provider, testGroupID, 5, false)
	if next.ID != 4 {
		t.Fatalf("expected newly appended key 4 after interval expiry, got %d", next.ID)
	}
}

func TestRemovingPinnedKeyClearsRotationState(t *testing.T) {
	provider, memStore := newTestProvider(t, nil)
	seedActiveKeys(t, provider, memStore, testGroupID, 1, 2, 3)

	current := mustSelectKey(t, provider, testGroupID, 5, false)
	if err := provider.removeKeyFromStore(current.ID, testGroupID); err != nil {
		t.Fatalf("remove pinned key: %v", err)
	}

	state, err := memStore.HGetAll(rotationStateKey(testGroupID))
	if err != nil {
		t.Fatalf("read rotation state: %v", err)
	}
	if len(state) != 0 {
		t.Fatalf("expected rotation state to be cleared, got %v", state)
	}

	next := mustSelectKey(t, provider, testGroupID, 5, false)
	if next.ID != 2 {
		t.Fatalf("expected next available key 2 after removing pinned key, got %d", next.ID)
	}
}

func TestHandleFailureClearsRotationStateForPinnedKey(t *testing.T) {
	db := newTestDB(t)
	provider, memStore := newTestProvider(t, db)
	seedActiveKeys(t, provider, memStore, testGroupID, 1, 2, 3)

	for _, id := range []uint{1, 2, 3} {
		if err := db.Create(&models.APIKey{
			ID:        id,
			GroupID:   testGroupID,
			KeyValue:  "key",
			Status:    models.KeyStatusActive,
			CreatedAt: time.Unix(int64(id), 0),
		}).Error; err != nil {
			t.Fatalf("seed db key %d: %v", id, err)
		}
	}

	current := mustSelectKey(t, provider, testGroupID, 5, false)
	group := &models.Group{
		ID: testGroupID,
		EffectiveConfig: types.SystemSettings{
			BlacklistThreshold: 10,
		},
	}

	if err := provider.handleFailure(current, group, keyHashKey(current.ID), activeKeysListKey(testGroupID)); err != nil {
		t.Fatalf("handle failure: %v", err)
	}

	state, err := memStore.HGetAll(rotationStateKey(testGroupID))
	if err != nil {
		t.Fatalf("read rotation state after failure: %v", err)
	}
	if len(state) != 0 {
		t.Fatalf("expected pinned rotation state to be cleared after failure, got %v", state)
	}

	next := mustSelectKey(t, provider, testGroupID, 5, false)
	if next.ID != 2 {
		t.Fatalf("expected next key 2 after failure-triggered reset, got %d", next.ID)
	}
}

func TestConcurrentSelectAtBoundaryUsesSingleNewWindowKey(t *testing.T) {
	provider, memStore := newTestProvider(t, nil)
	seedActiveKeys(t, provider, memStore, testGroupID, 1, 2, 3)

	current := mustSelectKey(t, provider, testGroupID, 5, false)
	expireRotationState(t, memStore, testGroupID, current.ID, 301)

	const goroutines = 20
	results := make(chan uint, goroutines)
	start := make(chan struct{})
	var wg sync.WaitGroup

	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results <- mustSelectKey(t, provider, testGroupID, 5, false).ID
		}()
	}

	close(start)
	wg.Wait()
	close(results)

	for id := range results {
		if id != 2 {
			t.Fatalf("expected all goroutines to reuse the same new window key 2, got %d", id)
		}
	}
}

func newTestProvider(t *testing.T, db *gorm.DB) (*KeyProvider, *store.MemoryStore) {
	t.Helper()

	memStore := store.NewMemoryStore()
	encryptionSvc, err := encryption.NewService("")
	if err != nil {
		t.Fatalf("create encryption service: %v", err)
	}

	return NewProvider(db, memStore, nil, encryptionSvc), memStore
}

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()

	db, err := gorm.Open(sqlite.Open("file:"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	if err := db.AutoMigrate(&models.APIKey{}); err != nil {
		t.Fatalf("migrate sqlite db: %v", err)
	}
	return db
}

func seedActiveKeys(t *testing.T, provider *KeyProvider, memStore *store.MemoryStore, groupID uint, keyIDs ...uint) {
	t.Helper()

	ids := make([]any, 0, len(keyIDs))
	for _, id := range keyIDs {
		apiKey := &models.APIKey{
			ID:        id,
			GroupID:   groupID,
			KeyValue:  "key",
			Status:    models.KeyStatusActive,
			CreatedAt: time.Unix(int64(id), 0),
		}
		if err := memStore.HSet(keyHashKey(id), provider.apiKeyToMap(apiKey)); err != nil {
			t.Fatalf("seed key hash %d: %v", id, err)
		}
		ids = append(ids, id)
	}
	if err := memStore.RPush(activeKeysListKey(groupID), ids...); err != nil {
		t.Fatalf("seed active keys: %v", err)
	}
}

func mustSelectKey(t *testing.T, provider *KeyProvider, groupID uint, intervalMinutes int, forceRotate bool) *models.APIKey {
	t.Helper()

	apiKey, err := provider.SelectKey(groupID, intervalMinutes, forceRotate)
	if err != nil {
		t.Fatalf("select key: %v", err)
	}
	return apiKey
}

func expireRotationState(t *testing.T, memStore *store.MemoryStore, groupID uint, currentKeyID uint, ageSeconds int64) {
	t.Helper()

	if err := memStore.HSet(rotationStateKey(groupID), map[string]any{
		"current_key_id": currentKeyID,
		"rotated_at":     time.Now().Unix() - ageSeconds,
	}); err != nil {
		t.Fatalf("expire rotation state: %v", err)
	}
}

func activeKeysListKey(groupID uint) string {
	return fmt.Sprintf("group:%d:active_keys", groupID)
}

func rotationStateKey(groupID uint) string {
	return fmt.Sprintf("group:%d:rotation_state", groupID)
}

func keyHashKey(keyID uint) string {
	return fmt.Sprintf("key:%d", keyID)
}
