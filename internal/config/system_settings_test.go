package config

import (
	"testing"

	"gorm.io/datatypes"
)

func TestGetEffectiveConfigAppliesKeyRotationIntervalOverride(t *testing.T) {
	sm := NewSystemSettingsManager()

	settings := sm.GetEffectiveConfig(datatypes.JSONMap{
		"key_rotation_interval_minutes": 5,
	})

	if settings.KeyRotationIntervalMinutes != 5 {
		t.Fatalf("expected group override to set key rotation interval to 5, got %d", settings.KeyRotationIntervalMinutes)
	}
}
