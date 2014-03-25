package statsdclient

import (
	"testing"
)

func TestMakePrefix(t *testing.T) {
	prefix := MakePrefix("test", "statsdclient", "test-001.example.com")
	expectedPrefix := "test.statsdclient.test-001_example_com."
	if prefix != expectedPrefix {
		t.Errorf("expected %q, got %q", expectedPrefix, prefix)
	}
}
