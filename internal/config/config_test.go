package config

import (
	"os"
	"testing"
)

func TestEnvHelpers(t *testing.T) {
	t.Setenv("TEST_INT", "42")
	t.Setenv("TEST_INT_BAD", "nope")
	t.Setenv("TEST_BOOL_TRUE", "true")
	t.Setenv("TEST_BOOL_FALSE", "0")
	t.Setenv("TEST_BOOL_BAD", "maybe")

	if got := getenvInt("TEST_INT", 7); got != 42 {
		t.Fatalf("getenvInt valid: got %d want 42", got)
	}
	if got := getenvInt("TEST_INT_BAD", 7); got != 7 {
		t.Fatalf("getenvInt invalid: got %d want 7", got)
	}
	if got := getenvBool("TEST_BOOL_TRUE", false); got != true {
		t.Fatalf("getenvBool true: got %v want true", got)
	}
	if got := getenvBool("TEST_BOOL_FALSE", true); got != false {
		t.Fatalf("getenvBool false: got %v want false", got)
	}
	if got := getenvBool("TEST_BOOL_BAD", true); got != true {
		t.Fatalf("getenvBool invalid: got %v want true", got)
	}
	if got := getenv("TEST_MISSING", "fallback"); got != "fallback" {
		t.Fatalf("getenv fallback: got %q want fallback", got)
	}
}

func TestNormalizePositiveInt(t *testing.T) {
	if got := normalizePositiveInt("x", 3, 7); got != 3 {
		t.Fatalf("normalizePositiveInt valid: got %d want 3", got)
	}
	if got := normalizePositiveInt("x", 0, 7); got != 7 {
		t.Fatalf("normalizePositiveInt invalid: got %d want 7", got)
	}
}

func TestSanitizeDeviceName(t *testing.T) {
	if got := sanitizeDeviceName("/dev/tty.usb0"); got != "_dev_tty_usb0" {
		t.Fatalf("sanitizeDeviceName: got %q", got)
	}
}

func TestFlagPassed(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{"equals form", []string{"prog", "-web-ui-addr=0.0.0.0:9080"}, true},
		{"space form", []string{"prog", "-web-ui-addr", "0.0.0.0:9080"}, true},
		{"absent", []string{"prog", "-debug"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			old := os.Args
			t.Cleanup(func() { os.Args = old })
			os.Args = tt.args
			if got := flagPassed("web-ui-addr"); got != tt.want {
				t.Fatalf("flagPassed: got %v want %v", got, tt.want)
			}
		})
	}
}
