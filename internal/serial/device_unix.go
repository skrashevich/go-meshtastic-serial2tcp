//go:build !windows

package serial

import "os"

func DeviceExists(device string) bool {
	_, err := os.Stat(device)
	return err == nil
}
