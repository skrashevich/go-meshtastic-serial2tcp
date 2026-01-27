//go:build !windows

package main

import "os"

func deviceExists(device string) bool {
	_, err := os.Stat(device)
	return err == nil
}
