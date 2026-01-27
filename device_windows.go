//go:build windows

package main

import (
	"os"
	"strings"

	"golang.org/x/sys/windows"
)

func deviceExists(device string) bool {
	_, name := normalizeWindowsDevice(device)
	if isCOMDevice(name) {
		return queryDosDevice(name)
	}

	_, err := os.Stat(device)
	return err == nil
}

func queryDosDevice(name string) bool {
	utf16Name, err := windows.UTF16PtrFromString(name)
	if err != nil {
		return false
	}

	for size := uint32(256); size <= 4096; size *= 2 {
		buf := make([]uint16, size)
		n, err := queryDosDeviceFn(utf16Name, &buf[0], size)
		if err == nil {
			return n > 0
		}
		if err != windows.ERROR_INSUFFICIENT_BUFFER {
			return false
		}
	}

	return false
}

var queryDosDeviceFn = windows.QueryDosDevice

func normalizeWindowsDevice(device string) (path string, name string) {
	if strings.HasPrefix(device, `\\.\`) {
		name = strings.TrimPrefix(device, `\\.\`)
		return device, name
	}

	trimmed := strings.TrimSuffix(device, ":")
	name = trimmed
	if isCOMDevice(name) {
		return `\\.\` + name, name
	}

	return device, name
}

func isCOMDevice(name string) bool {
	upper := strings.ToUpper(name)
	return strings.HasPrefix(upper, "COM") && len(upper) > 3
}
