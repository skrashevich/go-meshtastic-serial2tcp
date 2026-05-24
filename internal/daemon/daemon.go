package daemon

import (
	"fmt"
	"os"
	"os/exec"
)

const envDaemonStarted = "MESHTASTIC_SERIAL2TCP_DAEMON"

// IsChild reports whether the current process was spawned by Daemonize.
func IsChild() bool {
	return os.Getenv(envDaemonStarted) == "1"
}

// Daemonize re-executes the current binary detached from the controlling
// terminal and returns after the child has been started. The caller is
// expected to exit the parent process.
func Daemonize() error {
	exe, err := os.Executable()
	if err != nil {
		exe = os.Args[0]
	}

	cmd := exec.Command(exe, os.Args[1:]...)
	cmd.Env = append(os.Environ(), envDaemonStarted+"=1")
	cmd.Stdin = nil
	cmd.Stdout = nil
	cmd.Stderr = nil
	applySysProcAttr(cmd)

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("start daemon: %w", err)
	}

	fmt.Printf("Daemon started with PID %d\n", cmd.Process.Pid)
	if err := cmd.Process.Release(); err != nil {
		return fmt.Errorf("release daemon process: %w", err)
	}
	return nil
}
