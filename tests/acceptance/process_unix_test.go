//go:build acceptance && !windows

package acceptance

import (
	"os"
	"syscall"
)

// freezeProcess stops a process from running without ending it, which is what
// a long garbage collection pause, a stalled disk or a suspended virtual
// machine looks like to the rest of the cluster.
func freezeProcess(p *os.Process) error { return p.Signal(syscall.SIGSTOP) }

// thawProcess lets a frozen process run again.
func thawProcess(p *os.Process) error { return p.Signal(syscall.SIGCONT) }
