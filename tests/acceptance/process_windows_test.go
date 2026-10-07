//go:build acceptance && windows

package acceptance

import (
	"fmt"
	"os"
	"syscall"
)

const processSuspendResume = 0x0800

var (
	ntdll            = syscall.NewLazyDLL("ntdll.dll")
	ntSuspendProcess = ntdll.NewProc("NtSuspendProcess")
	ntResumeProcess  = ntdll.NewProc("NtResumeProcess")
)

func withProcessHandle(p *os.Process, call *syscall.LazyProc) error {
	handle, err := syscall.OpenProcess(processSuspendResume, false, uint32(p.Pid))
	if err != nil {
		return fmt.Errorf("open process %d: %w", p.Pid, err)
	}
	defer syscall.CloseHandle(handle)
	if status, _, _ := call.Call(uintptr(handle)); status != 0 {
		return fmt.Errorf("%s on process %d: status 0x%x", call.Name, p.Pid, status)
	}
	return nil
}

// freezeProcess stops a process from running without ending it, which is what
// a long garbage collection pause, a stalled disk or a suspended virtual
// machine looks like to the rest of the cluster.
func freezeProcess(p *os.Process) error { return withProcessHandle(p, ntSuspendProcess) }

// thawProcess lets a frozen process run again.
func thawProcess(p *os.Process) error { return withProcessHandle(p, ntResumeProcess) }
