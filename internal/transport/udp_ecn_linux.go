//go:build linux

package transport

import "golang.org/x/sys/unix"

const udpECNTOS = unix.IP_TOS
