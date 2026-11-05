//go:build darwin || linux

package service

import (
	"os"
	"syscall"
)

func checkSocketOwner(info os.FileInfo) error {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != uint32(os.Geteuid()) {
		return ErrUnauthorized
	}
	return nil
}
