//go:build !darwin && !linux

package service

import (
	"github.com/vkviyu/agent-temporal-engine/api"
	"os"
)

func checkSocketOwner(os.FileInfo) error { return apiError(api.ErrorUnsupported) }
