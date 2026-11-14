package service

import "github.com/vkviyu/agent-temporal-engine/protocol"

type responseBuilder func(error, any) protocol.Response
type successBuilder func(any) protocol.Response
