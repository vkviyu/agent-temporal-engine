package api_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/vkviyu/agent-temporal-engine/api"
)

// Old emission envelopes are deliberately built independently from the
// production encoder. Clean V1 does not decode or re-issue those protocols:
// the only public command envelope is schema 3.
func TestEmissionSchemaThreeRoundTripAndRejectsLegacy(t *testing.T) {
	body := []byte(`{"claim_authority":{"Deployment":"","OwnerEpoch":0,"Incarnation":0,"Holder":""},"revoke_id":"grant","revoke_version":3}`)
	currentCommand := api.EmissionCommand{SchemaVersion: 3, RevokeID: "grant", RevokeVersion: 3}
	current, err := api.EncodeEmissionCommand(currentCommand)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := api.DecodeEmissionCommand(current)
	if err != nil || decoded.SchemaVersion != 3 || decoded.RevokeID != currentCommand.RevokeID {
		t.Fatalf("current decode=%+v %v", decoded, err)
	}
	if !bytes.Equal(current, mustEncodeEmission(t, decoded)) {
		t.Fatal("schema-3 command is not canonical")
	}
	for _, version := range []uint64{1, 2} {
		for _, command := range []api.EmissionCommand{{SchemaVersion: version, RevokeID: "grant", RevokeVersion: 3}, {SchemaVersion: version, Issue: &api.EmissionSpec{ID: "grant"}}} {
			if _, err := api.EncodeEmissionCommand(command); !errors.Is(err, api.ErrInvalidRequest) {
				t.Fatalf("schema %d encode err=%v, want invalid request", version, err)
			}
		}
		legacy := append([]byte("ATE-EMISSION\x00"), byte(version))
		legacy = binary.AppendUvarint(legacy, uint64(len(body)))
		legacy = append(legacy, body...)
		if _, err := api.DecodeEmissionCommand(legacy); !errors.Is(err, api.ErrInvalidRequest) {
			t.Fatalf("schema %d decode err=%v, want invalid request", version, err)
		}
	}
	unknown := append([]byte(nil), current...)
	unknown[len("ATE-EMISSION\x00")] = 4
	if _, err := api.DecodeEmissionCommand(unknown); !errors.Is(err, api.ErrInvalidRequest) {
		t.Fatal("unknown envelope schema accepted")
	}
}

func mustEncodeEmission(t *testing.T, c api.EmissionCommand) []byte {
	t.Helper()
	raw, err := api.EncodeEmissionCommand(c)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
