"""Build Control 5, ContextPacket 4 and ToolDefinition 1 independently of Go."""

import hashlib
import json
from pathlib import Path


def uint(value):
    out = bytearray()
    while value >= 128:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)


def blob(raw):
    return uint(len(raw)) + raw


def fields(*values):
    return b"".join(uint(value) if isinstance(value, int) else blob(value.encode()) for value in values)


def identity(domain, data):
    return hashlib.sha256(domain + data).hexdigest()


authority = fields("deployment", 2, 3, "holder")
activity, recipient, namespace, key = "activity", "recipient", "tool/v1", "operation-key"
request_digest = "a" * 64
contract = fields(
    "contract/v1", "b" * 64, recipient, True, False, True, namespace, 500,
    "reject", "strong", "supported", "supported", "before settlement", "receiver_epoch",
)
contract_id = identity(b"ATE-EFFECT-CONTRACT\x00\x01", contract)
spec = fields(
    activity, "e", "", "external_tool", 1, "invocation", 1,
    "store", 7, "c" * 64, "", request_digest, "d" * 64,
    recipient, "idempotent", key, "resource",
) + contract

guarantee = fields(
    activity, 1, "active", contract_id, request_digest, namespace, key, "epoch",
    1, "first-attempt", 2, 1_000, 1_400, "receiver_epoch", "e" * 64, "f" * 64,
) + authority
guarantee_id = identity(b"ATE-IDEMPOTENCY-GUARANTEE\x00\x01", fields("") + guarantee)
authorization = fields(activity, 3, 2, guarantee_id, 1_010, 1_020, 1_390, "1" * 64) + authority
authorization_id = identity(b"ATE-RETRY-AUTHORIZATION\x00\x01", fields("") + authorization)
use = fields(authorization_id, 1_025, 1_380, "2" * 64)

goal, observation = "3" * 64, "4" * 64
source = fields("store", 7, "5" * 64, "s", 0, 1, "actor", "e", 1, "6" * 64, 1, "7" * 64)
input_source = fields("store", 8, "8" * 64, "s", 0, 0, 0, 0)
requirements = fields(1, "agent_mandatory_context/v1", "host/v1", 1, "goal", "task", observation, goal)
packet = (
    fields(4, "s", 1, "actor", "e") + source
    + fields(goal, 1, 0, observation, goal, "goal", "renderer/v1", "tokenizer/v1", 10)
    + fields(1, "9" * 64) + input_source + fields("json", 0, 1) + blob(requirements)
)
packet_id = identity(b"ATE-CONTEXT-PACKET\x00", packet)


def vector(name, family, version, raw):
    return {"name": name, "family": family, "version": version, "hex": raw.hex()}


def control_vector(name, op, payload):
    return vector(name, "agent.control", 5, b"ATE-AGENT-CONTROL\x00" + fields(5, op) + payload)


tool_schema = b'{"type":"object"}'
tool_definition = (
    b"ATE-TOOL-DEFINITION\x00" + fields(1, "write", "write/v1", "json-schema") + blob(tool_schema)
    + fields("external", "idempotent", "contract/v1", "", recipient, True, False, True, namespace, 500,
             "reject", "strong", "supported", "supported", "before settlement", "receiver_epoch")
)

vectors = [
    vector("agent-tool-definition-v1", "agent.tool-definition", 1, tool_definition),
    control_vector("control-effect-contract-v5", 6, spec + authority),
    control_vector("control-retry-claim-v5", 7, fields("next-attempt", activity, 2) + authority + use),
    control_vector("control-retry-dispatch-v5", 8, fields("next-attempt") + authority + fields("0" * 64) + use),
    control_vector("control-retry-use-v5", 13, fields(activity, 2) + authority + use),
    control_vector("control-idempotency-guarantee-v5", 32, fields(guarantee_id) + guarantee),
    control_vector("control-retry-authorization-v5", 33, fields(authorization_id) + authorization),
    vector("packet-context-requirements-v4", "knowledge.packet", 4, packet + fields(packet_id)),
]
corpus_path = Path(__file__).with_name("compatibility.json")
corpus = json.loads(corpus_path.read_text())
by_name = {row["name"]: row for row in corpus}
by_name.update((row["name"], row) for row in vectors)
corpus_path.write_text(json.dumps(list(by_name.values()), indent=2) + "\n")
