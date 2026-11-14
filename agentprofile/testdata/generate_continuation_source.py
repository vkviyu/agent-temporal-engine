"""Independent V1 continuation source fixture; does not call a Go codec."""
from pathlib import Path
import json


def uint(value):
    out = bytearray()
    while value >= 128:
        out.append((value & 127) | 128)
        value >>= 7
    out.append(value)
    return bytes(out)


def text(value):
    raw = value.encode("utf-8")
    return uint(len(raw)) + raw


# Continuation fields: E, version, incarnation, pending key/generation,
# input fence, phase, Position presence; then Position fields in registry order.
position = text("s") + text("p") + uint(0)
position += text("") + uint(0) + text("")  # no direct KnowledgeFence
position += text("") * 3 + text("") * 2  # no direct Cursor or KnowledgeKey
position += text("host/v1") + uint(1) + text("next") + uint(1)
# KnowledgeSource presence, Store, Space, ProducerTx, Cursor?, KnowledgeKey?.
position += uint(1) + text("store") + text("s") + text("a" * 32)
position += uint(1) + text("store") + text("s") + text("b" * 64)
position += uint(1) + text("actor") + text("e")
continuation = text("e") + uint(1) + uint(1) + text("next") + uint(1)
continuation += text("store") + uint(1) + text("f" * 64)
continuation += text("pending-render") + uint(1) + position
lifecycle = uint(8) + continuation
wrapped = b"ATE-AGENT-COMMAND\x00" + uint(1) + uint(12) + uint(len(lifecycle)) + lifecycle
root = Path(__file__).resolve().parent
(root / "continuation-source-v1.hex").write_text(wrapped.hex() + "\n")
# SaveContinuation is the 14th Control operation: Continuation, ExpectedVersion,
# then AuthorityBinding{Deployment,OwnerEpoch,Incarnation,Holder}.
control = b"ATE-AGENT-CONTROL\x00" + uint(6) + uint(14) + continuation
control += uint(0) + text("d") + uint(1) + uint(1) + text("h")
corpus_path = root.parents[1] / "formats" / "testdata" / "compatibility.json"
corpus = json.loads(corpus_path.read_text())
vectors = [
    {"name": "continuation-source-op8", "family": "agent.continuation", "version": 8, "hex": lifecycle.hex()},
    {"name": "control-source-v6", "family": "agent.control", "version": 6, "hex": control.hex()},
]
by_name = {row["name"]: row for row in corpus}
by_name.update((row["name"], row) for row in vectors)
corpus_path.write_text(json.dumps(list(by_name.values()), indent=2) + "\n")
