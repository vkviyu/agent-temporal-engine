"""Independent Observation schema 1–4 vectors (stdlib Python; no Go encoder).

Run from the module root: python3 testdata/generate_knowledge_vectors.py
The JSON fixture commits both exact bytes and the independently hashed ID.
"""
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


def text(value):
    raw = value.encode("utf-8")
    return uint(len(raw)) + raw


def vector(kind, imported):
    version = (4 if imported else 3) if kind else (2 if imported else 1)
    # The historical source anchor is shared by all four forms. A direct
    # observation has no source Knowledge selector; an import fixes both the
    # other execution's KnowledgeKey and Cursor.
    source = text("store") + uint(7) + text("a" * 64) + text("space")
    source += uint(int(imported))
    if imported:
        source += text("store") + text("space") + text("b" * 64)
    source += uint(int(imported))
    if imported:
        source += text("source-actor") + text("source-execution")
    source += uint(0) + uint(0)  # no ViewDigest or Frontier
    record = uint(version) + text("target-actor") + text("target-execution")
    record += source + text("c" * 64) + uint(1) + text("fixture")
    if kind:
        record += text(kind)
    if imported:
        record += text("d" * 64)
    identity = hashlib.sha256(b"ATE-OBSERVATION\x00" + record).hexdigest()
    return {
        "version": version,
        "kind": kind,
        "imported": imported,
        "id": identity,
        "hex": (record + text(identity)).hex(),
    }


vectors = [vector("", False), vector("", True)]
for payload_kind in ("memory", "experience_path"):
    vectors.extend((vector(payload_kind, False), vector(payload_kind, True)))
Path(__file__).with_name("knowledge-observations.json").write_text(
    json.dumps(vectors, indent=2) + "\n", encoding="utf-8"
)
