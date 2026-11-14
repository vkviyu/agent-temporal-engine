"""Independent Summary schema 2 fixture: hashlib + unsigned LEB128 only."""

import hashlib
from pathlib import Path


def uint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 127) | 128)
        value >>= 7
    result.append(value)
    return bytes(result)


def fields(*values):
    return b"".join(uint(v) if isinstance(v, int) else uint(len(v.encode())) + v.encode() for v in values)


def source(lsn, selected=False):
    head = fields("r", lsn, "a" * 64, "s")
    return head + (fields(1, "r", "s", "b" * 64, 1, "w", "x", 1, "d" * 64, 1, "c" * 64) if selected else fields(0, 0, 0, 0))


sources = fields(1, "e" * 64, "f" * 64, "")
source_digest = hashlib.sha256(b"ATE-SUMMARY-SOURCESET\x00" + uint(2) + source(1, True) + sources).hexdigest()
raw = b"ATE-KNOWLEDGE-SUMMARY\x00" + uint(2) + fields("s") + source(1, True) + sources
raw += fields(source_digest, 1, "topic", 1, "e" * 64, 0, "goal", "env", 1, 1, "tag", 700)
raw += fields("act", "attempt", "result", "model", "prompt/v1", "policy/v1")
raw += fields("1" * 64) + source(2) + fields("application/json")
raw += fields("2" * 64) + source(3) + fields("text/plain")
raw += fields("2026-11-02T00:00:00Z", "2026-11-02T00:00:00Z", "unreviewed", "", "", 0, "")
identity = hashlib.sha256(b"ATE-SUMMARY-ID\x00" + raw).hexdigest()
Path(__file__).with_name("summary-schema2.hex").write_text((raw + fields(identity)).hex() + "\n")
