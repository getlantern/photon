#!/usr/bin/env python3
"""Strict, decoder-independent RFC 9204 feedback accounting diagnostic.

Requires the pinned encoder-peer driver. Exits 1 on a Known Received Count
mismatch; this is an expected failure with the currently pinned ls-qpack.
"""
import os
from pathlib import Path
import struct
import subprocess
import sys


def encode(stream_id, value):
    fields = [(b":status", b"200"), (b"x-dynamic", value),
              (b"x-repeat", b"persistent")]
    command = b"E" + struct.pack(">QI", stream_id, len(fields))
    for name, content in fields:
        command += struct.pack(">II", len(name), len(content)) + name + content
    return command


def feedback(data):
    return b"F" + struct.pack(">I", len(data)) + data


def main():
    directory = os.environ.get("LSQPACK_BIN")
    if not directory:
        sys.exit("set LSQPACK_BIN to the pinned build/bin directory")
    # Acknowledge section 0 (RIC 2), then receive one of the next insertions.
    # RFC 9204 section 2.1.4 requires the increment to advance KRC from 2 to 3.
    commands = (encode(0, b"value-000") + feedback(b"\x80") + b"S"
                + encode(4, b"value-001") + feedback(b"\x01") + b"SQ")
    result = subprocess.run([str(Path(directory) / "encoder-peer"), "--duplicates"],
                            input=commands, capture_output=True, timeout=10, check=True)
    output = memoryview(result.stdout)

    def take(n):
        nonlocal output
        if len(output) < n:
            raise ValueError("truncated driver output")
        data, output = bytes(output[:n]), output[n:]
        return data

    def number():
        return struct.unpack(">I", take(4))[0]

    def section():
        update_len, block_len = number(), number()
        return take(update_len), take(block_len)

    take(number())  # Initial capacity instruction.
    updates, block = section()
    # No wraparound: maximum capacity 256 yields MaxEntries 8, RIC = 3 - 1.
    if not block or block[0] != 3 or take(1) != b"\x00":
        raise ValueError("unexpected initial fixture or rejected acknowledgment")
    before = tuple(number() for _ in range(3))
    if before != (2, 2, 0):
        raise ValueError(f"unexpected counters after acknowledgment: {before}")
    updates, block = section()
    if not updates or take(1) != b"\x00":
        raise ValueError("missing insertions or rejected increment")
    after = tuple(number() for _ in range(3))
    if output or after[0] < 3:
        raise ValueError("invalid fixture state")
    print(f"after ACK: inserts={before[0]} known={before[1]} last_ici={before[2]}")
    print(f"after +1: inserts={after[0]} known={after[1]} last_ici={after[2]}; expected known=3")
    return 0 if after[1] == 3 else 1


if __name__ == "__main__":
    sys.exit(main())
