"""
convert_ppk.py — Convert a PuTTY .ppk (v3, ed25519) to OpenSSH PEM format.
Requires no extra dependencies beyond cryptography (already installed).

Run:
    python eda/convert_ppk.py
"""

import base64
import struct
from pathlib import Path

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import (
    Encoding,
    NoEncryption,
    PrivateFormat,
)

PPK_PATH = Path(__file__).parent.parent / "keys" / "eagriculturai_keysecesi.ppk"
PEM_PATH = Path(__file__).parent.parent / "keys" / "eagriculturai_key.pem"


def read_string(data: bytes, offset: int):
    length = struct.unpack(">I", data[offset : offset + 4])[0]
    value = data[offset + 4 : offset + 4 + length]
    return value, offset + 4 + length


def convert(ppk_path: Path, pem_path: Path) -> None:
    lines = ppk_path.read_text().splitlines()

    encryption = None
    private_data: list[str] = []

    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("Encryption:"):
            encryption = line.split(": ", 1)[1].strip()
        elif line.startswith("Private-Lines:"):
            count = int(line.split(": ", 1)[1])
            private_data = lines[i + 1 : i + 1 + count]
            i += count
        i += 1

    if encryption and encryption != "none":
        raise ValueError("Encrypted PPK files are not supported by this converter.")

    priv_bytes = base64.b64decode("".join(private_data))

    # Ed25519 private blob: string(64-byte key) = seed (32 bytes) + public key (32 bytes)
    seed, _ = read_string(priv_bytes, 0)
    private_key = Ed25519PrivateKey.from_private_bytes(seed[:32])

    pem = private_key.private_bytes(
        encoding=Encoding.PEM,
        format=PrivateFormat.OpenSSH,
        encryption_algorithm=NoEncryption(),
    )
    pem_path.write_bytes(pem)
    print(f"Saved PEM key to {pem_path}")


if __name__ == "__main__":
    convert(PPK_PATH, PEM_PATH)
