"""Minimal glTF 2.0 GLB writer (POSITION + NORMAL, no textures)."""
from __future__ import annotations

import json
import struct
from pathlib import Path

import numpy as np


def write_terrain_glb(
    path: Path,
    positions: np.ndarray,
    normals: np.ndarray,
    indices: np.ndarray,
) -> dict:
    """positions/normals: (N,3) float32; indices: (M,) uint32."""
    pos = np.ascontiguousarray(positions, dtype=np.float32)
    nrm = np.ascontiguousarray(normals, dtype=np.float32)
    idx = np.ascontiguousarray(indices, dtype=np.uint32)
    pos_b = pos.tobytes()
    nrm_b = nrm.tobytes()
    idx_b = idx.tobytes()

    def _pad(b: bytes, pad_byte: bytes) -> bytes:
        pad = (4 - (len(b) % 4)) % 4
        return b + (pad_byte * pad)

    # BIN padded with 0x00; JSON chunk MUST be padded with 0x20 (spaces).
    bin_chunk = _pad(pos_b + nrm_b + idx_b, b"\x00")
    pos_len = len(pos_b)
    nrm_off = pos_len
    nrm_len = len(nrm_b)
    idx_off = nrm_off + nrm_len
    idx_len = len(idx_b)

    mins = pos.min(axis=0).tolist()
    maxs = pos.max(axis=0).tolist()
    gltf = {
        "asset": {"version": "2.0", "generator": "globalskiatlas game_export 0.1.0"},
        "buffers": [{"byteLength": len(bin_chunk)}],
        "bufferViews": [
            {"buffer": 0, "byteOffset": 0, "byteLength": pos_len, "target": 34962},
            {"buffer": 0, "byteOffset": nrm_off, "byteLength": nrm_len, "target": 34962},
            {"buffer": 0, "byteOffset": idx_off, "byteLength": idx_len, "target": 34963},
        ],
        "accessors": [
            {
                "bufferView": 0,
                "componentType": 5126,
                "count": int(pos.shape[0]),
                "type": "VEC3",
                "min": mins,
                "max": maxs,
            },
            {
                "bufferView": 1,
                "componentType": 5126,
                "count": int(nrm.shape[0]),
                "type": "VEC3",
            },
            {
                "bufferView": 2,
                "componentType": 5125,
                "count": int(idx.shape[0]),
                "type": "SCALAR",
            },
        ],
        "meshes": [
            {
                "primitives": [
                    {
                        "attributes": {"POSITION": 0, "NORMAL": 1},
                        "indices": 2,
                        "mode": 4,
                    }
                ]
            }
        ],
        "nodes": [{"mesh": 0, "name": "terrain"}],
        "scenes": [{"nodes": [0]}],
        "scene": 0,
    }
    json_bytes = _pad(json.dumps(gltf, separators=(",", ":")).encode("utf-8"), b" ")
    # JSON chunk type 0x4E4F534A; BIN 0x004E4942
    json_header = struct.pack("<I", len(json_bytes)) + b"JSON"
    bin_header = struct.pack("<I", len(bin_chunk)) + b"BIN\x00"
    body = json_header + json_bytes + bin_header + bin_chunk
    total = 12 + len(body)
    header = struct.pack("<4sII", b"glTF", 2, total)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(header + body)
    return {
        "vertex_count": int(pos.shape[0]),
        "triangle_count": int(idx.shape[0] // 3),
        "byte_size": total,
    }
