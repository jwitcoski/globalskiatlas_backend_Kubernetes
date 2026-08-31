"""glTF 2.0 GLB writer for terrain (POSITION + NORMAL).

Writes KHR_draco_mesh_compression when DracoPy is available. DracoPy encodes
NORMAL as unique_id 0 and POSITION as unique_id 1; those ids must be mapped
explicitly or Three.js treats unit-length normals as world positions.
"""
from __future__ import annotations

import json
import logging
import struct
from functools import lru_cache
from pathlib import Path

import numpy as np

log = logging.getLogger("game_export")

GENERATOR = "globalskiatlas game_export 0.2.0"
# Balanced ski-terrain defaults: ~cm–dm error on 20 km tiles, not ultra-aggressive.
DRACO_QUANTIZATION_BITS = 16
DRACO_COMPRESSION_LEVEL = 5

try:
    import DracoPy
except ImportError:  # pragma: no cover
    DracoPy = None


def _pad(b: bytes, pad_byte: bytes) -> bytes:
    n = (4 - (len(b) % 4)) % 4
    return b + pad_byte * n


def _write_glb(path: Path, gltf: dict, bin_chunk: bytes) -> int:
    bin_chunk = _pad(bin_chunk, b"\x00")
    gltf["buffers"] = [{"byteLength": len(bin_chunk)}]
    json_bytes = _pad(json.dumps(gltf, separators=(",", ":")).encode("utf-8"), b" ")
    body = struct.pack("<I", len(json_bytes)) + b"JSON" + json_bytes
    body += struct.pack("<I", len(bin_chunk)) + b"BIN\x00" + bin_chunk
    total = 12 + len(body)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(struct.pack("<4sII", b"glTF", 2, total) + body)
    return total


def glb_has_draco(data: bytes) -> bool:
    if len(data) < 20 or data[:4] != b"glTF":
        return False
    json_len = struct.unpack_from("<I", data, 12)[0]
    chunk = data[20 : 20 + json_len]
    return b"KHR_draco_mesh_compression" in chunk


def read_uncompressed_terrain_glb(path: Path) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Read POSITION / NORMAL / indices from an uncompressed game_export GLB."""
    data = path.read_bytes()
    json_len = struct.unpack_from("<I", data, 12)[0]
    gltf = json.loads(data[20 : 20 + json_len].decode("utf-8"))
    if "KHR_draco_mesh_compression" in (gltf.get("extensionsUsed") or []):
        raise ValueError(f"{path} is already Draco-compressed")
    bin_off = 20 + json_len
    bin_len = struct.unpack_from("<I", data, bin_off)[0]
    blob = data[bin_off + 8 : bin_off + 8 + bin_len]
    views = gltf["bufferViews"]
    acc = gltf["accessors"]

    def arr(ai: int, dtype, cols: int):
        v = views[acc[ai]["bufferView"]]
        start = int(v.get("byteOffset") or 0)
        raw = blob[start : start + int(v["byteLength"])]
        a = np.frombuffer(raw, dtype=dtype)
        return a.reshape(-1, cols) if cols > 1 else a

    return arr(0, np.float32, 3), arr(1, np.float32, 3), arr(2, np.uint32, 1)


@lru_cache(maxsize=1)
def _draco_unique_ids() -> dict[str, int]:
    """DracoPy encode-with-normals unique_ids (stable across mesh size)."""
    pos = np.array([[0.0, 1.0, 0.0], [1.0, 1.0, 0.0], [0.0, 1.0, -1.0]], dtype=np.float64)
    nrm = np.array([[0.0, 1.0, 0.0], [0.0, 1.0, 0.0], [0.0, 1.0, 0.0]], dtype=np.float64)
    faces = np.array([[0, 1, 2]], dtype=np.uint32)
    mesh = DracoPy.decode(
        DracoPy.encode(
            pos,
            faces=faces,
            quantization_bits=DRACO_QUANTIZATION_BITS,
            compression_level=1,
            normals=nrm,
            preserve_order=False,
        )
    )
    out: dict[str, int] = {}
    for a in mesh.attributes:
        if a["attribute_type"] == 0:
            out["POSITION"] = int(a["unique_id"])
        elif a["attribute_type"] == 1:
            out["NORMAL"] = int(a["unique_id"])
    if "POSITION" not in out or "NORMAL" not in out:
        raise RuntimeError(f"DracoPy unique_id map incomplete: {out}")
    return out


def _encode_draco(pos: np.ndarray, nrm: np.ndarray, idx: np.ndarray) -> bytes:
    faces = np.ascontiguousarray(idx.reshape(-1, 3), dtype=np.uint32)
    return DracoPy.encode(
        np.ascontiguousarray(pos, dtype=np.float64),
        faces=faces,
        quantization_bits=DRACO_QUANTIZATION_BITS,
        compression_level=DRACO_COMPRESSION_LEVEL,
        normals=np.ascontiguousarray(nrm, dtype=np.float64),
        preserve_order=False,
        create_metadata=False,
    )


def _gltf_uncompressed(pos: np.ndarray, nrm: np.ndarray, idx: np.ndarray, bin_len: int) -> dict:
    pos_len = int(pos.nbytes)
    nrm_len = int(nrm.nbytes)
    idx_len = int(idx.nbytes)
    mins = pos.min(axis=0).tolist()
    maxs = pos.max(axis=0).tolist()
    return {
        "asset": {"version": "2.0", "generator": GENERATOR},
        "buffers": [{"byteLength": bin_len}],
        "bufferViews": [
            {"buffer": 0, "byteOffset": 0, "byteLength": pos_len, "target": 34962},
            {"buffer": 0, "byteOffset": pos_len, "byteLength": nrm_len, "target": 34962},
            {"buffer": 0, "byteOffset": pos_len + nrm_len, "byteLength": idx_len, "target": 34963},
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
            {"bufferView": 1, "componentType": 5126, "count": int(nrm.shape[0]), "type": "VEC3"},
            {"bufferView": 2, "componentType": 5125, "count": int(idx.shape[0]), "type": "SCALAR"},
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


def _gltf_draco(pos: np.ndarray, nrm: np.ndarray, idx: np.ndarray, drc_len: int, ids: dict[str, int]) -> dict:
    mins = pos.min(axis=0).tolist()
    maxs = pos.max(axis=0).tolist()
    return {
        "asset": {"version": "2.0", "generator": GENERATOR},
        "extensionsUsed": ["KHR_draco_mesh_compression"],
        "extensionsRequired": ["KHR_draco_mesh_compression"],
        "buffers": [{"byteLength": drc_len}],
        "bufferViews": [{"buffer": 0, "byteOffset": 0, "byteLength": drc_len}],
        "accessors": [
            {
                "componentType": 5126,
                "count": int(pos.shape[0]),
                "type": "VEC3",
                "min": mins,
                "max": maxs,
            },
            {"componentType": 5126, "count": int(nrm.shape[0]), "type": "VEC3"},
            {"componentType": 5125, "count": int(idx.shape[0]), "type": "SCALAR"},
        ],
        "meshes": [
            {
                "primitives": [
                    {
                        "attributes": {"POSITION": 0, "NORMAL": 1},
                        "indices": 2,
                        "mode": 4,
                        "extensions": {
                            "KHR_draco_mesh_compression": {
                                "bufferView": 0,
                                "attributes": {
                                    "POSITION": ids["POSITION"],
                                    "NORMAL": ids["NORMAL"],
                                },
                            }
                        },
                    }
                ]
            }
        ],
        "nodes": [{"mesh": 0, "name": "terrain"}],
        "scenes": [{"nodes": [0]}],
        "scene": 0,
    }


def write_terrain_glb(
    path: Path,
    positions: np.ndarray,
    normals: np.ndarray,
    indices: np.ndarray,
    *,
    draco: bool = True,
) -> dict:
    """positions/normals: (N,3) float32; indices: (M,) uint32."""
    pos = np.ascontiguousarray(positions, dtype=np.float32)
    nrm = np.ascontiguousarray(normals, dtype=np.float32)
    idx = np.ascontiguousarray(indices, dtype=np.uint32)
    compressed = False
    quant_bits = None
    if draco and DracoPy is not None and idx.size >= 3:
        try:
            ids = _draco_unique_ids()
            drc = _encode_draco(pos, nrm, idx)
            gltf = _gltf_draco(pos, nrm, idx, len(drc), ids)
            total = _write_glb(path, gltf, drc)
            compressed = True
            quant_bits = DRACO_QUANTIZATION_BITS
        except Exception:
            log.exception("Draco encode failed; writing uncompressed GLB")
            compressed = False
    if not compressed:
        bin_chunk = pos.tobytes() + nrm.tobytes() + idx.tobytes()
        gltf = _gltf_uncompressed(pos, nrm, idx, len(bin_chunk))
        total = _write_glb(path, gltf, bin_chunk)
    info = {
        "vertex_count": int(pos.shape[0]),
        "triangle_count": int(idx.shape[0] // 3),
        "byte_size": total,
        "compression": "draco" if compressed else "none",
    }
    if quant_bits is not None:
        info["draco_quantization_bits"] = quant_bits
        info["draco_compression_level"] = DRACO_COMPRESSION_LEVEL
    return info


def compress_terrain_glb_file(src: Path, dst: Path) -> dict:
    """Re-encode an uncompressed terrain GLB with Draco."""
    pos, nrm, idx = read_uncompressed_terrain_glb(src)
    return write_terrain_glb(dst, pos, nrm, idx, draco=True)
