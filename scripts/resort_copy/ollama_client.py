"""Thin Ollama chat client and JSON-reply helpers."""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
from typing import Any

from . import bedrock

from .config import (
    DEFAULT_HOST,
)


def ollama_base_url(raw: str | None) -> str:
    host = (raw or os.environ.get("OLLAMA_HOST") or DEFAULT_HOST).strip().rstrip("/")
    if "://" not in host:
        host = "http://" + host
    return host


def _num_predict(tier: str) -> int:
    """About four characters per token, plus room for the model to finish a sentence."""
    return max(700, min(4096, bedrock.copy_goal_for_tier(tier).characters[1] // 3))


def ollama_chat(
    *,
    base_url: str,
    model: str,
    system_text: str,
    user_text: str,
    temperature: float,
    num_predict: int,
    num_ctx: int,
    timeout: float,
    json_format: bool = False,
) -> str:
    body: dict[str, Any] = {
        "model": model,
        "messages": [
            {"role": "system", "content": system_text},
            {"role": "user", "content": user_text},
        ],
        "stream": False,
        "keep_alive": "30m",
        "options": {
            "temperature": temperature,
            "num_predict": num_predict,
            "num_ctx": num_ctx,
        },
    }
    if json_format:
        body["format"] = "json"
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        base_url + "/api/chat",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw_body = resp.read().decode("utf-8", errors="replace")
                payload = json.loads(raw_body)
            message = payload.get("message") or {}
            text = message.get("content") or ""
            if not text.strip():
                err = payload.get("error") or "empty reply"
                raise RuntimeError(f"Ollama returned no text: {err}")
            return text.strip()
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")[:500]
            raise RuntimeError(f"Ollama {e.code} from {base_url}: {detail}") from e
        except (urllib.error.URLError, json.JSONDecodeError, OSError, RuntimeError) as e:
            last_error = e
            if attempt == 2:
                break
            print(
                f"    ollama connection failed ({e}); retry {attempt + 2}/3",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(8 * (attempt + 1))
    raise RuntimeError(
        f"Cannot reach Ollama at {base_url} ({last_error}). "
        "The box closed the connection or returned an empty reply."
    ) from last_error


def list_models(base_url: str, timeout: float = 8.0) -> list[str]:
    req = urllib.request.Request(base_url + "/api/tags", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.URLError as e:
        raise SystemExit(f"Cannot reach Ollama at {base_url} ({e.reason}).") from e
    return [str(m.get("name")) for m in payload.get("models") or [] if m.get("name")]


class OllamaJsonError(RuntimeError):
    """A model reply was not valid JSON after one repair attempt."""

    def __init__(self, message: str, raw: str) -> None:
        super().__init__(message)
        self.raw = raw


def _parse_json_loose(raw: str) -> dict[str, Any]:
    s = bedrock._strip_json_fence(raw)
    try:
        return json.loads(s)
    except json.JSONDecodeError:
        start = s.find("{")
        end = s.rfind("}")
        if start >= 0 and end > start:
            return json.loads(s[start : end + 1])
        raise


def _chat_json(
    *,
    base_url: str,
    model: str,
    system_text: str,
    user_text: str,
    temperature: float,
    num_predict: int,
    num_ctx: int,
    timeout: float,
    json_format: bool = True,
) -> dict[str, Any]:
    """Call the model in JSON mode, then one repair pass. Log the raw reply on failure."""

    def _once(system: str, user: str, temp: float) -> str:
        return ollama_chat(
            base_url=base_url,
            model=model,
            system_text=system,
            user_text=user,
            temperature=temp,
            num_predict=num_predict,
            num_ctx=num_ctx,
            timeout=timeout,
            json_format=json_format,
        )

    raw = _once(system_text, user_text, temperature)
    try:
        return _parse_json_loose(raw)
    except json.JSONDecodeError as first_error:
        print(
            f"    json parse failed ({first_error}); raw follows\n{raw[:2000]}",
            file=sys.stderr,
            flush=True,
        )
        repair_user = (
            user_text
            + "\n\nThe previous reply was not valid JSON. Repair it. "
            + "Return one JSON object and nothing else.\nBROKEN:\n"
            + raw[:4000]
        )
        repaired = _once(system_text + " Return only a JSON object.", repair_user, 0.0)
        try:
            return _parse_json_loose(repaired)
        except json.JSONDecodeError as second_error:
            print(
                f"    json repair failed ({second_error}); raw follows\n{repaired[:2000]}",
                file=sys.stderr,
                flush=True,
            )
            raise OllamaJsonError(
                f"Ollama JSON parse failed after repair: {second_error}",
                repaired,
            ) from second_error


def unload_model(base_url: str, model: str) -> None:
    body = json.dumps({"model": model, "keep_alive": 0, "prompt": ""}).encode("utf-8")
    request = urllib.request.Request(
        base_url.rstrip("/") + "/api/generate",
        data=body,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=30):
            pass
    except Exception as exc:
        print(f"unload {model}: {exc}", file=sys.stderr)
