"""osint-inference：e5 向量推論服務（ADR-016 D2，階段 5 M2-1）。

只對內（compose 網路），沒有認證：呼叫端是 Core 的 Rust worker。**不要**把埠對外開放——
compose 只綁 127.0.0.1。

端點：
- `POST /v1/embeddings`：OpenAI 相容子集（`input` 字串或字串陣列、`model` 選填）
- `GET /health`：行程活著
- `GET /ready`：模型已載入且通過 sha256
- `GET /metrics`：Prometheus

背壓：同時在等推論的請求數有上限（`INFERENCE_MAX_PENDING`）。滿了立刻回 503＋Retry-After，
不排隊等——呼叫端（chunk-embedder）會退避。無限排隊會讓延遲無上限成長，而且呼叫端看不出來。
"""

from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from pydantic import BaseModel, Field

from .model import DIMENSIONS, Embedder, InputTooLong, ModelFiles


def _env_int(name: str, default: int, lo: int, hi: int) -> int:
    raw = os.environ.get(name)
    value = default if raw is None else int(raw)
    if not lo <= value <= hi:
        raise RuntimeError(f"{name}={value} 不在 {lo}..={hi}")
    return value


def _read_sums(path: Path) -> dict[str, str]:
    if not path.is_file():
        return {}
    out = {}
    for line in path.read_text().splitlines():
        parts = line.split()
        if len(parts) == 2:
            out[parts[1].lstrip("*")] = parts[0]
    return out


class Settings:
    def __init__(self) -> None:
        self.model_name = os.environ.get("INFERENCE_MODEL_NAME", "multilingual-e5-small-int8")
        self.onnx = Path(os.environ.get("INFERENCE_ONNX_PATH", "/opt/model/model.onnx"))
        self.tokenizer = Path(os.environ.get("INFERENCE_TOKENIZER_PATH", "/opt/model/tokenizer.json"))
        # 預期 hash：環境變數優先，否則讀 image 建置時寫的 SHA256SUMS（sha256sum 格式）。
        # 兩個都沒有就拒絕啟動——不驗 hash 等於不知道載入的是哪個模型。
        sums = _read_sums(Path(os.environ.get("INFERENCE_SHA256SUMS", "/opt/model/SHA256SUMS")))
        self.onnx_sha256 = os.environ.get("INFERENCE_ONNX_SHA256") or sums.get("model.onnx", "")
        self.tokenizer_sha256 = os.environ.get("INFERENCE_TOKENIZER_SHA256") or sums.get("tokenizer.json", "")
        if not self.onnx_sha256 or not self.tokenizer_sha256:
            raise RuntimeError("沒有模型檔的預期 sha256：設 INFERENCE_ONNX_SHA256／INFERENCE_TOKENIZER_SHA256 或提供 SHA256SUMS")
        # 實驗 5-2a：4 執行緒以內接近線性，之後遞減。常駐 4、回填可放寬到 8（owner 裁決 2026-11-05）。
        self.threads = _env_int("INFERENCE_THREADS", 4, 1, 32)
        # batch 32 比 batch 8 慢、RSS 多約 630 MB（實驗 5-2a）；上限 32 是硬界線，呼叫端建議送 8。
        self.max_batch = _env_int("INFERENCE_MAX_BATCH", 32, 1, 64)
        self.max_pending = _env_int("INFERENCE_MAX_PENDING", 4, 1, 64)
        # 單筆字元上限：512 token 的中文約 500～700 字、英文約 2,000 字。8,000 字擋掉明顯不是段落的輸入，
        # 免得先花 CPU tokenize 一份 1 MB 的字串才發現超長。
        self.max_chars = _env_int("INFERENCE_MAX_CHARS", 8000, 100, 100_000)


registry = CollectorRegistry()
REQUESTS = Counter("osint_inference_requests_total", "embedding 請求", ["outcome"], registry=registry)
INPUTS = Counter("osint_inference_inputs_total", "向量化的輸入筆數", registry=registry)
TOKENS = Counter("osint_inference_tokens_total", "向量化的 token 數（含特殊 token）", registry=registry)
PENDING = Gauge("osint_inference_pending", "正在推論或等待推論的請求數", registry=registry)
LATENCY = Histogram(
    "osint_inference_batch_seconds",
    "單一請求的推論耗時",
    buckets=(0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0),
    registry=registry,
)


class EmbeddingsRequest(BaseModel):
    input: str | list[str]
    model: str | None = None
    encoding_format: str | None = Field(default=None, pattern="^float$")


def create_app(settings: Settings | None = None, embedder: Embedder | None = None) -> FastAPI:
    state: dict = {}

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        s = settings or Settings()
        state["settings"] = s
        state["embedder"] = embedder or Embedder(
            ModelFiles(s.onnx, s.onnx_sha256, s.tokenizer, s.tokenizer_sha256),
            s.threads,
            s.model_name,
        )
        state["slots"] = asyncio.Semaphore(s.max_pending)
        yield

    app = FastAPI(title="osint-inference", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    @app.get("/ready")
    async def ready():
        if "embedder" not in state:
            return JSONResponse({"ready": False, "reason": "模型未載入"}, status_code=503)
        return {"ready": True, "model": state["embedder"].model_name, "dimensions": DIMENSIONS}

    @app.get("/metrics")
    async def metrics() -> PlainTextResponse:
        return PlainTextResponse(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

    @app.post("/v1/embeddings")
    async def embeddings(body: EmbeddingsRequest, request: Request):
        s: Settings = state["settings"]
        emb: Embedder = state["embedder"]
        texts = [body.input] if isinstance(body.input, str) else body.input
        if not texts:
            REQUESTS.labels("bad_request").inc()
            raise HTTPException(400, "input 不可為空陣列")
        if len(texts) > s.max_batch:
            REQUESTS.labels("bad_request").inc()
            raise HTTPException(400, f"一次最多 {s.max_batch} 筆，收到 {len(texts)} 筆；請分批送")
        for i, t in enumerate(texts):
            if len(t) > s.max_chars:
                REQUESTS.labels("bad_request").inc()
                raise HTTPException(400, f"input[{i}] 有 {len(t)} 字元，超過 {s.max_chars}；請先切段")
        if body.model not in (None, emb.model_name):
            REQUESTS.labels("bad_request").inc()
            raise HTTPException(400, f"本服務只提供 {emb.model_name}，收到 model={body.model}")

        slots: asyncio.Semaphore = state["slots"]
        if slots.locked():
            REQUESTS.labels("overloaded").inc()
            return JSONResponse(
                {"error": {"message": "推論佇列已滿，請退避後重試", "type": "overloaded"}},
                status_code=503,
                headers={"Retry-After": "1"},
            )
        async with slots:
            PENDING.inc()
            started = time.perf_counter()
            try:
                # ORT 會釋放 GIL；丟到執行緒池，event loop 才能繼續回 /health 與 503。
                vectors, tokens = await asyncio.to_thread(emb.embed, texts)
            except InputTooLong as err:
                REQUESTS.labels("too_long").inc()
                raise HTTPException(400, str(err)) from err
            finally:
                PENDING.dec()
            LATENCY.observe(time.perf_counter() - started)
        REQUESTS.labels("ok").inc()
        INPUTS.inc(len(texts))
        TOKENS.inc(tokens)
        return {
            "object": "list",
            "model": emb.model_name,
            "data": [{"object": "embedding", "index": i, "embedding": v.tolist()} for i, v in enumerate(vectors)],
            "usage": {"prompt_tokens": tokens, "total_tokens": tokens},
        }

    return app


app = create_app()
