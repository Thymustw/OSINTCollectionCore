"""osint-inference 測試。用真的 e5-small int8 ONNX 與 tokenizer（不進 repo）：

    INFERENCE_ONNX_PATH=...  INFERENCE_TOKENIZER_PATH=...  pytest services/inference/tests

找不到檔案時**失敗**而不是跳過——跳過的測試在 CI 上是綠的，等於沒驗。
取得方式見 services/inference/README.md。
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path

import numpy as np
import pytest
from fastapi.testclient import TestClient

from app.main import Settings, create_app
from app.model import MAX_TOKENS, Embedder, ModelFiles

ONNX_SHA256 = "dd476dd0c2514e9b9be83aeb3853fac0763e0bdf4a71645407587d77c48a2d88"
TOKENIZER_SHA256 = "0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39"  # gitleaks:allow
REPO = Path(__file__).resolve().parents[3]


def _paths() -> tuple[Path, Path]:
    onnx = Path(os.environ.get("INFERENCE_ONNX_PATH", REPO / "models/e5-small/model.onnx"))
    tok = Path(os.environ.get("INFERENCE_TOKENIZER_PATH", REPO / "models/e5-small/tokenizer.json"))
    for p in (onnx, tok):
        if not p.is_file():
            pytest.fail(f"找不到 {p}；請照 services/inference/README.md 下載模型檔")
    return onnx, tok


@pytest.fixture(scope="session")
def embedder() -> Embedder:
    onnx, tok = _paths()
    return Embedder(ModelFiles(onnx, ONNX_SHA256, tok, TOKENIZER_SHA256), 2, "multilingual-e5-small-int8")


def _settings(**over) -> Settings:
    onnx, tok = _paths()
    os.environ.setdefault("INFERENCE_ONNX_SHA256", ONNX_SHA256)
    os.environ.setdefault("INFERENCE_TOKENIZER_SHA256", TOKENIZER_SHA256)
    s = Settings()
    s.onnx, s.tokenizer = onnx, tok
    for k, v in over.items():
        setattr(s, k, v)
    return s


@pytest.fixture()
def client(embedder):
    with TestClient(create_app(_settings(), embedder)) as c:
        yield c


def test_golden_vectors_match_ml_commons(embedder):
    """與 OpenSearch ml-commons 已部署同一模型的輸出比 cosine。

    門檻 0.995，不是實驗 5-2b 的 0.997：2026-11-24 這組 12 句（含全形、emoji、日文、簡體、
    英文 query）逐筆實測最低 0.99631（英文 query），其餘 ≥ 0.9976；**不同句之間最高 0.916**。
    0.997 是 5-2b 只用 20 句得到的最低值，不是模型保證。再加一條：同句 cosine 至少比
    最像的不同句高 0.05，確保門檻真的分得出「同一個模型」與「換了東西」。"""
    golden = json.loads(Path(__file__).with_name("golden.json").read_text())
    texts = [it["text"] for it in golden["items"]]
    want = np.array([it["embedding"] for it in golden["items"]], dtype=np.float32)
    want /= np.linalg.norm(want, axis=1, keepdims=True)
    got, _ = embedder.embed(texts)
    cos = (got * want).sum(axis=1)
    assert cos.min() >= 0.995, f"最低 cosine {cos.min():.5f}：{np.round(cos, 5).tolist()}"
    cross = (got @ want.T)[~np.eye(len(texts), dtype=bool)]
    assert cos.min() - cross.max() >= 0.05, f"同句最低 {cos.min():.4f}、不同句最高 {cross.max():.4f}"


def test_same_text_same_vector_regardless_of_batch(embedder):
    """同一段放在不同請求、不同批伴裡，向量必須**完全相同**。
    動態 int8 量化在 padding 批次下做不到（實測換批伴 cosine 0.9982），所以 embed() 逐筆推論。"""
    target = "passage: 勒索軟體 LockBit 3.0 於 2026 年第三季活動下降 30%。"
    alone, _ = embedder.embed([target])
    with_short, _ = embedder.embed(["passage: 短。", target])
    with_long, _ = embedder.embed([target, "passage: " + "很長的句子用來造成 padding。" * 20])
    assert np.array_equal(alone[0], with_short[1])
    assert np.array_equal(alone[0], with_long[0])


def test_vectors_are_unit_length_and_384(embedder):
    v, tokens = embedder.embed(["passage: a", "passage: 中文"])
    assert v.shape == (2, 384)
    assert np.allclose(np.linalg.norm(v, axis=1), 1.0, atol=1e-5)
    assert tokens > 0


def test_http_embeddings_openai_shape(client):
    r = client.post("/v1/embeddings", json={"input": ["passage: 一", "passage: 二"]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["model"] == "multilingual-e5-small-int8"
    assert [d["index"] for d in body["data"]] == [0, 1]
    assert len(body["data"][0]["embedding"]) == 384
    assert body["usage"]["prompt_tokens"] > 0
    # 單一字串也收。
    assert client.post("/v1/embeddings", json={"input": "passage: 單筆"}).status_code == 200


def test_over_512_tokens_is_rejected_not_truncated(client, embedder):
    text = "passage: " + " ".join(f"{i:064x}" for i in range(60))
    assert embedder.count_tokens([text])[0] > MAX_TOKENS
    r = client.post("/v1/embeddings", json={"input": ["passage: ok", text]})
    assert r.status_code == 400
    assert "input[1]" in r.json()["detail"] and "不截斷" in r.json()["detail"]


@pytest.mark.parametrize(
    "payload",
    [
        {"input": []},
        {"input": ["x"] * 33},
        {"input": ["x" * 9000]},
        {"input": ["x"], "model": "other-model"},
        {"input": ["x"], "encoding_format": "base64"},
        {"input": 123},
    ],
)
def test_bad_requests(client, payload):
    assert client.post("/v1/embeddings", json=payload).status_code in (400, 422)


def test_overload_returns_503_with_retry_after(embedder):
    """佇列滿就立刻 503，不排隊。用 max_pending=1 加一個卡住的推論模擬。"""
    gate = threading.Event()
    entered = threading.Event()
    real = embedder.embed

    def slow(texts):
        entered.set()
        gate.wait(10)
        return real(texts)

    app = create_app(_settings(max_pending=1), embedder)
    with TestClient(app) as c:
        embedder.embed = slow
        try:
            results = {}
            t = threading.Thread(
                target=lambda: results.setdefault("first", c.post("/v1/embeddings", json={"input": ["passage: a"]}))
            )
            t.start()
            assert entered.wait(10), "第一個請求沒有進到推論"
            r = c.post("/v1/embeddings", json={"input": ["passage: b"]})
            assert r.status_code == 503
            assert r.headers["retry-after"] == "1"
            gate.set()
            t.join(10)
            assert results["first"].status_code == 200
        finally:
            embedder.embed = real
            gate.set()
        m = c.get("/metrics").text
        assert 'osint_inference_requests_total{outcome="overloaded"}' in m


def test_health_ready_metrics(client):
    assert client.get("/health").json() == {"status": "ok"}
    assert client.get("/ready").json()["ready"] is True
    assert "osint_inference_batch_seconds" in client.get("/metrics").text


def test_hash_mismatch_refuses_to_load():
    onnx, tok = _paths()
    with pytest.raises(RuntimeError, match="sha256 不符"):
        Embedder(ModelFiles(onnx, "0" * 64, tok, TOKENIZER_SHA256), 1, "x")
