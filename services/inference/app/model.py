"""e5-small int8 ONNX 推論：tokenize → session.run → mean pooling → L2 normalize。

與 OpenSearch ml-commons 部署的是**同一個 ONNX 檔**（sha256 見 Dockerfile）。實驗 5-2b
（ADR-016）量過本地 ORT 與 ml-commons 同段 cosine 最低 0.9978。pooling 方式（mean + L2）
就是那次實驗用的，改了要重跑 tests/test_golden.py。

前綴（`passage: `／`query: `）由**呼叫端**加。這裡只算向量——前綴規則屬於模型使用方式，
已寫在 Rust 端 `EmbeddingKind`，兩邊各拼一次會重複加。
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import onnxruntime as ort
from tokenizers import Tokenizer

# e5 `max_position_embeddings`。含特殊 token（<s>、</s>）。
MAX_TOKENS = 512
DIMENSIONS = 384


class InputTooLong(ValueError):
    """輸入超過 512 token。**不截斷**：截斷會讓長段後半靜默消失，要呼叫端自己切。"""

    def __init__(self, index: int, tokens: int) -> None:
        super().__init__(f"input[{index}] 有 {tokens} token，超過上限 {MAX_TOKENS}；請先切段，本服務不截斷")
        self.index = index
        self.tokens = tokens


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1 << 20), b""):
            h.update(block)
    return h.hexdigest()


@dataclass(frozen=True)
class ModelFiles:
    onnx: Path
    onnx_sha256: str
    tokenizer: Path
    tokenizer_sha256: str


class Embedder:
    """單一 ORT session。`embed()` 以 lock 序列化：intra-op 執行緒數已經吃滿配額，
    同時跑兩批只會互搶 CPU、讓每批 p95 變差（實驗 5-2a：batch 32 比 batch 8 慢 10～20%）。"""

    def __init__(self, files: ModelFiles, intra_op_threads: int, model_name: str) -> None:
        for path, want in ((files.onnx, files.onnx_sha256), (files.tokenizer, files.tokenizer_sha256)):
            got = sha256_of(path)
            if got != want:
                raise RuntimeError(f"{path} sha256 不符：預期 {want}，實際 {got}。不要改 hash 繞過，先確認檔案來源")
        self.model_name = model_name
        self._tokenizer = Tokenizer.from_file(str(files.tokenizer))
        # 檔案若帶 truncation，超長輸入會被截成剛好 512 而通過檢查。明確關掉。
        self._tokenizer.no_truncation()
        self._tokenizer.no_padding()
        opts = ort.SessionOptions()
        opts.intra_op_num_threads = intra_op_threads
        opts.inter_op_num_threads = 1
        opts.graph_optimization_level = ort.GraphOptimizationLevel.ORT_ENABLE_ALL
        self._session = ort.InferenceSession(str(files.onnx), opts, providers=["CPUExecutionProvider"])
        self._input_names = {i.name for i in self._session.get_inputs()}
        self._lock = threading.Lock()

    def count_tokens(self, texts: list[str]) -> list[int]:
        return [len(e.ids) for e in self._tokenizer.encode_batch(texts, add_special_tokens=True)]

    def embed(self, texts: list[str]) -> tuple[np.ndarray, int]:
        """回傳 (向量 [n, 384]，總 token 數)。任何一筆超過 512 就整批拒絕（先檢查完才推論）。

        **一筆一筆送進模型，不做 padding 批次。** 這個 ONNX 是動態 int8 量化：activation 的量化
        尺度依整個輸入張量（含 padding）計算，所以同一段放在不同批次裡會得到不同向量
        （2026-11-24 實測：同一句換批伴 cosine 0.9982；12 句成批對 ml-commons 最低 0.9947，
        逐筆 0.9963）。向量要可重現——重建時同一段要得到同一個向量——所以逐筆。
        逐筆也比較快：真實段落長短不一，批次要 padding 到最長那筆（實測 4 執行緒 batch 1
        59.0 段／秒、batch 8 30.3 段／秒，段落 token 中位數 161）。
        """
        encodings = self._tokenizer.encode_batch(texts, add_special_tokens=True)
        for i, e in enumerate(encodings):
            if len(e.ids) > MAX_TOKENS:
                raise InputTooLong(i, len(e.ids))
        vectors = np.empty((len(encodings), DIMENSIONS), dtype=np.float32)
        tokens = 0
        with self._lock:
            for i, e in enumerate(encodings):
                ids = np.asarray([e.ids], dtype=np.int64)
                feeds = {"input_ids": ids, "attention_mask": np.ones_like(ids)}
                if "token_type_ids" in self._input_names:
                    feeds["token_type_ids"] = np.zeros_like(ids)
                hidden = self._session.run(None, feeds)[0][0]
                pooled = hidden.mean(axis=0)
                vectors[i] = pooled / max(float(np.linalg.norm(pooled)), 1e-12)
                tokens += len(e.ids)
        return vectors, tokens
