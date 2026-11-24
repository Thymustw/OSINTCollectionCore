# osint-inference：向量推論服務（ADR-016 D2，階段 5 M2-1）

e5-small int8（`intfloat/multilingual-e5-small`，`model_qint8_avx512_vnni.onnx`）的 CPU 推論，
OpenAI 相容的 `/v1/embeddings`。和 OpenSearch ml-commons 部署的是**同一個 ONNX 檔**。

Python 是刻意的：CLAUDE.md §3 把 Python 限定在 AI／模型執行環境，這個服務就是那一類。
Core（Rust）透過 HTTP 呼叫，不內嵌 Python（TECH_STACK §3）。

## 端點

| 端點 | 說明 |
|---|---|
| `POST /v1/embeddings` | `{"input": "..." 或 ["...", ...], "model"?: "multilingual-e5-small-int8"}`，回 OpenAI 格式 |
| `GET /health` | 行程活著 |
| `GET /ready` | 模型已載入且通過 sha256 |
| `GET /metrics` | Prometheus：`osint_inference_requests_total{outcome}`、`_inputs_total`、`_tokens_total`、`_pending`、`_batch_seconds` |

**前綴由呼叫端加**（`passage: `／`query: `）。這個服務只算向量。

| 情況 | 回應 |
|---|---|
| 任何一筆超過 512 token（含特殊 token） | **400，不截斷**。截斷會讓段落後半靜默消失，要先切段 |
| 一次超過 32 筆、單筆超過 8,000 字元、`input` 是空陣列 | 400 |
| 同時在推論或等待的請求達 `INFERENCE_MAX_PENDING` | 立刻 503＋`Retry-After: 1`，不排隊 |

不排隊是刻意的：無限排隊會讓延遲無上限地成長，而且呼叫端看不出來。

**沒有認證，只對內。** compose 把宿主埠綁在 `127.0.0.1:18091`。

## 逐筆推論（實測決定）

同一個請求裡的多筆輸入，會**一筆一筆**送進模型，不做 padding 批次。

1. **可重現**。這個 ONNX 是動態 int8 量化，activation 的量化尺度依整個輸入張量計算，padding 也算在內。
   所以同一段放在不同批次裡，會得到不同向量。2026-11-24 實測：

   | 比較 | cosine |
   |---|---|
   | 同一句換不同批伴 | 0.9982 |
   | 12 句成批，對 ml-commons 最低 | 0.9947 |
   | 逐筆，對 ml-commons 最低 | 0.9963 |
   | 同一句 ×4 成批（沒有 padding） | 與逐筆完全相同 |

   重建時同一段必須得到同一個向量，所以逐筆。

2. **比較快**。真實段落長短不一，批次要 padding 到最長那筆。用開發 DB 取 128 段實測（token 中位數 161，load 1.3）：

   | 執行緒 | batch 1 | batch 8 |
   |---|---|---|
   | 4 | **59.0 段／秒** | 30.3 段／秒 |
   | 8 | **75.6 段／秒** | 51.0 段／秒 |

   ADR-016 實驗 5-2a 的「batch 8」結論是用等長（約 300 token）合成段落量的，不適用於真實段落。

## 和 ml-commons 的一致性

`tests/golden.json` 是 12 句合成句子（中英混雜、全形、emoji、IoC、簡體、日文、query／passage），
打 ml-commons 已部署模型取得的向量。產生方式見 `tests/make_golden.py`。

2026-11-24 逐筆實測：同句 cosine 最低 0.99631（英文 query），其餘 ≥ 0.9976；**不同句之間最高 0.916**。
測試門檻設 0.995，另外要求「同句最低比不同句最高多 0.05」。
ADR 實驗 5-2b 寫的 0.997 是只用 20 句量出的最低值，不是模型的保證。

pooling 與 ml-commons 設定相同：`pooling_mode: MEAN`、`normalize_result: true`
（見 `scripts/opensearch-ml-setup-e5.sh`）。

## 設定（環境變數）

| 變數 | 預設 | 說明 |
|---|---|---|
| `INFERENCE_THREADS` | 4 | ORT intra-op 執行緒。owner 裁決常駐 4 核，回填時可放寬到 8（compose 的 `cpus` 要一起改） |
| `INFERENCE_MAX_PENDING` | 4 | 同時在推論或等待的請求上限，超過回 503 |
| `INFERENCE_MAX_BATCH` | 32 | 單一請求筆數上限 |
| `INFERENCE_MAX_CHARS` | 8000 | 單筆字元上限，在 tokenize 之前先擋明顯不是段落的輸入 |
| `INFERENCE_ONNX_PATH`／`INFERENCE_TOKENIZER_PATH` | `/opt/model/...` | |
| `INFERENCE_SHA256SUMS` | `/opt/model/SHA256SUMS` | 預期 hash。image 建置時驗過後寫入，啟動時再驗一次；不符就拒絕啟動 |
| `INFERENCE_ONNX_SHA256`／`INFERENCE_TOKENIZER_SHA256` | — | 覆寫 SHA256SUMS（本機測試用） |

hash 不放在 Dockerfile 的 ENV：ENV 不支援行尾註解，放不下 `gitleaks:allow`。
gitleaks 會把「TOKENizer」當成 token 關鍵字而誤報。

## 相依版本

`requirements.in` 是直接相依；`requirements.lock` 是 uv 產生、帶 hash 的完整鎖檔。image 用 `--require-hashes` 安裝。

| 套件 | 版本 | 為什麼 |
|---|---|---|
| fastapi | 0.163.2 | **最舊的已修補版**。GHSA-f56c-v883-vx8r（2026-11-17，high，參數二次方處理 DoS）影響 `>=0.123.2,<0.163.2`；安全修補優先於「發布滿兩週」的慣例 |
| starlette | 1.9.0 | ≥1.3.1 對 form 欄位數有上限（同一公告提到的緩解） |
| pydantic | 2.14.2 | 滿兩週 |
| onnxruntime／tokenizers／numpy | 1.31.1／0.23.3／2.5.4 | 實驗 5-2a／5-2b 實際驗證過的版本 |
| uvicorn | 0.54.0 | |
| prometheus-client | 0.26.0 | |

傳遞相依裡有 `huggingface-hub`（含 `httpx2`、`hf-xet`、`fsspec`、`tqdm`）。它是 Python `tokenizers` 的**硬相依**，拿不掉。
本服務只讀本機檔，image 設了 `HF_HUB_OFFLINE=1` 禁止下載。

更新流程：

```bash
cd services/inference
# 改 requirements.in 後
uv pip compile requirements.in --generate-hashes -o requirements.lock --python-version 3.12 --python-platform x86_64-manylinux_2_28
uv pip compile requirements-dev.in --generate-hashes -o requirements-dev.lock --python-version 3.12 --python-platform x86_64-manylinux_2_28
uvx --from pip-audit==2.10.0 pip-audit -r requirements.lock --require-hashes --disable-pip
```

2026-11-24 的 pip-audit 結果：No known vulnerabilities found。

## 本機測試

```bash
# 模型檔不進 repo；放到 models/e5-small/（/models/ 已在 .gitignore）
scripts/fetch-e5-tokenizer.sh
curl -sSL -o models/e5-small/model.onnx \
  https://huggingface.co/intfloat/multilingual-e5-small/resolve/main/onnx/model_qint8_avx512_vnni.onnx
sha256sum models/e5-small/model.onnx   # 應為 dd476dd0…2d88

cd services/inference
uv venv --python 3.12 .venv
uv pip sync --python .venv/bin/python requirements.lock requirements-dev.lock
.venv/bin/python -m pytest -q tests
```

找不到模型檔時測試會**失敗**，不會跳過。

測試涵蓋：
- 黃金向量
- 同一段在不同批次裡向量完全相同
- 單位長度
- OpenAI 回應格式
- 超過 512 token 回 400
- 六種錯誤請求
- 佇列滿回 503
- health、ready、metrics
- hash 不符時拒絕載入

## 容器

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.dev.yml --profile app up -d osint-inference
```

容器設定：
- 以 uid 10001 執行；模型與程式都是 root 擁有、唯讀。
- HEALTHCHECK 用 Python 打 `/health`，不另外裝 curl。
- 單一 uvicorn worker，模型只載一份；SIGTERM 時最多等 10 秒，讓手上的請求做完。

2026-11-24 容器實測：

| 項目 | 結果 |
|---|---|
| 閒置記憶體 | 461 MiB／1 GiB |
| 黃金向量 | 最低 cosine 0.99631 |
| 超長輸入 | 400 |
| 20 個併發請求 | 4 個 200、16 個 503，都帶 `Retry-After: 1` |
