# 系統需求

這一頁說明把整套系統跑起來需要哪些軟硬體。

---

## 軟體需求

| 軟體 | 版本 | 確認指令 | 備注 |
|---|---|---|---|
| Docker | 24 以上 | `docker --version` | |
| Docker Compose | **2.24 以上** | `docker compose version` | compose 設定用到 `!override` 語法（dev override 覆寫 OpenSearch 埠），低於 2.24 會解析失敗 |
| make | 任意 | `make --version` | |
| Python 3 | 3.9 以上 | `python3 --version` | 僅用於產生開發憑證 |
| PyJWT | 任意 | `python3 -c "import jwt"` | `pip install pyjwt` 或 `sudo apt install python3-jwt` |
| curl | 任意 | `curl --version` | 部署語意搜尋模型的腳本會用到 |

**不需要安裝 Rust**——所有服務都在 Docker 裡編譯與執行。

---

## 硬體建議值

下表數字來自 `docker/docker-compose.dev.yml` 的 `mem_limit` 與 `cpus` 加總，反映的是**開發環境的容器上限**，不是正式部署的容量規劃。正式部署應依實際流量與 `docs/architecture/RESOURCE_BUDGET.md` 的原則重新評估。

### 記憶體

| 類別 | 加總 |
|---|---|
| 基礎設施（6 個服務） | PostgreSQL 1 GB + OpenSearch 3 GB + Redpanda 512 MB + Redis 256 MB + SeaweedFS 512 MB + Neo4j 1 GB = **約 6.25 GB** |
| 應用服務（10 個服務） | osint-api 512 MB + osint-indexer 384 MB + 其餘 8 個各 256 MB = **約 2.9 GB** |
| **合計** | **約 9.1 GB** |

!!! note "OpenSearch 的記憶體不能縮減"
    OpenSearch 設為 3 GB（JVM heap 1536 MB）是語意搜尋模型的最低需求，不是餘裕。
    兩個 embedding 模型（英文 MiniLM + 多語 e5-small）並存時，低於這個值模型無法部署，
    而且容器**不會崩潰、健康檢查仍然全綠**——症狀只有 `embedding-worker` 一直重啟。

建議主機至少 **16 GB RAM**（給 OS 與其他行程留餘裕）。

### CPU

開發環境容器上限加總約 **8 CPU**（`cpus` 是 cgroup 上限，不是實際用量；這些消費者大部分時間閒置等訊息）。

建議主機至少 **4 核心**，若主機同時跑其他工作負載（例如虛擬機）建議 8 核心以上。

### 磁碟

| 項目 | 估計大小 |
|---|---|
| Docker 映像檔（所有服務） | 約 3–5 GB（Rust 編譯產物） |
| 語意搜尋模型（第一次部署下載） | 約 **600 MB** |
| - 英文 all-MiniLM-L6-v2（ONNX 本體） | 約 92 MB |
| - DJL PyTorch native libs | 約 507 MB |
| 資料 volume（PostgreSQL、OpenSearch、Neo4j…） | 視收集量而定 |

建議至少 **20 GB** 可用磁碟空間。

!!! warning "磁碟滿會讓 OpenSearch 變唯讀"
    OpenSearch 預設在磁碟使用率超過 95% 時進入唯讀模式，拒絕新的寫入。
    症狀是搜尋索引停止更新、匯入資料後查不到新內容。
    恢復方式見 [疑難排解](troubleshooting.md)。

---

## 網路需求

### 部署語意搜尋模型時需要對外連線

執行 `bash scripts/opensearch-ml-setup.sh` 與 `bash scripts/opensearch-ml-setup-e5.sh` 時，
OpenSearch 容器會從外部下載以下資源（合計約 600 MB）：

| 來源 | 內容 |
|---|---|
| `artifacts.opensearch.org` | 英文模型本體（ONNX，約 92 MB） |
| `publish.djl.ai` | PyTorch CPU native libs（約 507 MB） |
| `huggingface.co` | 多語 e5-small tokenizer 相關資源 |

!!! warning "離線環境"
    封閉網路環境必須預先把 `ml_cache` volume 準備好再掛進 OpenSearch 容器，否則模型部署會卡住不動。
    兩支腳本都是冪等的——模型已部署的情況下重跑會直接跳過，不重複下載。

### 平時不需要對外連線

模型部署完成後，系統日常運作（收集、正規化、搜尋、圖查詢）全部在本地完成，不需要對外連線。
