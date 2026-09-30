# 目前的限制

這一頁如實列出系統目前做不到的事、已知的設計邊界，以及各版本的狀態。

**讀這頁的原因**：避免把系統拿去做它設計上不支援的事。每一條限制都是現實，不是待修的 bug，除非另外說明。

---

## 版本狀態總表

| 版本 | 內容 | 狀態 |
|---|---|---|
| V0.1 資料基礎 | 收集、正規化、去重、實體抽取、搜尋投影 | 完成 |
| V0.2 情報分析 | 實體合併、關聯圖、語意搜尋、STIX 2.1 | 完成 |
| V0.3 發現引擎 | Seed/Candidate 資料模型、圖擴張 Discovery、AI Gateway | 完成 |
| V0.4 自動化基礎 | 規則引擎、警示、訊號偵測 | 未開始 |
| V1.0 正式環境就緒 | API 穩定性、正式維運、備份還原 | 未開始 |

---

## 操作介面

### 沒有網頁操作介面

**現況：** 所有操作透過 REST API 或命令列（`osint-cli`）。唯一能用眼睛看的是 Neo4j 內建的圖資料庫網頁。

**影響：** 不能在瀏覽器裡管理資料來源、審核候選、查看搜尋結果。

**替代做法：** 直接呼叫 API。快速上手指南有完整的 `curl` 範例。`osint-cli` 提供本機唯讀查詢，需要有 Rust 開發環境。

---

## 關聯圖的範圍

### 圖上只有實體和實體之間的關係，文件不是節點

**現況：** Neo4j 只存 Entity→Entity 的邊。文件和實體的關係（例如文件「提到」某個 CVE）存在 PostgreSQL 裡，查得到，但不是圖上的節點。

**影響：** 不能在 Neo4j 裡做「哪些文件同時提到這兩個 IP」這類跨文件圖查詢，那類查詢要靠搜尋索引或直接查 PostgreSQL。

---

## 實體抽取的範圍

### 不從自由文字辨識人名或組織名

**現況：** 實體抽取靠確定性規則（格式比對）。CVE、IP、網域、URL、Email、雜湊值這類有固定格式的指標可以自動抽出。人名只從文件的「作者」欄位取得。文章內文裡提到的人名和組織名不會被自動識別。

**影響：** 如果你上傳一篇文章，裡面提到「攻擊者來自 Phantom Group」，「Phantom Group」不會變成一個組織實體。

### 從收集或匯入進來的資料，不會產生組織實體

**現況：** 系統有「從發布單位、廠商、網站名稱等欄位取出組織名稱」的處理邏輯，但目前**沒有任何收集或匯入方式會把這些資訊填進去**——RSS、網頁、REST API、JSON、CSV 都一樣。所以即使來源資料寫明了發布單位（例如 JSON 裡的 `publisher` 欄位），也不會變成組織實體。

**影響：** 無法用「這個組織發布過哪些公告」來查詢一般收集到的資料。

**替代做法：** 目前唯一會產生組織實體的方式是 **STIX 匯入**——STIX 資料裡的組織（`identity`）會直接成為組織實體。見 [STIX 匯入匯出](../usage/stix.md)。系統沒有提供手動建立實體或關聯的 API。

---

## Discovery（發現新關聯）的範圍

### 只在已收集的資料裡尋找

**現況：** Discovery 在 Neo4j 關聯圖裡尋找與已知實體相關的目標。系統不會主動上網搜尋或呼叫外部情報 API。

**影響：** Discovery 找得到什麼，取決於系統已經收進來多少資料。如果一個相關目標從未出現在任何收集到的文件裡，Discovery 找不到它。

### 目前只往外看 1 層鄰居

**現況：** Graph Expansion 方法找的是目標實體在 Neo4j 裡的直接鄰居（1-hop），不做多層遞迴展開。

**影響：** 「A 認識 B，B 認識 C，所以 C 可能跟 A 有關」這類間接關聯，目前的 Discovery 找不到。

### 觸發點只有實體（Entity）

**現況：** 目前 Discovery 的起點是一個已存在的 Entity（透過 `POST /entities/{id}/discover` 或 `POST /discovery/run` 加 seed）。不能直接用文字關鍵字或集合（Collection）做為起點觸發 Discovery。

---

## AI 相關的限制

### Discovery 的每日 AI 配額欄位設定了但尚未生效

**現況：** Discovery Budget 裡的 `max_ai_calls_per_run`、`daily_ai_budget` 這兩個配額欄位在介面和資料庫 schema 都存在，但目前沒有任何執行路徑會消耗它們（確認方式：`grep -rn "try_consume_daily_ai_budget" crates/ --include="*.rs"` 在生產路徑零命中）。真正會被消耗的配額只有 `max_candidates_per_run`、`daily_request_budget`、`max_depth`。

**影響：** 就算把 `daily_ai_budget` 設為 0，目前也不會阻止任何 AI 呼叫。

### 資源壓力感知永遠回報「正常」

**現況：** AI Gateway 的 Admission Controller 有「資源壓力升高時降級低優先權請求」的邏輯，但目前唯一的壓力訊號實作（`StaticPressure`）永遠回報正常狀態（確認：`crates/ai-gateway/src/admission.rs` 的 `StaticPressure::current()` 直接回傳 `ResourcePressure::Normal`）。

**影響：** 自動降級機制（高壓力擋 P2+ 請求）在目前部署下永遠不會觸發。要手動控制 AI 並發量，需要調整設定檔裡的 `auto_approval.llm.max_concurrent`。

---

## 語意搜尋（向量搜尋）

### 語意搜尋需要先部署向量模型

**現況：** 語意搜尋和混合搜尋依賴 OpenSearch ml-commons 上部署的向量模型。這個模型不會自動安裝，需要執行 `scripts/opensearch-ml-setup.sh`（英文）或 `scripts/opensearch-ml-setup-e5.sh`（多語言）。

**影響：** 沒有部署模型時，語意搜尋的 API 呼叫會失敗。關鍵字搜尋不受影響。

### OpenSearch 重啟後需要重新部署模型

**現況：** OpenSearch ml-commons 的模型在重啟後需要重新載入（re-deploy）。這個過程需要手動執行，不是自動的。

**替代做法：** 如果不需要語意搜尋，關鍵字搜尋（包含布林、日期範圍、語言過濾）在任何狀態下都可用。

---

## 安全性

### Kafka 連線目前沒有 TLS 或 SASL 認證

**現況：** `rust-rdkafka` 在這個部署以 `--disable-ssl` 編譯（確認：`Cargo.toml` 第 101 行，`rdkafka` 的 features 只有 `["tokio", "libz"]`，不含 ssl 相關功能）。Redpanda 連線是明文，沒有傳輸層加密，也沒有身分認證。

**影響：** 在同一個網路上的任何程式都可以連線到 Redpanda 端口，讀取或寫入事件。

**目前的替代做法：** 在網路層隔離（僅允許這些服務所在的主機存取 Redpanda 端口）。**正式環境部署前必須在網路層做隔離，或重新編譯加入 TLS/SASL 支援。**

### SQLite 後端的稽核記錄與 API Token 尚未實作

**現況：** 使用 SQLite 作為後端時（用於本機應用程式開發），`AuditLog` 和 `ApiTokenStore` 的持久化未實作（migration schema 已建立，但 store 層沒有對應的寫入邏輯）。Token 只在記憶體裡，服務重啟後失效。

**影響：** 這只影響使用 SQLite 的情境。使用 PostgreSQL 的正式部署不受影響。

---

## 維運可見性

### 稽核記錄沒有保留期限

**現況：** `audit_log` 表目前沒有任何自動刪除或封存機制，資料無限期保留。`auth.failed`（認證失敗）事件的寫入速率可被外部控制（例如大量嘗試登入）。

**影響：** 長期運行後 `audit_log` 表會持續增長，需要手動維護。

**目前替代做法：** 定期手動清理超過特定日期的稽核記錄，或限制服務的公開存取。

### 物件儲存健康檢查只確認閘道存活

**現況：** SeaweedFS 的健康檢查（`GET /status`）只確認 S3 閘道的 HTTP 端口有人接，不驗證底層的 filer 或 master 服務是否存活（確認：`docker/docker-compose.yml` 的 `seaweedfs` healthcheck 使用 `curl -sf http://127.0.0.1:8333/status`，此端點固定回 200）。

**影響：** 健康檢查全綠不代表物件讀寫正常。若 filer 已掛，上傳原始證據的請求會失敗，但健康狀態仍顯示為健康。

**確認物件層是否真的可用：** 嘗試實際上傳一筆資料，或執行 `cargo test -p storage-s3`。

---

## 背壓與資料流控

### 跨服務自動背壓尚未實作

**現況：** 系統有監控指標 `osint_queue_depth` 可以反映下游積壓，但收集器的主迴圈是固定間隔執行，不會讀取這個指標來降速（確認：`grep -rn queue_depth crates/collector/src/ | wc -l` 回傳 0）。

**影響：** 當下游（例如 OpenSearch）處理速度跟不上收集速度時，Redpanda 的積壓會增加，但收集器不會自動放慢。

**目前替代做法：** 手動調整 `[collector].tick_secs`（收集間隔）、`global_inflight`（全域並發上限）、`per_domain_inflight`（每網域並發上限），或暫停特定 Connector。

---

## 資料完整性的已知邊界

### Graph Expansion 重跑同一個實體可能產生重複候選

**現況：** 對同一個 Entity 在同一個 Collection 裡重複執行 Discovery，會產生重複的 Candidate 列（目前沒有先查重再插入的邏輯）。

**影響：** 候選清單裡可能出現同樣內容的重複項目。

**目前替代做法：** 核准或拒絕時，重複的候選都需要分別處理。下一步 Discovery 工作完成後此行為可能改變。

### Candidate 核准的 URL 只驗格式，不驗 scheme

**現況：** 核准候選時，URL 驗證只確認能解析成合法的絕對 URL，`javascript:` 或 `data:` 這類有 scheme 的不透明 URL 不會被擋下（確認：`crates/core-api/src/resources/discovery.rs` 第 318–319 行的已知邊界說明）。

**影響：** 若有不受信任的資料來源被誤判為候選，核准時應額外確認 URL scheme。

### STIX 匯出對關係超過 100 條的實體會靜默略過部分關係

**現況：** `EXPORT_TRAVERSAL_LIMIT = 100`（確認：`crates/stix-worker/src/export.rs` 第 38 行）。任何一個 Entity 若參與超過 100 條關係，匯出時只會取前 100 條，多出來的靜默略過，不會報錯。

**影響：** 核心關聯超過 100 條的高度連結實體（例如知名 CVE）在 STIX 匯出時，接收方只會看到部分關係。

---

## Collection 的歸屬

### 收集的資料目前不附掛到任何 Collection

**現況：** Collector（主動收集）、`/api/v1/import`（手動匯入）、STIX 匯入這三條路徑在建立 RawEvidence 時，`collection_id` 都寫成 `None`（確認：`crates/collector/src/runner.rs` 第 263 行）。

**影響：** 即使建立了 Collection，已收集的資料也不會自動歸屬到它。Discovery 的 per-collection 配額和 Timeline 功能，目前無法依 Collection 篩選收集進來的資料。
