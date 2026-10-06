# 快速上手

跟著這一頁做，你會：

1. 把整套系統啟動起來（10 個服務 + 6 個基礎設施）
2. 拿到一把 API 憑證
3. 匯入兩篇範例資安公告
4. 看到系統自動抽出的漏洞、IP、網域
5. 用搜尋找到它們
6. 在圖資料庫的網頁介面看到實體之間的關聯

全程大約 **20 分鐘**，其中大部分是第一次建置等待的時間。

!!! note "這一頁的每一個指令都實際跑過"
    撰寫時在一台乾淨啟動的環境上逐步執行並確認結果。如果你照做卻得到不一樣的結果，
    請看最下方的 [遇到問題](#遇到問題)。

---

## 0. 準備

| 需要 | 版本 | 確認指令 |
|---|---|---|
| Docker | 24 以上 | `docker --version` |
| Docker Compose | **2.24 以上**（用到 `!override` 語法） | `docker compose version` |
| make | 任意 | `make --version` |
| Python 3 + PyJWT | 3.9 以上 | `python3 -c "import jwt"` |
| curl | 任意 | `curl --version` |

PyJWT 沒裝的話：`pip install pyjwt`（Debian/Ubuntu 也可以 `sudo apt install python3-jwt`）。

硬體：建議至少 **16 GB RAM、20 GB 可用磁碟**。完整說明見 [系統需求](../operations/requirements.md)。

**不需要**安裝 Rust——所有程式都在 Docker 裡編譯。

取得原始碼並進入目錄：

```bash
git clone https://github.com/Thymustw/OSINTCollectionCore.git
cd OSINTCollectionCore
```

以下所有指令都在這個目錄下執行。

---

## 1. 啟動基礎設施

先只啟動資料庫、搜尋引擎、訊息佇列這些基礎設施：

```bash
make compose-up
```

等所有容器變成 `healthy`（大約 1 分鐘）：

```bash
make compose-ps
```

你應該看到這 6 個都是 `(healthy)`：`postgres`、`opensearch`、`neo4j`、`redpanda`、
`redis`、`seaweedfs`。

!!! info "為什麼不直接一次全部啟動？"
    語意搜尋需要先在 OpenSearch 裡部署兩個 AI 向量模型。模型還沒部署的話，
    負責產生向量的服務（`osint-embedding-worker`）會發現模型不能用而主動停止——
    這是刻意的設計（不假裝正常運作），但會讓一次全部啟動的指令失敗。
    所以順序是：**基礎設施 → 部署模型 → 應用服務**。

---

## 2. 部署語意搜尋模型

依序執行兩支腳本（第一次會下載約 600 MB，需要 5～10 分鐘）：

```bash
bash scripts/opensearch-ml-setup.sh
bash scripts/opensearch-ml-setup-e5.sh
```

兩支都應該以類似這樣的訊息結束：

```text
   維度 384 ✅
   語意排序正確（差 0.790025）✅
== 完成。model_id=...
```

第一支是英文模型，第二支是多語（含中文）模型。兩支都可以重複執行——模型已經部署好的話會直接跳過。

過程中如果看到 `推論暫時被斷路器擋下（第 1 次），10 秒後重試…`，**這是正常的**：模型剛部署完，
記憶體用量會短暫衝高，腳本會自動等待並重試。只有重試 6 次（約 1 分鐘）後仍然失敗才需要處理，
見 [疑難排解](../operations/troubleshooting.md)。

!!! warning "OpenSearch 重新啟動後要再跑一次"
    模型的「已部署」狀態不會跨 OpenSearch 重啟保留。如果你之後重開過基礎設施，
    `osint-embedding-worker` 又一直重啟，就重跑這兩支腳本（已下載的檔案不會重下載）。

---

## 3. 啟動全部應用服務

```bash
make compose-up-full
```

第一次會從原始碼編譯 10 個服務，**需要 5～15 分鐘**（依機器效能）。之後再啟動只要幾十秒。

完成後確認 10 個應用服務都是 `healthy`：

```bash
make compose-ps-full
```

也可以直接打每個服務的健康檢查端點：

```bash
for p in 18080 18081 18082 18083 18084 18085 18086 18087 18088 18089; do
  curl -s localhost:$p/health; echo
done
```

每一行都應該是 `{"status":"ok"}`。

---

## 4. 取得 API 憑證

所有 API 都需要憑證。用內附的腳本產生一把本機開發用的憑證：

```bash
export TOKEN=$(python3 scripts/dev-token.py)
export API=http://127.0.0.1:18080/api/v1
```

確認有效：

```bash
curl -s $API/whoami -H "Authorization: Bearer $TOKEN"
```

```json
{"auth_method":"Jwt","role":"admin","subject":"local-dev"}
```

!!! warning "這把憑證只能在本機開發用"
    它是用 compose 內建的開發用密鑰簽的，有效期 1 小時（過期就重跑上面的指令）。
    正式環境的做法見 [取得 API 憑證](../usage/authentication.md)。

---

## 5. 匯入範例資料

### 5.1 建立一個資料來源

每一筆資料都要掛在一個「來源（Source）」底下，這樣之後才追得回資料是從哪裡來的：

```bash
export SOURCE_ID=$(curl -s -X POST $API/sources \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"快速上手範例","source_type":"json_import","language":"zh"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
echo $SOURCE_ID
```

### 5.2 匯入兩篇資安公告

範例檔案 [`demo-advisory.json`](../assets/examples/demo-advisory.json) 內含兩篇虛構的資安公告，
刻意讓它們**共用同一個漏洞、同一個攻擊來源 IP、同一個 C2 網域**——這樣你會看到系統
怎麼把分散在不同報告裡的相同指標串起來。

這個檔案已經在 repo 裡（`docs-site/docs/assets/examples/demo-advisory.json`），直接匯入：

```bash
curl -s -X POST $API/import \
  -H "Authorization: Bearer $TOKEN" \
  -F "request={\"source_id\":\"$SOURCE_ID\",\"kind\":\"json\",\"object_type\":\"advisory\"};type=application/json" \
  -F 'file=@docs-site/docs/assets/examples/demo-advisory.json'
```

回應會像這樣：

```json
{
  "record_count": 2,
  "published": true,
  "message": "已收下並發出 raw.collected，normalizer 會接手正規化",
  "raw_evidence_id": "...",
  ...
}
```

- `record_count: 2`：兩篇都收到了
- `published: true`：已經交給後面的處理流程

接下來系統會在背景自動做：整理成文件 → 去重 → 抽取指標 → 建搜尋索引 → 投影到關聯圖。
**等 10 秒再往下做。**

---

## 6. 看看系統抽出了什麼

### 6.1 文件

```bash
curl -s "$API/objects?object_type=advisory&limit=5" -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import sys,json; [print(o['title']) for o in json.load(sys.stdin)['items']]"
```

```text
威脅情報：針對 Acme Router 的攻擊活動持續擴大
Acme Router 韌體多個遠端程式碼執行漏洞
```

### 6.2 自動抽出的實體

列出系統抽出的漏洞：

```bash
curl -s "$API/entities?entity_type=vulnerability" -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import sys,json; [print(e['name']) for e in json.load(sys.stdin)['items']]"
```

```text
CVE-2026-31337
CVE-2026-31338
```

把 `entity_type=vulnerability` 換成 `ip`、`domain`、`url`、`email`、`person`、`organization`，就能看其他類型。
從這兩篇公告，系統總共會抽出：

| 類型 | 抽出的值 | 出現在幾篇 |
|---|---|---|
| 漏洞 | `CVE-2026-31337` | **2 篇** |
| 漏洞 | `CVE-2026-31338` | 1 篇 |
| IP | `203.0.113.45` | **2 篇** |
| IP | `198.51.100.77` | 1 篇 |
| 網域 | `c2.malicious-example.net` | **2 篇** |
| 網域 | `acme-example.com`、`advisories.example.com` | 1 篇 |
| 網址 | `https://advisories.example.com/acme-2026-0928` | 1 篇 |
| Email | `psirt@acme-example.com` | 1 篇 |
| 人員 | `Acme PSIRT`、`Example Research Team`（來自作者欄位） | 各 1 篇 |
| 組織 | `Acme Security Response`、`Example Threat Intel`（來自發布單位欄位） | 各 1 篇 |

**粗體的三個就是兩篇報告的交集**——雖然來自不同來源、不同日期，但系統已經知道它們提到了
同一個漏洞、同一個攻擊 IP、同一個 C2。

### 6.3 一個漏洞出現在哪些報告裡

這是情報分析最常問的問題之一。先取得 `CVE-2026-31337` 的 id，再查它的明細：

```bash
export CVE_ID=$(curl -s "$API/entities?entity_type=vulnerability" -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import sys,json; print(next(e['id'] for e in json.load(sys.stdin)['items'] if e['name']=='CVE-2026-31337'))")

curl -s "$API/entities/$CVE_ID" -H "Authorization: Bearer $TOKEN" \
  | python3 -c "
import sys,json; d=json.load(sys.stdin)
print(d['name'], '— 出現在', len(d['recent_extractions']), '篇文件')
for x in d['recent_extractions']: print('  -', x['excerpt'].splitlines()[0])"
```

```text
CVE-2026-31337 — 出現在 2 篇文件
  - 威脅情報：針對 Acme Router 的攻擊活動持續擴大
  - Acme Router 韌體多個遠端程式碼執行漏洞
```

每一筆都能再往回追到原始的匯入檔案、匯入時間與匯入者——這就是「完整來源證據鏈」。

!!! note "發布單位會變成組織實體；內文裡的組織名不會"
    範例資料裡的 `publisher`（發布單位）會寫進文件屬性，抽出成組織實體，並與文件建立
    `published_by` 關聯。系統**不會**從內文自由文字辨識組織名（「攻擊者來自 Phantom Group」
    不會自動變成實體）。人名來自作者欄位。詳見 [實體類型](../reference/entity-types.md)。

---

## 7. 搜尋

```bash
curl -s -X POST $API/search \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"query":"CVE-2026-31337"}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('共', d['total'], '篇'); [print(' -', h['title']) for h in d['hits']]"
```

```text
共 2 篇
 - Acme Router 韌體多個遠端程式碼執行漏洞
 - 威脅情報：針對 Acme Router 的攻擊活動持續擴大
```

（結果依相關度分數排序，兩篇的先後順序可能跟這裡不同。）

中文也可以：

```bash
curl -s -X POST $API/search \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"query":"韌體"}'
```

!!! warning "欄位名稱是 `query`，不是 `q`"
    寫成 `{"q": "..."}` 會得到 422 錯誤（`unknown field`）。

更多搜尋語法（布林、片語、日期範圍、語意搜尋）見 [搜尋](../usage/search.md)。

---

## 8. 在關聯圖上看

打開瀏覽器到 **<http://127.0.0.1:7474>**，登入：

| 欄位 | 填入 |
|---|---|
| Connect URL | `neo4j://127.0.0.1:7687`（預設值，不用改） |
| Username | `neo4j` |
| Password | `osint_dev_neo4j` |

登入後，在最上方的輸入框貼上這段查詢，按執行（▶）：

```cypher
MATCH (a:Entity)-[r]->(b:Entity)
WHERE a.display_name IN ['https://advisories.example.com/acme-2026-0928', 'psirt@acme-example.com']
RETURN a, r, b
```

你會看到兩組關聯：

- 網址 `https://advisories.example.com/...` **屬於（BELONGS_TO）** 網域 `advisories.example.com`
- Email `psirt@acme-example.com` **關聯到（ASSOCIATED_WITH）** 網域 `acme-example.com`

想看圖上所有東西（資料多時會很擠）：

```cypher
MATCH (a:Entity)-[r]->(b:Entity) RETURN a, r, b LIMIT 100
```

!!! warning "圖上只有「實體和實體之間」的關係"
    「哪篇文件提到了哪個 CVE」這類**文件與實體之間**的關係，**不會**出現在圖上——
    文件本身不是圖上的節點。所以第 6 步看到的「兩篇報告共用同一個 CVE」，
    在圖上看不到兩篇報告被連起來。

    同樣的道理，第 6 步抽出的**組織**（`Acme Security Response` 等）也**不會**出現在圖上：
    它和文件之間是「文件由某組織發布」的關係，屬於文件與實體之間，不是實體與實體之間。
    組織有被正確抽出，用 `entity_type=organization` 的 API 查得到。

    要查「這個 CVE 出現在哪些文件」，用第 6.3 步的 API。

    這是目前的設計限制，見 [目前的限制](../architecture/limitations.md)。

---

## 9. 發現新關聯（進階，可選）

從一個已知實體出發，讓系統找出相關的漏洞、程式庫、產品與情報來源：

```bash
# 沿用第 6.3 步的 $CVE_ID

# 建一個調查集合（Collection）
export COLLECTION_ID=$(curl -s -X POST $API/collections \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"name":"Acme Router 調查"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

# 從這個實體出發發現
curl -s -X POST "$API/entities/$CVE_ID/discover" \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d "{\"collection_id\":\"$COLLECTION_ID\"}"
```

這會建立一個背景工作，回應的 `status` 是 `queued`。等幾秒後看它找到了什麼：

```bash
curl -s "$API/candidates?collection_id=$COLLECTION_ID" -H "Authorization: Bearer $TOKEN" \
  | python3 -c "
import sys,json
for c in json.load(sys.stdin)['items']:
    print(c['candidate_type'], c['value'], '|', c['discovery_method'], '|', c['status'])"
```

```text
source 快速上手範例 | source_expansion | pending
```

系統從 `CVE-2026-31337` 出發，發現「快速上手範例」這個資料來源提到過它。這筆結果是
**候選（`pending`）**——系統找到的東西一律要經過人工核准，不會直接當成確定的結論。

完整說明（核准／拒絕候選、查看證據、預算限制）見 [發現新關聯](../usage/discovery.md)。

---

## 停止與清除

```bash
make compose-down-full     # 停止全部（資料保留，下次啟動還在）
```

要**連資料一起清掉**、回到全新狀態：

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.dev.yml \
  --profile app down -v
```

!!! danger "`-v` 會刪掉所有資料"
    包括資料庫、搜尋索引、關聯圖、原始證據檔案，以及已部署的語意搜尋模型
    （下次要重跑第 2 步）。

---

## 遇到問題

| 症狀 | 原因與解法 |
|---|---|
| `make compose-up-full` 失敗，訊息寫 `osint-embedding-worker is unhealthy` | 語意搜尋模型沒部署或 OpenSearch 重啟過。重跑第 2 步的兩支腳本 |
| `opensearch-ml-setup-e5.sh` 失敗：「臨時 HTTP 服務起不來」 | 預設用 18099 埠。被佔用的話換一個：`E5_SERVE_PORT=18199 bash scripts/opensearch-ml-setup-e5.sh` |
| API 回 **401** | 憑證過期（有效 1 小時）。重跑第 4 步的 `export TOKEN=...` |
| 搜尋回 **422** `unknown field` | 請求欄位要用 `query`，不是 `q` |
| 匯入後查不到文件 | 等久一點（第一次處理可能要 30 秒）。仍然沒有的話看 [疑難排解](../operations/troubleshooting.md) |
| 埠號衝突（`address already in use`） | 18080–18089、5432、7474、7687、8333、19200 其中之一被佔用。見 [服務與埠號](../operations/services.md) |

更多狀況見 [疑難排解](../operations/troubleshooting.md)。

---

## 下一步

- [核心概念](concepts.md)：了解 Source、原始證據、文件、實體、關聯這些名詞
- [使用指南](../usage/index.md)：每個功能的完整用法
