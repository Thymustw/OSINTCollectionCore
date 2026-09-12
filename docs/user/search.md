# 搜尋

在已經採集、正規化、去重、抽取完的文件裡找東西。

兩條路徑，語法與結果完全相同：

```bash
# 終端機（直接查資料庫，只能在本機用）
osint-cli search "ransomware"

# API（有認證與權限）
curl -s http://127.0.0.1:18080/api/v1/search \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"query": "ransomware"}'
```

> 搜尋到的東西來自**搜尋投影**，不是資料庫本身。投影由 `osint-indexer` 建立；
> 它沒在跑的話，新採集的文件不會出現在搜尋結果裡（資料本身還在，沒有遺失）。
> 詳見下面的「查不到東西的時候」。

---

## 查詢語法

### 關鍵字

```text
ransomware
```

找含這個詞的文件。中文一樣：

```text
勒索軟體
```

### 兩個以上的詞：預設**都要有**

```text
lockbit ransomware
```

等同 `lockbit AND ransomware`。多打一個字只會讓結果變少，不會變多。

### 片語（詞序要一樣）

用雙引號：

```text
"ransomware gang"
```

這只會找到「ransomware gang」連在一起的文件。`"gang ransomware"` 找不到同一批。

### 布林：`AND` / `OR` / `NOT`

```text
lockbit AND ransomware        兩個都要有
lockbit OR blackcat           至少一個
ransomware NOT decryptor      有 ransomware、沒有 decryptor
```

**必須全大寫。** 小寫的 `and` 會被當成一個普通的詞——
`crowdstrike and falcon` 是找這三個詞，不是布林運算。

### 括號

```text
(lockbit OR blackcat) AND ransomware
```

沒有括號時 `AND` 比 `OR` 先算：`a AND b OR c` 等於 `(a AND b) OR c`。

### 特殊符號沒有特殊意義

`*`、`?`、`~`、`欄位名:值`、`/正規表示式/` **都只是普通文字**。
搜 `title:*` 就是在找「title:*」這串字，不是在找「所有有標題的文件」。

這是刻意的設計（避免有人用一個查詢字串把服務拖垮）。要縮小範圍請用下面的篩選條件。

---

## 篩選條件

全部可以和查詢字串一起用，也可以只用篩選條件不給查詢字串
（例如「列出這個來源的所有文件」）。

| 條件 | CLI | API |
|---|---|---|
| 來源 | `--source <SOURCE_ID>` | `"source_id": "…"` |
| 連接器 | `--connector <CONNECTOR_ID>` | `"connector_id": "…"` |
| 實體 | `--entity vulnerability:CVE-2026-0001` | `"entity": {"type": "…", "name": "…"}` |
| 起始時間 | `--from 2026-01-01` | `"date_from": "2026-01-01T00:00:00Z"` |
| 結束時間 | `--to 2026-09-10` | `"date_to": "2026-09-10T23:59:59Z"` |
| 比對哪個時間 | `--date-field published` | `"date_field": "published"` |
| 語言 | `--lang en` | `"language": "en"` |
| 物件型別 | `--type article` | `"object_type": "article"` |
| 筆數 | `-n 50` | `"limit": 50` |

### 實體（entity）

找「提到某個 CVE／IP／網域／信箱／雜湊的文件」：

```bash
osint-cli search --entity vulnerability:CVE-2026-0001
osint-cli search --entity domain:example.com
osint-cli search --entity 203.0.113.5          # 不確定型別就只給名稱
```

* **大小寫不敏感**：`cve-2026-0001` 與 `CVE-2026-0001` 結果相同。
* 型別可省略。給了型別就必須是同一個實體同時符合型別與名稱。
* 型別名稱：`vulnerability`／`ip`／`domain`／`hostname`／`url`／`email`／`hash`／
  `person`／`organization`／`account`／`file`／`malware`／`campaign`／`location`／`event`。
* 名稱本身含冒號（IPv6、網址）不用跳脫——只有已知型別開頭才會被當成型別前綴：
  `--entity 2001:db8::1` 會整串當作名稱。

### 時間

CLI 接受 `YYYY-MM-DD` 或完整時間 `2026-09-10T12:00:00Z`。
`--to 2026-09-10` 會**包含當天一整天**（自動補到 23:59:59）。

`--date-field` 決定比對哪一個時間：

| 值 | 意義 |
|---|---|
| `effective`（預設） | 有發布時間就用發布時間，沒有就用「本系統第一次看到它的時間」 |
| `published` | 只看發布時間。**沒有發布時間的文件不會出現在結果裡** |
| `observed` | 只看「本系統第一次看到它的時間」 |

預設是 `effective`，因為不少來源（靜態網頁、部分 API、手動匯入）根本沒有發布時間。
若用 `published` 而結果比預期少很多，多半就是這個原因。

### 語言與物件型別

* 語言用來源標示的代碼（`en`／`zh`／`ja`…），大小寫不敏感。
* 物件型別：`article`／`web_page`／`post`／`message`／`report`／`file`／`advisory`。

---

## 結果

### CLI

預設是表格（document_id、分數、標題、片段、發布時間）。
要看完整欄位（含 `raw_evidence_id` 與 entities）加 `--json`：

```bash
osint-cli search "ransomware" --json | jq '.hits[] | {document_id, raw_evidence_id}'
```

### 每筆結果都帶得回原始證據

`raw_evidence_id` 是這份文件的原始採集紀錄。拿它可以一路查回來源與連接器：

```bash
osint-cli raw show <RAW_EVIDENCE_ID>          # 含 source_id / connector_id
osint-cli raw show <RAW_EVIDENCE_ID> --body   # 連原始內容一起看
osint-cli sources show <SOURCE_ID>
osint-cli connectors show <CONNECTOR_ID>
```

### 重複的文件不會出現

同一篇文章被多個來源轉載時，只有第一份（canonical）會出現在搜尋結果裡。
要看重複關係請用 `osint-cli documents show <DOCUMENT_ID>`。

### 排序

有查詢字串時依相關度排；只有篩選條件時依時間由新到舊。

### 分頁（API）

回應的 `next_cursor` 放進下一次請求的 `cursor`：

```jsonc
{ "query": "ransomware", "limit": 20, "cursor": "eyJ..." }
```

`next_cursor` 是 `null` 就代表沒有下一頁。**不要自己組 cursor**，一律用回應裡的值。

CLI 一次只取一頁，要更多筆請調大 `-n`（上限 100）。

---

## 中文搜尋

中文用**兩字一組**的方式比對（沒有裝中文斷詞套件）。實務上的影響：

* 查「勒索軟體」會正確找到含這個詞的文件，不會把任何含「軟」或「體」的文件都撈出來。
* 偶爾會有跨詞邊界的誤中（「防毒軟體。攻擊者…」這種）。
  要精確請用引號：`"勒索軟體攻擊"`。

---

## 查不到東西的時候

依序確認：

1. **投影有沒有建立。** 搜尋查的是 OpenSearch，不是 PostgreSQL。
   `osint-indexer` 沒跑過的話它是空的：

   ```bash
   make run-indexer        # 常駐，跟著新資料即時索引
   make rebuild-index      # 一次性：從資料庫補齊
   ```

2. **服務是不是活著。**

   ```bash
   osint-cli health
   ```

3. **條件是不是太窄。** 先把篩選條件全部拿掉只留關鍵字，再一個一個加回去。
   特別常見的是 `--date-field published` 把沒有發布時間的文件全部排除了。

4. **是不是被當成重複文件。** 搜尋預設不顯示重複文件。

5. **關鍵字是不是被當成語法。** `*`、`欄位名:值` 不是萬用字元或欄位查詢，
   它們就只是文字。

---

## 相關

- `docs/user/cli.md`：`osint-cli` 的其他子命令
- `docs/user/connectors.md`：資料是怎麼進來的
