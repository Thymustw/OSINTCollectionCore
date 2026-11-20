# `chunker`：段落切分核心（ADR-016 D6／D7，階段 5 M1-2）

純計算 crate：輸入一份 `Document`，輸出 `storage_core::DocumentChunk`。唯一的 I/O 是啟動時讀
`tokenizer.json`。寫庫、發 `document.chunked` 是 chunk-worker（M1-3）的事；資料契約見
`storage-adapters.md` 的「`ChunkStore`」。

## tokenizer 檔

段落長度一律用 e5（XLM-RoBERTa）自己的斷詞器算（D7）。檔案 17 MB、上游公開，**不進 repo**：

```bash
scripts/fetch-e5-tokenizer.sh            # → models/e5-small/tokenizer.json（/models/ 已在 .gitignore）
```

- 載入時比對 sha256（`E5_TOKENIZER_SHA256`），不符直接拒絕。同一顆 hash 也在
  `scripts/opensearch-ml-setup-e5.sh`（ml-commons 部署的就是這份）與 `fetch-e5-tokenizer.sh`，三處一起改。
- `tokenizers` 0.23.2 只開 `fancy-regex`：`cargo tree -p chunker -i cc` 為空，沒有 C／C++ 編譯。
  實驗 5-2c 證實與 Python 版 30／30 句 id 完全相同。
- 載入後明確關掉 truncation／padding。若檔案帶 truncation，超長段落會被截成剛好 512 而**通過**保護檢查。

## 策略（`select_strategy`）

| 文件 | 策略 | 說明 |
|---|---|---|
| `attributes.import_kind` 有值（JSON／CSV 匯入） | `record` | 一筆一段 |
| `object_type = article` 且非匯入（RSS／Atom） | `whole` | 不切，整篇一段 |
| 其他（`web_page`、`advisory`、`report`…） | `fixed` | 300 token／重疊 100 |

D6 的網頁／長篇報告預設是語意切段，那是 M3；在那之前走 `fixed`。

`fixed` 的打包方式：先斷句，再把整句貪婪塞進 300 token；下一段往回帶最多 100 token 的**完整句子**當重疊。
重疊以句為單位，所以實際重疊不會剛好 100。

## 512 token 保護

比的是「模型實際吃到的」：`passage: ` 前綴＋`embedding_text(header, text)`＋特殊 token。

| 情況 | 處理 |
|---|---|
| `whole`／`record` 超過上限 | 在這份文件內部改用 300／100 打包。段落仍記原策略名稱，這個退路是 `whole@1`／`record@1` 定義的一部分 |
| 單句超過 300 token（實驗 5-2g：IoC 表格被當成一句，840 token） | 在 token 起點上硬切成 ≤300 token 的片段，再打包 |
| 打包後某段仍超過上限（header 太長、斷詞邊界效應） | 該段再硬切。還超過就回 `ChunkerError::Invariant`，不寫入 |

硬切只切在 token 起點，不會把一個 token 切成兩半。

**實測的坑**：Metaspace 的前綴 `▁` 與第一個字共用同一個 offset，token 起點清單會比 token 數少一個。
直接取「第 N 個 token 起點」截出的前綴，重新斷詞會多 1 個 token（「很長的標題」×100 截 64 實算 65）。
所以 header 截斷改成往回退到實算 ≤ 上限為止，硬切的步長也用 `budget − 1`。

## header（D12 chunk header）

`header` 是文件標題（最多 64 token），向量化時放在段落前面：`embedding_text(header, text)`＝`"{header}\n{text}"`。
**M2 必須用這支函式組輸入**，否則 `token_count` 與 512 保護對不上。

已經從頭包含整個標題的段落（seq 0 通常是）不加 header，避免標題重複兩次。

`token_count`＝`embedding_text` 的 token 數，不含前綴與特殊 token。

## 偏移不變式

- `start_char`／`end_char` 是 `chunk_source_text(doc, None)` 的**字元**偏移。
- `text` 是那段字元原樣，不 trim。
- 所有非空白字元至少落在一個段落裡，內容不會被丟。
- 只含空白的段落會被丟掉；沒有任何文字的文件得到 0 段。

`tests/chunking.rs` 的 `assert_invariants` 逐段驗以上幾點，再加 `token_count` 實算一致與模型輸入 ≤ 512。

## 斷句（`sentence.rs`）

- 全形 `。！？；`、半形 `!?;`、`…` 遇到就斷，連續標點視為一個句尾。
- 半形 `.` 要後面接空白或在文末才斷，所以 `3.14`、`example.com`、`v1.2.3` 不會斷。
- 換行一律斷。
- 句尾後緊接的收尾引號、括號併入前一句：`他說：「已修補。」` 是一句。
- 開括號、開引號內不斷句；括號深度遇到換行歸零，避免一個沒關的括號吃掉整篇。
- 輸出的區間連續、不重疊、覆蓋全文，空白歸給前一句。

## 設定（`ChunkerConfig`）

| 欄位 | 預設 | 檢查 |
|---|---|---|
| `target_tokens` | 300 | > 0 |
| `overlap_tokens` | 100 | < `target_tokens`，否則不前進 |
| `max_model_tokens` | 512 | `target + max_header + 16 ≤ max_model`，否則幾乎每段都要二次硬切 |
| `max_header_tokens` | 64 | |
| `max_source_bytes` | 2 MiB | 串接文字超過就截在字元邊界，`ChunkOutput::truncated = true` |

## 版本

段落 id 由「策略＋版本」推導。以下任何一項改了，**所有策略版本都要升**，舊段落會被整批取代：

- tokenizer 檔
- 斷句規則
- header 規則
- 打包參數預設值
- 策略選擇規則

## 測試

```bash
scripts/fetch-e5-tokenizer.sh
cargo test -p chunker                     # 7 個斷句單元測試 + 12 個用真 tokenizer 的整合測試
```

找不到 tokenizer 檔時整合測試**失敗**，不會跳過。跳過的測試在 CI 上看起來是綠的，等於沒驗。
CI 的 integration job 會先跑 `fetch-e5-tokenizer.sh`。

語料驗收（手動，語料不進 repo）：

```bash
docker exec osint-core-postgres-1 psql -U osint -d osint_core -At -c \
  "select json_build_object('id',id,'object_type',object_type,'title',title,'summary',summary,'body',body,'attributes',attributes) \
   from documents where duplicate_of is null" > /path/to/docs.jsonl
CHUNKER_CORPUS=/path/to/docs.jsonl cargo test -p chunker --release --test chunking corpus_invariants -- --ignored --nocapture
```

### 2026-11-18 開發 DB 實測（6,098 份非重複文件，全部是 JSON 匯入）

| 項目 | 數值 |
|---|---|
| 段落數 | 16,833（平均每份 2.76 段，單份最多 91 段） |
| 策略 | 全部 `record`（開發 DB 沒有 RSS／網頁來源的文件） |
| 超過上限改走 300／100 | 1,387 份（22.7%） |
| 單句硬切 | 12 次 |
| 串接文字被截 | 0 份 |
| `token_count` | p50 274、p90 304、p99 415、最大 508 |
| 不變式 | 6,098／6,098 通過：偏移逐字相符、0 段超過 512、沒有字元遺失 |
| 耗時 | 25.1 s，單執行緒 release，約 243 份／秒、670 段／秒 |
| 記憶體 | 最大 RSS 約 303 MB（含 17 MB tokenizer 與測試行程） |

`token_count` 最大值 508 > 300，有兩種來源：`record` 整筆沒超過 512 就不切；硬切片段的上限是扣掉 header 後的 512 預算，不是 300。

**限制**：這批語料沒有 `whole` 與 `fixed` 策略的實際文件，這兩種只有合成資料的測試。M1-3 接上 RSS 來源後要再量一次。
