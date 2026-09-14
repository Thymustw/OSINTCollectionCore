# Embedding（OpenSearch ml-commons）

V0.2 的 semantic search（SPEC_V0.2 §11–§14）需要一條「文字 → 向量」的路。
本文件記錄 **2026-09-12 在本工作站實測**的結果：用 OpenSearch 內建的
ml-commons plugin 直接跑 embedding 模型是可行的，以及可行的代價是什麼。

**目前是雙模型架構**（Phase 0a-2 決定）：什麼語言就用什麼模型。

| 用途 | 模型 | 設定腳本 |
|---|---|---|
| 英文 | `all-MiniLM-L6-v2`（ONNX，384 維） | `scripts/opensearch-ml-setup.sh` |
| 中文／多語 | `multilingual-e5-small`（ONNX **int8**，384 維） | `scripts/opensearch-ml-setup-e5.sh` |

兩支腳本都是冪等的，可重複執行。兩個模型維度相同（384），
**但不能共用同一個 k-NN 欄位**：向量空間不相通，混在一起做最近鄰
會得到無意義鄰居而且不會報錯。`osint-documents` 因此分
`embedding_en`（MiniLM，`space_type: l2`）與
`embedding_multi`（e5，`space_type: cosinesimil`）兩個欄位。

---

## 1. 結論

| 項目 | 結果 |
|---|---|
| 模型能否載入 OpenSearch 2.19.6 | ✅ 可以（含非官方清單的 custom model） |
| 能否推論出 384 維向量 | ✅ 兩個模型都可以 |
| 英文語意 | ✅ 兩個模型都可用 |
| **中文語意** | ✅ e5-small 可用；MiniLM **不可用**，見 §5 |
| 雙模型並存 | ✅ 需要容器 3 GB + heap 1536m，見 §4 |
| 2 GB 容器放第二個模型 | ❌ kernel **OOM-kill**，見 §4.2 |
| e5-small 的 FP32 版 | ❌ 3 GB 也 OOM-kill，必須用 int8，見 §4.3 |
| 需要的磁碟 | OpenSearch volume 共 **~968 MB**（兩個模型） |
| 外部下載 | MiniLM 約 600 MB + e5 約 135 MB，見 §3 |

### Phase 0a-2 的決定

0a 的結論是「MiniLM 的中文區隔度只有英文的 1/17」。0c 實測證實那不只是
「區隔度低」而已——在小型檢索測試裡 **MiniLM 的中文 top-1 只有 2/5**，
它會把「這個週末天氣如何」配到談釣魚郵件的段落。中文實質上不可用。

換上 `multilingual-e5-small` 之後中文 top-1 **5/5**，英文維持 5/5
（沒有退化）。細節見 §5。

---

## 2. 模型

### 2.1 多語模型 `multilingual-e5-small`（中文用這個）

| 欄位 | 值 |
|---|---|
| ml-commons 裡的名稱 | `intfloat/multilingual-e5-small-int8` |
| 上游 repo | `intfloat/multilingual-e5-small`（HuggingFace） |
| 格式 | ONNX，**int8 量化**（`onnx/model_qint8_avx512_vnni.onnx`） |
| 維度 | 384（與 MiniLM 相同，可共用 index mapping） |
| 池化 | MEAN、`normalize_result: true`（依模型卡與 `1_Pooling/config.json`） |
| 底層架構 | BertModel，12 層、hidden 384、`max_position_embeddings` 512 |
| tokenizer | `XLMRobertaTokenizer`，vocab **250,037** |
| ONNX 檔大小 | 118,346,824 B（112.9 MiB） |
| ONNX SHA-256 | `dd476dd0c2514e9b9be83aeb3853fac0763e0bdf4a71645407587d77c48a2d88` |
| tokenizer.json SHA-256 | `0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39` |
| **打包後 zip** SHA-256 | `e8f6bd1be427a518c2160f1742fd3b70a1a1e0c01b4a93edf98671c8128c9ff6`（87,640,324 B） |
| 授權 | **MIT** |

ONNX 與 tokenizer 的 SHA-256 就是 HuggingFace 自己宣告的 LFS `oid`，可對照：

```bash
curl -s https://huggingface.co/api/models/intfloat/multilingual-e5-small/tree/main/onnx
```

腳本下載後會逐一比對這兩顆 hash，不符就中止。

#### 為什麼是 custom model 而不是 pretrained

ml-commons 2.19 的官方 pretrained 清單裡**沒有** e5。清單裡唯二的多語 dense
模型中，`paraphrase-multilingual-MiniLM-L12-v2` 的 MIRACL zh 只有 21.35，
不夠好。e5-small 的公開評測：繁中 BelebeleRetrieval 90.91、MIRACL zh 45.95、
C-MTEB 55.38。

代價是註冊流程完全不同——要自己備 ONNX + tokenizer、打包 zip、算 SHA-256、
開一個 OpenSearch 容器連得到的 HTTP 服務。見 §7.2。

#### zip 的打包方式必須可重現

冪等檢查靠的是 `model_content_hash_value`，也就是 **zip 的 SHA-256**。
如果打包時讓 `zipfile` 用當下時間當 entry 的 `date_time`，每次打包出來的
位元組都不同，hash 就不穩定，冪等檢查會永遠命中不了，於是每次執行都重新
註冊一次——而且完全不會報錯。

`scripts/opensearch-ml-setup-e5.sh` 的 `build_zip()` 因此固定
`date_time=(1980,1,1,0,0,0)`、固定 `ZIP_DEFLATED`。已實測：刪掉快取重新
下載並重新打包，SHA-256 仍是上表那顆。

⚠️ zip 內的 ONNX entry **必須叫 `model.onnx`**（DJL 靠副檔名找模型），
所以 int8 檔在 zip 裡也是這個名字。

### 2.2 英文模型 `all-MiniLM-L6-v2`

| 欄位 | 值 |
|---|---|
| 名稱 | `huggingface/sentence-transformers/all-MiniLM-L6-v2` |
| 上游版本 | `1.0.2` |
| 格式 | ONNX |
| 維度 | 384 |
| 池化 | MEAN，`normalize_result: true` |
| 底層架構 | BERT，6 層、hidden 384、`max_position_embeddings` 512 |
| 模型檔大小 | 91,719,191 B（87.5 MiB） |
| SHA-256 | `89b6737ca1745a89eafdf37cc7de2a3aae05aca9aac32ac50a3349add67206bb` |
| 授權 | Apache-2.0（模型權重，sentence-transformers 上游） |
| 來源 | `https://artifacts.opensearch.org/models/ml-models/huggingface/sentence-transformers/all-MiniLM-L6-v2/1.0.2/onnx/` |

`space_type` 上游宣告為 `l2`。Phase 3 Step 2 的 `embedding_en` 欄位
已照這個宣告使用 `l2`（見下方「已由 Phase 3 Step 2 接住」）。

### 上游版本號沒有被存下來——冪等檢查不能靠它

註冊 `1.0.2` 之後，`.plugins-ml-model` 裡那筆文件的 `model_version` 欄位是
**`"1"`**，那是 ml-commons 在 model group 內自己的遞增序號，和上游的 `1.0.2`
沒有關係；上游版本號**沒有被記在文件的任何欄位**。

所以 `{"term": {"model_version": "1.0.2"}}` 查出來穩定是 0 筆。拿它做冪等
判斷的話，腳本會在模型明明已經部署好的情況下每次都重新註冊一次，重抓
600 MB，而且**完全不會報錯**——只是「找不到」。

可靠的鍵是 `model_content_hash_value`：它就是上游 `config.json` 宣告的那顆
SHA-256，換版本一定會變。`scripts/opensearch-ml-setup.sh` 用的是這個。
換 `ML_MODEL_VERSION` 時，腳本裡的 `ML_MODEL_SHA256` 必須一起換。

另外查詢一定要 `must_not: [{"exists": {"field": "chunk_number"}}]`：
模型本體是以 10 個分塊文件存在同一個 index，不濾掉會抓到 chunk 而不是模型。

---

## 3. 外部下載：有兩份，不是一份

這是本次驗證最反直覺的一點。

| 來源 | 內容 | 大小 | 時機 |
|---|---|---|---|
| `artifacts.opensearch.org` | MiniLM 模型本體（ONNX） | 91.7 MB | register |
| `publish.djl.ai` | **PyTorch 1.13.1 CPU native libs** | 507 MB | deploy |
| `huggingface.co` | e5-small 的 ONNX int8 + tokenizer | 118.3 + 17.1 MB | 由設定腳本下載到宿主 |

e5 那一份和前兩份不同：**是設定腳本在宿主下載的，不是 OpenSearch 容器自己抓**
（容器只會去抓腳本開的那個臨時 HTTP 服務）。預設快取在 `/tmp/osint-e5-model`，
可用 `E5_CACHE_DIR` 換位置。離線環境把那三個檔案（`model.onnx`、
`tokenizer.json`、打包好的 zip）預先放進該目錄即可，腳本會驗 SHA-256 後跳過下載。

`publish.djl.ai` 的 507 MB 只會抓一次，兩個模型共用同一份 `ml_cache/pytorch/`。

第二份是預期外的：模型格式明明是 ONNX，DJL 仍然會把 PyTorch engine 的原生
函式庫（`libtorch.so`、`libtorch_cpu.so`、`libc10.so`、`libgomp`、`libstdc++`、
`libdjl_torch.so`）整組拉下來。

**這件事不會出現在任何 API 回應裡**，只在容器 log：

```
[a.d.p.j.LibUtils] Override PyTorch version: 1.13.1.
[a.d.p.j.LibUtils] Downloading https://publish.djl.ai/pytorch/1.13.1/cpu-precxx11/linux-x86_64/native/lib/libtorch_cpu.so.gz ...
[a.d.p.j.LibUtils] Downloading jni https://publish.djl.ai/pytorch/1.13.1/jnilib/0.31.1/linux-x86_64/cpu-precxx11/libdjl_torch.so to cache ...
```

後果：

- **封閉網路／離線環境**要先把 `ml_cache/pytorch/` 預先灌進 volume，
  否則 deploy 會卡在下載而不是失敗（狀態停在 `RUNNING`）。
- **CI** 若要跑 embedding 整合測試，每個乾淨 runner 都要付這 600 MB。
  V0.2 規劃整合測試時要先決定是快取還是跳過。
- 這也解釋了為什麼 volume 成長遠大於模型大小（§6）。

同時 log 會有一條可以忽略的 WARN：

```
[a.d.o.e.OrtEngine] CUDA is not supported OnnxRuntime engine: ... libcublasLt.so.11: cannot open shared object file
```

OpenSearch 容器裡沒有 CUDA，ONNX Runtime 退回 CPU。這是預期行為，不是故障。

---

## 4. 記憶體：1 GB 容器一定失敗，而且不會有人發現

### 實測（失敗案例，`mem_limit: 1g` + `-Xmx512m`）

register **成功**（模型檔寫進 volume），deploy **必定失敗**：

```json
{"state":"FAILED",
 "error":"{\"z2Vdy0rOTaetv-k4UyzqDA\":\"Memory Circuit Breaker is open, please check your resources!\"}"}
```

當下的數字：

| 指標 | 值 |
|---|---|
| 容器記憶體 | 897 MiB / 1 GiB（87.6 %） |
| `os.mem.free_in_bytes`（容器視角） | 65 MB |
| `ml_jvm_heap_usage` | **89 %** |
| `plugins.ml_commons.jvm_heap_memory_threshold`（預設） | 85 |
| `docker inspect .State.OOMKilled` | **false** |

**擋下來的是 JVM heap 那條斷路器**（89 > 85），不是 native memory，也不是
OOM-kill。所以症狀是：容器照常 running、healthcheck 全綠、叢集 green，
只有模型永遠 deploy 不起來。這是典型的靜默失效——監控不會響。

⚠️ 因此**只加 `mem_limit` 不加 heap 是修不好的**。也不要靠調高
`jvm_heap_memory_threshold` 繞過：那只是把「模型起不來」換成「整個叢集
在 GC 壓力下不穩」，而且 ONNX 模型真正要的是 heap **以外**的 native memory。

### 實測（成功設定，`mem_limit: 2g` + `-Xmx1g`）

| 階段 | 容器記憶體 | `ml_jvm_heap_usage` |
|---|---|---|
| 啟動後閒置 | 1.407 GiB / 2 GiB | 37 % |
| deploy 進行中 | 1.50 → 1.58 GiB | — |
| deploy 完成 | 1.822 GiB / 2 GiB（91 %） | 58 % |
| 推論數次後 | 1.831 GiB / 2 GiB | 28 % |

deploy 完成當下 JVM heap 用 595 MB / 1024 MB、non-heap 187 MB。

⚠️ **`os.mem.used_percent` 在部署後是 100 %**（容器視角，OpenSearch 讀的是
cgroup limit 不是宿主的 31 GB）。已部署的模型繼續推論沒問題（實測 13 次推論、
`ml_failure_count: 0`、`ml_circuit_breaker_trigger_count: 0`），
但這個設定**放不下第二個模型**——見 §4.2。

---

## 4.1 `native_memory_threshold` 是個空設定（重要）

0a 時這份文件寫過「把 `plugins.ml_commons.native_memory_threshold` 設成 95
可以避免第二個模型被擋」。**那個結論是錯的，已更正。**

`plugins.ml_commons.native_memory_threshold` 在 ml-commons 2.19 的原始碼裡
**已經被 comment out**（上游 PR #1015）。它的行為是：

- `PUT /_cluster/settings` 會回 `acknowledged: true`
- `GET /_cluster/settings` 讀得回你設的值
- **但沒有任何程式碼在讀它**

也就是說設了完全不生效。這是最難查的一種失效——API 全程不報錯。

2.19 實際唯一生效的門檻是 **`jvm_heap_memory_threshold`（預設 85）**。

後果很實際：**native memory 沒有任何斷路器保護**。ONNX 模型要的記憶體
絕大部分在 heap 之外，吃爆的表現不是「被斷路器溫和擋下」，而是
**kernel 直接 OOM-kill 整個 OpenSearch 行程**（§4.2）。

`scripts/opensearch-ml-setup.sh` 已經移除這個設定，只留 `only_run_on_ml_node`。

## 4.2 兩個模型並存：2 GB 會被 kernel OOM-kill

實測步驟：先 undeploy MiniLM 騰出空間，註冊 e5-small，deploy。

`mem_limit: 2g` + `-Xmx1g`，第二個模型 deploy 到一半，整個 OpenSearch 沒了：

```
Sep 12 18:08:59 kernel: Memory cgroup out of memory: Killed process 166663 (java)
  total-vm:8530280kB, anon-rss:2080956kB, file-rss:164352kB ...
  oom_memcg=/system.slice/docker-ce80057a....scope
```

`anon-rss: 2,080,956 kB` ≈ 剛好是 2 GiB 上限。

### ⚠️ 三個會騙人的徵兆

1. **`docker inspect --format '{{.State.OOMKilled}}'` 回報 `false`**——
   即使 kernel journal 明白寫著 `Killed process ... (java)`。
   **不要用 `.State.OOMKilled` 判斷有沒有被 OOM-kill。**
   可靠的證據是 `docker inspect --format '{{.RestartCount}}'`
   與 `journalctl -k | grep 'Memory cgroup out of memory'`。

2. **ml-commons 的 task 文件永遠停在 `RUNNING`**。因為節點是被直接殺掉的，
   沒有人回來把 task 標成 FAILED。輪詢 task 的腳本會一直等到逾時。
   同一時間 `GET /_plugins/_ml/models/<id>` 的 `model_state` 已經是
   `DEPLOY_FAILED`——**兩個 API 互相矛盾，要以 model_state 為準**。

3. 容器重啟後 `ml_deployed_model_count` 歸 0，**原本部署好的模型也一起掉了**。
   auto-redeploy 會在 log 留下 `Failed to query need auto redeploy models`。

### 各種組合的實測結果

| 容器 | heap | 模型組合 | 結果 |
|---|---|---|---|
| 2 GB | 1 GB | MiniLM 單獨 | ✅ 可用（0a 的設定） |
| 2 GB | 1 GB | e5 FP32 單獨 | ❌ **OOM-kill**（anon-rss 2,080,956 kB） |
| 2 GB | 1 GB | e5 int8 單獨 | ✅ 1.891 GiB / 2 GiB（94.5 %，餘裕很薄） |
| 2 GB | 1 GB | e5 int8 + MiniLM | ❌ **OOM-kill** |
| 3 GB | 1 GB | e5 FP32 單獨 | ✅ 2.525 GiB / 3 GiB |
| 3 GB | 1 GB | e5 FP32 + MiniLM | ⚠️ 兩個都 DEPLOYED（2.685 GiB），但推論被 429，見下 |
| 3 GB | 1 GB | e5 int8 + MiniLM | ⚠️ 同上，2.004 GiB / 3 GiB 但推論被 429 |
| 3 GB | **1536m** | **e5 int8 + MiniLM** | ✅ **採用**：2.55 GiB / 3 GiB、heap 76 % |
| 3 GB | 1536m | e5 FP32 + MiniLM | ❌ **OOM-kill**（anon-rss 3,128,400 kB） |

### 容器記憶體很閒，但推論全部被 429

這是最容易誤診的一格。`3 GB + -Xmx1g` 兩個模型都 deploy 成功，容器只用
2.0 GiB / 3 GiB（67 %）——看起來很健康。但每一次推論都回：

```json
{"error":{"type":"circuit_breaking_exception",
 "reason":"Memory Circuit Breaker is open, please check your resources!",
 "durability":"TRANSIENT"},"status":429}
```

原因是 `ml_jvm_heap_usage` 穩定停在 **86–92 %**，超過
`jvm_heap_memory_threshold`（85）。實測連續量 60 秒，heap 從 88 % 一路爬到
92 %，**GC 並沒有把它降回來**，所以這不是暫時性的垃圾堆積，而是穩態。

⚠️ **瓶頸是 heap，不是容器記憶體。看 `docker stats` 會得到完全相反的結論。**
把 `mem_limit` 再往上加不會有任何幫助。

改成 `-Xmx1536m` 之後 heap 落在 76 %（1170 / 1536 MB），
`ml_circuit_breaker_trigger_count: 0`，16 次推論 0 失敗。

### ⚠️ 3 GB 的餘裕比穩態數字看起來的薄

採用設定的容器用量實測有明顯區間，**不要只記住最小的那個數字**：

| 情境 | 容器記憶體 |
|---|---|
| 剛部署完 | 2.55 GiB / 3 GiB（85 %） |
| 重啟 + auto-redeploy 後穩態 | 2.61 GiB / 3 GiB（87 %） |
| 經過大量 deploy／undeploy／forcemerge 之後 | **2.85 GiB / 3 GiB（94.9 %）** |

也就是說 3 GB 對「兩個模型」是**夠用但不寬裕**。JVM 的 RSS 在 undeploy 之後
**不會還給作業系統**，所以反覆換模型會讓用量單向往上爬，直到下次重啟。

實務建議：**換模型後重啟一次 OpenSearch 容器**再看數字，不要拿長時間跑下來
的用量當基準。而且這個餘裕不足以再塞第三個模型（§8）。

### 為什麼用 int8 而不是 FP32

不是為了省磁碟——是 FP32 在 3 GB 容器 **也會 OOM-kill**（上表最後一列）。
e5-small 的 FP32 ONNX 有 470 MB，因為它的 vocab 是 250,037
（XLM-R 的多語詞表），光 embedding 表就吃掉 250037 × 384 × 4 B ≈ 384 MB。

而且 int8 **沒有付出品質代價**（§5.3）：中英文檢索 top-1 都是 5/5，
與 FP32 相同，延遲還快了一倍。

### 叢集設定

兩支腳本合計會設這些 persistent 設定（重啟後保留，已實測）：

| 設定 | 預設 | 設成 | 為什麼 |
|---|---|---|---|
| `plugins.ml_commons.only_run_on_ml_node` | `true` | `false` | 單節點開發叢集沒有專用 ml node，不關掉 deploy 找不到可派工的節點 |
| `plugins.ml_commons.allow_registering_model_via_url` | `false` | `true` | custom model 只能從 URL 註冊，不開的話 e5 一定註冊不了 |

`plugins.ml_commons.max_model_on_node` 預設 **10**，兩個模型遠低於上限，
不需要調整——擋住我們的從來不是這個數字，是實體記憶體。

---

## 5. 語意品質：這是英文模型，中文區隔度很低

同一組句型，中英文各跑一次（`normalize_result: true`，所以 cosine 等於內積）：

| 語言 | cos(同主題) | cos(不相關) | 差距 |
|---|---|---|---|
| 英文 | 0.816086 | 0.026061 | **0.790025** |
| 中文 | 0.290911 | 0.244016 | **0.046895** |

- 英文：`"Microsoft is a technology company"` / `"Microsoft is a major software vendor"` / `"The weather is nice today"`
- 中文：`"Microsoft 是一家科技公司"` / `"微軟是軟體大廠"` / `"今天天氣很好"`

中文的斷言（相近 > 不相關）**成立**，但差距只有英文的 1/17。

`all-MiniLM-L6-v2` 是在英文語料上訓練的，中文字多半落進 BERT 的
byte/UNK 處理，向量帶不出語意。0.29 vs 0.24 這種差距在真實語料裡會被
文件長度、雜訊淹沒——**不足以支撐中文的 semantic search**。

不要用中文句子當 MiniLM 的 CI 斷言：0.047 的差距是脆弱的測試，模型微幅變動
就會翻盤。`scripts/opensearch-ml-setup.sh` 的冒煙測試因此用英文。

## 5.1 e5-small 的同一組句子

| 語言 | 前綴 | cos(同主題) | cos(不相關) | 差距 |
|---|---|---|---|---|
| 中文 | 無 | 0.918066 | 0.829833 | 0.088234 |
| 中文 | `query:` | 0.926859 | 0.835138 | **0.091721** |
| 中文 | `passage:` | 0.933088 | 0.850977 | 0.082111 |
| 英文 | 無 | 0.931452 | 0.806012 | 0.125440 |
| 英文 | `query:` | 0.924330 | 0.766453 | **0.157877** |
| 英文 | `passage:` | 0.934359 | 0.786269 | 0.148091 |

⚠️ **不要把這裡的 0.092 跟 MiniLM 英文的 0.790 直接相比。**
兩個模型的向量各向異性差很多：e5 把所有句子都壓在一個窄錐裡，連完全
不相關的句子 cosine 都有 0.83，所以**絕對差距天生就小**。
跨模型比較絕對 cosine 值是沒有意義的。

有意義的是同一個模型內的中英對比：

| 模型 | 中文差距 ÷ 英文差距 |
|---|---|
| MiniLM | 0.047 / 0.790 = **6 %** |
| e5-small | 0.092 / 0.158 = **58 %** |

## 5.2 小型檢索測試（真正的決策依據）

三句話的 cosine 差距證據力太弱。這裡用 5 個 query × 5 個 passage
（主題：公司介紹／釣魚郵件防護／週末天氣／醫院勒索軟體／半導體製程），
每個 query 的正解是同索引的 passage，量 top-1 命中與「正解 − 最佳錯誤答案」
的邊際：

| 模型 | 語言 | top-1 | 平均邊際 | 最小邊際 |
|---|---|---|---|---|
| MiniLM | 中文 | **2 / 5** ❌ | +0.008981 | **−0.196647** |
| MiniLM | 英文 | 5 / 5 | +0.507789 | +0.248829 |
| e5-small（無前綴） | 中文 | **5 / 5** ✅ | +0.058062 | +0.028766 |
| e5-small（無前綴） | 英文 | 5 / 5 | +0.083435 | +0.031508 |
| e5-small（`query:`/`passage:`） | 中文 | **5 / 5** ✅ | +0.061034 | +0.037504 |
| e5-small（`query:`/`passage:`） | 英文 | 5 / 5 | +0.090889 | +0.043103 |

MiniLM 在中文不只是「區隔度低」——它的最小邊際是**負的 0.197**，
也就是錯誤答案贏正解贏很多。實際錯法：「這個週末天氣如何」被配到
談釣魚郵件的段落、「台積電的先進製程」也被配到同一段。
**中文檢索實質不可用，換模型是必要的，不是優化。**

e5-small 中英文都 5/5，**英文沒有因為換多語模型而退化**。

## 5.3 `query:` / `passage:` 前綴要不要加

e5 系列的模型卡明講查詢要加 `query: `、文件要加 `passage: `。實測（int8）：

| 情境 | 中文平均邊際 | 中文最小邊際 |
|---|---|---|
| 都不加前綴 | +0.058062 | +0.028766 |
| 加 `query:` / `passage:` | +0.061034 | **+0.037504（+30 %）** |

**結論：要加。** 兩者 top-1 都是 5/5，但加了之後最小邊際改善約 30 %，
方向一致（中英文皆然）。最小邊際才是會不會排錯的關鍵。

⚠️ 規則是**非對稱**的：查詢加 `query: `、被檢索的文件加 `passage: `。
實測若三句都加同一種前綴（全 `query:` 或全 `passage:`），效果不如非對稱用法。
這件事必須在 Core 產 embedding 時就決定——同一段文字加不同前綴會得到
不同向量，**索引時用 `passage:`、查詢時用 `query:`，兩邊不一致就會靜默地
降低召回率**，而且不會有任何錯誤訊息。

### int8 量化有沒有掉分

| 版本 | 中文 top-1 | 中文平均邊際 | 英文 top-1 |
|---|---|---|---|
| FP32 | 5 / 5 | +0.062407 | 5 / 5 |
| int8 | 5 / 5 | +0.061034 | 5 / 5 |

差 0.0014，在這個規模的測試裡沒有意義。**int8 沒有可觀測的品質代價**，
但省下 352 MB 記憶體並讓延遲快一倍（§6）。

⚠️ 這是 5 題的小樣本，只夠支撐「int8 沒有明顯退化」與「MiniLM 中文不可用」
這兩個結論。**不足以**當成 e5-small 在真實 OSINT 語料上的召回率估計。
要那個數字得用真實語料另外評測。

---

## 6. 延遲與磁碟

### 推論延遲

單筆短文字、`POST /_plugins/_ml/_predict/text_embedding/<id>`，
含 HTTP 往返、本機 loopback、容器限 1.0 CPU：

| 模型 | 1 筆 × 10 次 | 8 筆一次 |
|---|---|---|
| MiniLM | min 66.8 ms / median ~96–175 ms / max 199 ms | 608 ms（約 76 ms/筆） |
| e5-small **FP32** | min 97.9 ms / median 193.6 ms / max 297.4 ms | 796.6 ms（約 99.6 ms/筆） |
| e5-small **int8** | min 7.1 ms / median 96.3 ms / max 101.3 ms | 309.4 ms（約 **38.7 ms/筆**） |

int8 對 FP32 是約 **2 倍**的加速（median 96.3 vs 193.6 ms、
批次每筆 38.7 vs 99.6 ms），而且品質沒有可觀測的差異（§5.3）。

注意 MiniLM 的 median 在兩輪量測分別是 174.8 ms 與 96.4 ms——**變異很大**，
因為容器只有 1.0 CPU 且與宿主上的其他堆疊競爭。規劃 V0.2 的 embedding
吞吐時，不要用單次量測當基準；批次化明顯划算（8 筆批次的每筆成本約為
單筆的一半以下）。

### 磁碟

OpenSearch volume（`osint-core_opensearch-data`）：

| 時點 | 大小 |
|---|---|
| 基準（無模型） | 1,894,550 B（1.8 MiB） |
| 只有 MiniLM | 761,294,606 B（726.8 MiB） |
| **MiniLM + e5-small int8** | **1,014,567,876 B（967.6 MiB）** |
| e5-small **int8** 的淨增 | 約 241 MiB |

拆解（兩個模型都在時）：

| 路徑 | 大小 | 說明 |
|---|---|---|
| `ml_cache/pytorch/` | 507.5 MiB | DJL 抓的 PyTorch native libs（§3），**兩個模型共用一份** |
| `.plugins-ml-model` index | 231.1 MiB | 兩個模型的 base64 分塊 |
| `ml_cache/models_cache/` | 216.7 MiB | 解出來的模型本體（87.5 + 112.9） |
| `ml_cache/tokenizers/` | 13.3 MiB | tokenizer |

宿主端另外還有 e5 的下載快取（預設 `/tmp/osint-e5-model`，約
**222 MiB**：ONNX 112.9 + tokenizer 16.3 + zip 83.6）。那是 `/tmp`，
重開機會清掉，腳本會重抓。要固定保留就設 `E5_CACHE_DIR`。

⚠️ 量 volume 大小前一定要先 forcemerge。刪掉模型後量到的數字會虛高
（實測刪掉 e5 FP32 後仍顯示 1.30 GiB，forcemerge 後才降到 967.6 MiB）——
多出來的是尚未 merge 的舊 segment，不是新增內容：

```bash
curl -X POST 'http://127.0.0.1:19200/.plugins-ml-model/_forcemerge?only_expunge_deletes=true'
```

CLAUDE.md §15 的磁碟紀律：這 ~968 MB 是**常駐**的，`make disk` 會看到
`osint-core_opensearch-data` 從 2 MB 跳到 ~970 MB。這是預期的，不是洩漏。

---

## 7. 怎麼跑

```bash
make compose-up                          # 需要已套用 3 GB / 1536m heap 的 dev override
bash scripts/opensearch-ml-setup.sh      # 英文 MiniLM，冪等
bash scripts/opensearch-ml-setup-e5.sh   # 多語 e5-small int8，冪等
```

兩支都印出各自的 `model_id`。**`model_id` 每次重新註冊都會變**，
不要寫死在程式碼或設定檔裡；要用 §2 的名稱 + SHA-256 去查。

| 腳本 | 已部署時 | 從零跑 |
|---|---|---|
| `opensearch-ml-setup.sh` | 0.18 s | 約 16 s（PyTorch libs 已快取） |
| `opensearch-ml-setup-e5.sh` | 0.43 s | 約 13 s（模型已在 `E5_CACHE_DIR`） |

兩支腳本開頭都會檢查對面 `version.distribution == "opensearch"` 才繼續。
這不是形式檢查：本機常見情境：9200 可能被其他本機服務占用（例如另一套安全/情資平台），打錯埠號就會對別人的叢集下 `_cluster/settings`。腳本開頭驗對面 `version.distribution == "opensearch"` 才繼續，已實測指向 Elasticsearch 叢集會被擋下。

### 7.1 環境變數

| 腳本 | 變數 |
|---|---|
| `opensearch-ml-setup.sh` | `OPENSEARCH_URL`、`ML_MODEL_NAME`、`ML_MODEL_VERSION`、`ML_MODEL_SHA256`、`ML_TIMEOUT_SECS` |
| `opensearch-ml-setup-e5.sh` | `OPENSEARCH_URL`、`E5_CACHE_DIR`、`E5_SERVE_ADDR`、`E5_SERVE_PORT`、`ML_TIMEOUT_SECS` |

### 7.2 e5 的註冊流程為什麼比較繞

e5 不在 ml-commons 的 pretrained 清單，只能走 custom model，而 custom model
**只接受 URL**，不能吃宿主路徑。所以腳本要：

1. 從 HuggingFace 下載 ONNX int8 + `tokenizer.json`，驗 SHA-256
2. 打包成可重現的 zip（§2.1），驗 SHA-256
3. 用 `python3 -m http.server` 開一個臨時服務，**綁 compose network 的
   gateway**（`docker network inspect osint-core-net`，實測是 `172.19.0.1`）
4. `PUT` 開啟 `plugins.ml_commons.allow_registering_model_via_url`（預設 false）
5. `_register` 帶 `url` → `_deploy` → 中文冒煙測試
6. `trap EXIT` 關掉臨時服務

⚠️ **位址不能用 `127.0.0.1`**——在容器裡那是容器自己，會抓不到。

⚠️ `plugins.ml_commons.trusted_url_regex` 預設是
`^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]`，
`http://` 的區網位址本來就通過，不需要調整。

### 7.3 踩過的 API 陷阱

- `_register` 的 body 裡**不要**放 `"model_group_id": null`。
  ml-commons 會回 HTTP 500 `Can't get text on a VALUE_NULL at 6:21`，
  訊息只給行列號、不講是哪個欄位。省略該欄位即可。
- deploy 回 `FAILED` 且 error 是 `Duplicate deploy model task` 是**良性的**：
  容器重啟後 auto-redeploy 已經在跑同一個模型。腳本會把它當成功處理，
  判斷依據是 `model_state` 而不是 task。

### 7.4 Rust adapter `MlCommonsEmbeddingProvider`

`crates/storage-opensearch/src/embedding.rs`。HTTP client 與
`OpenSearchStore` 同一套（`opensearch` crate，不另引 reqwest）。

- 查 `model_id`：`POST /_plugins/_ml/models/_search`，鍵是
  `model_content_hash_value`，`must_not chunk_number`。這是 plugin
  API，與 setup 腳本相同；不要打內部 index `/.plugins-ml-model`。
- 推論：`POST /_plugins/_ml/_predict/text_embedding/<model_id>`，
  body 必帶 `return_number: true` 與 `target_response: ["sentence_embedding"]`。
- `model_id` 在 `connect` 時查一次並快取。重新註冊後必須重啟。
- `model_version` 填內容雜湊，不是 ml-commons 的 `"1"`。
- 混合語言的 `embed_batch` 拆成 MiniLM／e5 兩次**串行** `_predict`，
  再依原始順序組回。
- HTTP 429／`circuit_breaking_exception` 對成 `StorageError::Timeout`
  （暫時性）。對應函式 `classify_ml_http` 有單元測試。

conformance：`cargo test -p storage-opensearch --test embedding_conformance`。
**只查詢與推論，不會 undeploy／delete 模型。**

---

## 8. 未決事項（V0.2 規劃時處理，本文件不決定）

### 已由 `EmbeddingProvider` trait 接住（V0.2 Phase 0g）

- **語言偵測放哪裡**：OpenSearch **沒有**內建語言偵測 processor。決定是
  **Core（Rust）偵測，結果放進 `EmbeddingRequest.language`**；
  `EmbeddingProvider` **不做偵測**，只做「這個語言用哪個模型」
  （`model_for`／`embed`）。`None` = 未知 → 走多語 e5，不是 MiniLM。
  ingest pipeline 的 `if` 條件若之後採用，仍需要 Core 先把語言欄位寫進文件。
- **Embedding 由誰產**：決定是 **Core 經 `EmbeddingProvider` 產**，回傳
  `EmbeddingVector { model, model_version, dimensions, content_hash, vector }`
  寫進 SPEC §11 的 Embedding record。`query:`／`passage:` 前綴由實作依
  `EmbeddingKind` 決定要不要加（MiniLM 不加、e5 加）；呼叫端不要自己拼。
  生產 adapter 是 `storage_opensearch::MlCommonsEmbeddingProvider`
  （V0.2 Phase 3）：`connect` 時用內容雜湊查兩個 `DEPLOYED` 模型的
  `model_id` 並快取到重啟；`embed`／`embed_batch` 打
  `POST /_plugins/_ml/_predict/text_embedding/<id>`，`return_number`
  必帶（否則回 base64）。混合語言批次會拆成 MiniLM／e5 兩次串行呼叫
  再組回原順序。走 ingest pipeline 自動產生仍是一條可能的實作路徑，但
  也必須從這個 trait 出去，不能讓 indexer 直接打 ml-commons。

### 已由 Phase 3 Step 2 接住（OpenSearch k-NN mapping）

- **兩個模型的向量分欄位，不共用**：`osint-documents` 宣告
  `embedding_en`（MiniLM）與 `embedding_multi`（e5-small），外加
  `embedding_en_model_version`／`embedding_multi_model_version`
  （keyword，存內容雜湊）。查詢／稽核不用回頭查 PostgreSQL 就能看出
  這份文件目前索引的向量是哪個模型版本算的——模型升級時用來判斷
  哪些文件的向量已過期。維度都是 384 但空間不相通，混一個欄位做
  k-NN 會得到無意義鄰居而且**不會報錯**。
- **k-NN `engine`／`space_type`**（2026-09-14 在 OpenSearch 2.19.6 實測）：
  | 欄位 | engine | space_type | 依據 |
  |---|---|---|---|
  | `embedding_en` | `lucene` | `l2` | MiniLM 上游宣告 l2（§2.2） |
  | `embedding_multi` | `lucene` | `cosinesimil` | e5 已 `normalize_result: true`，正規化向量下 cosine 與內積等價 |
  `PUT` mapping `method.engine=lucene` 回 200。官方映像
  `opensearchproject/opensearch:2.19.6` 內建 `opensearch-knn` 2.19.6.0；
  本機另起一個**未跑** `opensearch-ml-setup.sh` 的乾淨容器（埠 19210）
  同樣能建 knn index 並查出最近鄰。選 lucene 而不是 faiss／nmslib：
  純 Java、knn 子句內的 `filter` 原生可用、不需要額外 native library。
  本機／CI 都是單節點小索引，不需要近似搜尋的效能優勢。
- **`index.knn` 只能在建立 index 時開啟**。對既有 `osint-documents`
  這是破壞性 mapping 變更（再加上 `dynamic: strict` 不接受未宣告欄位），
  必須 `osint-indexer --rebuild --drop`。
- **filter 放 knn 子句內，不是外層 bool 的 post-filter。** 同日實測：
  lucene engine 下，最近鄰是 `kind=note`、filter `kind=report`、`k=1`
  時，knn 子句內 `filter` 仍回傳下一份符合的 report；外層
  `bool.must knn + filter` 回空——knn 先取 k 再過濾，最近鄰被濾掉就沒東西。
  `SearchStore::vector_search` 走 native filter。
- **部分更新走 `SearchStore::update_fields`**（OpenSearch `_update`，
  `doc_as_upsert=false`）。embedding-worker 事後補寫向量欄位時
  **不能**用 `index()`：index API 取代整個 `_source`，會把
  title／body／entities 清空。文件不存在回 `NotFound`，不憑空 upsert。

### 已由 Phase 3 Step 3 接住（embedding-worker）

- **觸發**：訂閱 `entity.extracted`（與 indexer 同一 topic、獨立 consumer group）。**不**訂 `embedding.requested`（SPEC §20 保留名、零生產者）。不發 `embedding.completed`。
- **兩個目標、兩個 index**：Document title／body 用 `update_fields` 疊加進既有 `osint-documents`；Entity description 用 `index()` 寫進獨立 `osint-entities`。永不對 documents 呼叫 `index()`。
- **Entity 語言永遠未知**：`language = None` → 多語 e5。V0.2 只寫 `description_vector_multi`。
- **跳過 merged／duplicate**：`merged_into`／`duplicate_of` 不寫向量。
- **`--rebuild --drop` 只刪 `osint-entities`**，永不刪 `osint-documents`。
- 服務文件：`docs/developer/embedding-worker.md`。

### 已由 Phase 3 Step 1 接住（schema／config，還沒有 worker）

- **PostgreSQL 存 embedding metadata**：`embeddings` 表（migration `0010`）
  只記「這個目標、這個模型、這個內容雜湊算過了」。向量本體仍不在這裡，
  等 Step 2 的 OpenSearch k-NN 投影。re-generate 走
  `RelationalStore::find_embedding`；`put_embedding` 撞 UNIQUE 回 Conflict，
  不是 upsert。
- **`[embedding]`／`[search_hybrid]` config**：batch／併發／cosine 門檻與
  hybrid RRF 權重。OpenSearch URL **不**另開一份，沿用 `[storage.search].url`。
  `similarity_threshold` 預設 0.90 是**暫定值**（e5-small 不相關文字也有
  ~0.83，見 §5），Step 6 要用真實 OSINT 語料重校。hybrid 的
  entity_match／recency／source_score／confidence 權重預設 0.0（Step 5 才接）。
- **Semantic Dedup 的 model 欄位**：`duplicate_groups.model`（migration `0011`）。
  Stage 1-4 是 `NULL`；Stage 5 才填模型名稱。

### 維持未決

- CI 是否跑 **ml-commons embedding** 整合測試（要付 600 MB + 135 MB 下載，§3）。
  k-NN 查詢本身**不**在這條未決裡：`opensearch-knn` 是官方映像內建 plugin，
  `storage-opensearch` 的 `vector_search` 整合測試沒有 `#[ignore]`。
- 3 GB 容器在兩個模型下已用到 85 %。**再加第三個模型（例如 reranker）
  必須先重估**，而且 native memory 沒有斷路器保護（§4.1），估錯的後果是
  整個 OpenSearch 被 OOM-kill，不是溫和地拒絕部署。
