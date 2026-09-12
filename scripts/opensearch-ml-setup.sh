#!/usr/bin/env bash
# opensearch-ml-setup.sh — 在 OpenSearch ml-commons 上備妥 V0.2 的 embedding 模型。
#
# SPEC_V0.2.md §11 Embedding／§14 Vector Search 的前提：Core 必須能取得
# 384 維向量。這支腳本把「叢集設定 → 註冊 → 部署 → 冒煙測試」整條路跑完，
# 並且**冪等**：模型已經在 DEPLOYED 狀態就直接跳過，不會重下載。
#
# 用法：
#   bash scripts/opensearch-ml-setup.sh            # 本機（預設 19200）
#   OPENSEARCH_URL=http://opensearch:9200 bash scripts/opensearch-ml-setup.sh
#
# 環境變數：
#   OPENSEARCH_URL   預設 http://127.0.0.1:19200（本工作站的 dev override 埠）
#   ML_MODEL_NAME    預設 huggingface/sentence-transformers/all-MiniLM-L6-v2
#   ML_MODEL_VERSION 預設 1.0.2
#   ML_TIMEOUT_SECS  單一非同步工作的輪詢上限，預設 900
#
# ---------------------------------------------------------------------------
# ⚠️ 這支腳本會讓 OpenSearch 容器往外抓 **兩份** 東西，合計約 600 MB
# ---------------------------------------------------------------------------
#   1. artifacts.opensearch.org  → 模型本體（ONNX，91.7 MB）
#   2. publish.djl.ai            → PyTorch 1.13.1 CPU native libs（507 MB）
#
# 第 2 份是反直覺的、而且**不會出現在任何 API 回應裡**（只在容器 log）：
# 模型格式明明是 ONNX，DJL 仍然會把 PyTorch engine 的原生函式庫拉下來。
# 離線／封閉網路環境要先把 ml_cache 預先灌進 volume，否則 deploy 會卡住。
# 完整實測數據與磁碟拆解見 docs/developer/embedding.md。
#
# ⚠️ 資源需求：OpenSearch 容器至少 2 GB、JVM heap 至少 1 GB。
#    低於此值 deploy 會失敗在「Memory Circuit Breaker is open」——
#    那是斷路器擋下來的，容器不會掛、健康檢查全綠，只有模型永遠起不來。

set -euo pipefail

OPENSEARCH_URL="${OPENSEARCH_URL:-http://127.0.0.1:19200}"
ML_MODEL_NAME="${ML_MODEL_NAME:-huggingface/sentence-transformers/all-MiniLM-L6-v2}"
ML_MODEL_VERSION="${ML_MODEL_VERSION:-1.0.2}"
ML_TIMEOUT_SECS="${ML_TIMEOUT_SECS:-900}"
# all-MiniLM-L6-v2 的輸出維度。寫死是刻意的：冒煙測試要斷言的就是這個數字，
# 從回應裡讀回來再拿去比對自己等於沒有斷言。
EXPECTED_DIM=384
# 上游 artifacts.opensearch.org 的 .../all-MiniLM-L6-v2/1.0.2/onnx/config.json
# 宣告的 model_content_hash_value。**這是冪等檢查唯一可靠的鍵**，理由見
# find_model() 的註解。換 ML_MODEL_VERSION 時這個值要一起換，
# 來源：curl -s https://artifacts.opensearch.org/models/ml-models/${ML_MODEL_NAME}/${ML_MODEL_VERSION}/onnx/config.json
ML_MODEL_SHA256="${ML_MODEL_SHA256:-89b6737ca1745a89eafdf37cc7de2a3aae05aca9aac32ac50a3349add67206bb}"

log()  { printf '== %s\n' "$*"; }
fail() { printf '❌ %s\n' "$*" >&2; exit 1; }

command -v curl    >/dev/null 2>&1 || fail "找不到 curl。"
command -v python3 >/dev/null 2>&1 || fail "找不到 python3（用來解析 JSON 回應）。"

api() {
  # api <METHOD> <PATH> [BODY]
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -sS -X "$method" "${OPENSEARCH_URL}${path}" \
      -H 'Content-Type: application/json' -d "$body"
  else
    curl -sS -X "$method" "${OPENSEARCH_URL}${path}"
  fi
}

jq_py() { python3 -c "$1"; }

# --------------------------------------------------------------------------
# 0. 確認對面真的是 OpenSearch，而不是別人的 Elasticsearch
# --------------------------------------------------------------------------
# 這不是形式檢查。本工作站的 9200 是 OpenCTI 的 Elasticsearch，打錯一個埠號
# 就會對別人的叢集下 _cluster/settings，而且前幾步都不會報錯。
# 與 storage-core 的 verify_not_opencti_search 是同一個理由（ADR-005）。
log "檢查 ${OPENSEARCH_URL} 的身分"
ROOT_JSON="$(api GET / )" || fail "連不上 ${OPENSEARCH_URL}。請先 make compose-up。"
DIST="$(jq_py "
import json,sys
d=json.load(sys.stdin)
print(d.get('version',{}).get('distribution',''))
" <<<"$ROOT_JSON")"
if [[ "$DIST" != "opensearch" ]]; then
  fail "${OPENSEARCH_URL} 不是 OpenSearch（version.distribution='${DIST}'）。
   這台工作站的 9200 是 OpenCTI 的 Elasticsearch，ml-commons 的 API 在那裡不存在。
   本專案的 OpenSearch 在 19200，請設 OPENSEARCH_URL=http://127.0.0.1:19200。"
fi
OS_VER="$(jq_py "import json,sys;print(json.load(sys.stdin)['version']['number'])" <<<"$ROOT_JSON")"
log "確認為 OpenSearch ${OS_VER}"

# --------------------------------------------------------------------------
# 1. 叢集設定
# --------------------------------------------------------------------------
# only_run_on_ml_node 預設 true：單節點開發叢集沒有專用 ml node，
# 不關掉的話 deploy 會找不到可派工的節點。
#
# native_memory_threshold 預設 90，指的是**容器看得到的系統記憶體**使用率
# （OpenSearch 讀 cgroup limit，不是宿主的 31 GB）。模型 deploy 之後這台
# 開發機量到 100%，維持 90 會讓「再部署第二個模型」被擋下來。
# 設 95 而不是 100：100 等於把斷路器關掉，真的吃光記憶體時會變成 OOM-kill。
log "設定 plugins.ml_commons.*"
api PUT /_cluster/settings '{
  "persistent": {
    "plugins.ml_commons.only_run_on_ml_node": false,
    "plugins.ml_commons.native_memory_threshold": 95
  }
}' | jq_py "
import json,sys
d=json.load(sys.stdin)
if not d.get('acknowledged'):
    sys.exit('叢集設定未被接受：%s' % json.dumps(d, ensure_ascii=False))
print('   acknowledged')
"

# --------------------------------------------------------------------------
# 2. 冪等檢查：這個 name+version 是不是已經部署好了
# --------------------------------------------------------------------------
find_model() {
  # 回傳 "<model_id> <model_state>"，找不到就回空字串。
  #
  # ⚠️ 冪等檢查**不能**用 model_version 比對 ML_MODEL_VERSION。
  # 2026-09-12 實測：註冊 1.0.2 之後，文件裡的 `model_version` 是 `"1"`——
  # 那是 ml-commons 自己在 model group 內的遞增序號，跟上游的 1.0.2 無關，
  # 而且上游版本號**沒有被記在文件的任何欄位**。
  # 用 `{"term":{"model_version":"1.0.2"}}` 查的結果是穩定的 0 筆，於是這支
  # 腳本會在模型明明已經部署好的情況下每次都重新註冊一次，重下載 600 MB。
  # 那是典型的靜默失效：沒有任何錯誤訊息，只是「找不到」。
  #
  # 可靠的鍵是 model_content_hash_value：它就是上游 config.json 宣告的
  # 那顆 SHA-256，改版本一定會變。
  #
  # must_not chunk_number 是必要的：模型本體以 10 個分塊文件存在同一個
  # index，不濾掉會抓到 chunk 而不是模型。
  api POST /_plugins/_ml/models/_search '{
    "size": 5,
    "_source": ["model_state", "model_content_hash_value", "model_config.embedding_dimension"],
    "query": {"bool": {
      "must": [
        {"term": {"name.keyword": "'"${ML_MODEL_NAME}"'"}},
        {"term": {"model_content_hash_value": "'"${ML_MODEL_SHA256}"'"}}
      ],
      "must_not": [{"exists": {"field": "chunk_number"}}]
    }}
  }' 2>/dev/null | jq_py "
import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    print(''); raise SystemExit
for h in d.get('hits',{}).get('hits',[]):
    print(h['_id'], h['_source'].get('model_state',''))
    break
"
}

EXISTING="$(find_model || true)"
MODEL_ID="$(awk '{print $1}' <<<"$EXISTING")"
MODEL_STATE="$(awk '{print $2}' <<<"$EXISTING")"

# --------------------------------------------------------------------------
# 3. 非同步工作輪詢
# --------------------------------------------------------------------------
wait_task() {
  local task_id="$1" what="$2" waited=0
  while :; do
    local r state
    r="$(api GET "/_plugins/_ml/tasks/${task_id}")"
    state="$(jq_py "import json,sys;print(json.load(sys.stdin).get('state',''))" <<<"$r")"
    case "$state" in
      COMPLETED)
        jq_py "import json,sys;print(json.load(sys.stdin).get('model_id',''))" <<<"$r"
        return 0
        ;;
      FAILED|COMPLETED_WITH_ERROR)
        local err
        err="$(jq_py "import json,sys;print(json.load(sys.stdin).get('error',''))" <<<"$r")"
        if [[ "$err" == *"Circuit Breaker"* ]]; then
          fail "${what} 失敗：${err}

   這是 ml-commons 的記憶體斷路器擋下來的，**不是容器被 OOM-kill**。
   容器仍然是 running、健康檢查仍然全綠，只有模型永遠起不來。
   下一步：把 OpenSearch 容器提高到至少 2 GB、JVM heap 至少 1 GB
   （docker/docker-compose.dev.yml 的 mem_limit 與 OPENSEARCH_JAVA_OPTS），
   然後重新執行這支腳本。詳細實測數據見 docs/developer/embedding.md。
   不要靠調高 jvm_heap_memory_threshold 繞過——那是把問題換成整個叢集不穩。"
        fi
        fail "${what} 失敗（state=${state}）：${err}"
        ;;
    esac
    if (( waited >= ML_TIMEOUT_SECS )); then
      fail "${what} 超過 ${ML_TIMEOUT_SECS} 秒仍是 ${state}。
   多半卡在外部下載（artifacts.opensearch.org 或 publish.djl.ai）。
   請看容器 log：docker logs osint-core-opensearch-1 | grep -i download"
    fi
    sleep 5
    waited=$(( waited + 5 ))
    printf '   %s… %ss（state=%s）\n' "$what" "$waited" "$state" >&2
  done
}

# --------------------------------------------------------------------------
# 4. 註冊
# --------------------------------------------------------------------------
if [[ -z "$MODEL_ID" ]]; then
  log "註冊 ${ML_MODEL_NAME}（版本 ${ML_MODEL_VERSION}、ONNX，約 92 MB 下載）"
  TASK_ID="$(api POST /_plugins/_ml/models/_register '{
    "name": "'"${ML_MODEL_NAME}"'",
    "version": "'"${ML_MODEL_VERSION}"'",
    "model_format": "ONNX"
  }' | jq_py "
import json,sys
d=json.load(sys.stdin)
if 'task_id' not in d:
    sys.exit('註冊未被接受：%s' % json.dumps(d, ensure_ascii=False))
print(d['task_id'])
")"
  MODEL_ID="$(wait_task "$TASK_ID" "註冊模型")"
  MODEL_STATE="REGISTERED"
  log "已註冊：model_id=${MODEL_ID}"
else
  log "已存在，跳過註冊：model_id=${MODEL_ID}（state=${MODEL_STATE}）"
fi

# --------------------------------------------------------------------------
# 5. 部署
# --------------------------------------------------------------------------
if [[ "$MODEL_STATE" == "DEPLOYED" ]]; then
  log "已部署，跳過 deploy"
else
  log "部署模型（首次會另外抓 publish.djl.ai 的 PyTorch native libs，約 507 MB）"
  TASK_ID="$(api POST "/_plugins/_ml/models/${MODEL_ID}/_deploy" | jq_py "
import json,sys
d=json.load(sys.stdin)
if 'task_id' not in d:
    sys.exit('部署未被接受：%s' % json.dumps(d, ensure_ascii=False))
print(d['task_id'])
")"
  wait_task "$TASK_ID" "部署模型" >/dev/null
  log "已部署"
fi

# --------------------------------------------------------------------------
# 6. 冒煙測試：真的推得出 384 維向量，而且語意排序正確
# --------------------------------------------------------------------------
# 「deploy 回 COMPLETED」只證明它沒抱怨，不證明它推得出東西
# （CLAUDE.md「不報錯不等於正常」）。這裡實際取三段文字的向量並斷言：
#   1. 維度是 384
#   2. 語意相近的兩句，cosine 相似度高於語意無關的那一句
# 用英文句子做斷言是刻意的：all-MiniLM-L6-v2 是**英文模型**，中文的
# 區隔度只有 0.047（英文是 0.79），拿中文當 CI 斷言會是脆弱的測試。
# 中文能力的實測數字與後果見 docs/developer/embedding.md。
log "冒煙測試推論"
api POST "/_plugins/_ml/_predict/text_embedding/${MODEL_ID}" '{
  "text_docs": [
    "Microsoft is a technology company",
    "Microsoft is a major software vendor",
    "The weather is nice today"
  ],
  "return_number": true,
  "target_response": ["sentence_embedding"]
}' | EXPECTED_DIM="$EXPECTED_DIM" jq_py "
import json, math, os, sys
expected = int(os.environ['EXPECTED_DIM'])
d = json.load(sys.stdin)
results = d.get('inference_results')
if not results:
    sys.exit('推論沒有回傳 inference_results：%s' % json.dumps(d, ensure_ascii=False)[:500])
vecs = [r['output'][0]['data'] for r in results]
for i, v in enumerate(vecs):
    if len(v) != expected:
        sys.exit('第 %d 段文字的向量是 %d 維，預期 %d 維。'
                 '模型或版本不對，index mapping 的 dimension 會跟著錯。' % (i, len(v), expected))
def cos(a, b):
    return sum(x*y for x, y in zip(a, b)) / (
        math.sqrt(sum(x*x for x in a)) * math.sqrt(sum(y*y for y in b)))
near, far = cos(vecs[0], vecs[1]), cos(vecs[0], vecs[2])
print('   維度 %d ✅' % expected)
print('   cos(同主題) = %.6f' % near)
print('   cos(不相關) = %.6f' % far)
if near <= far:
    sys.exit('語意排序錯誤：同主題的相似度 %.6f 沒有高於不相關的 %.6f。'
             '模型雖然載入成功，但推出來的向量沒有語意，不能用來做 semantic search。' % (near, far))
print('   語意排序正確（差 %.6f）✅' % (near - far))
"

log "完成。model_id=${MODEL_ID}"
echo
echo "把這個 id 記進 embedding 設定（SPEC_V0.2 §11 的 model / model_version）："
echo "  OSINT__EMBEDDING__MODEL_ID=${MODEL_ID}"
echo "  模型：${ML_MODEL_NAME} v${ML_MODEL_VERSION}（ONNX，${EXPECTED_DIM} 維）"
