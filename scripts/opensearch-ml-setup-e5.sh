#!/usr/bin/env bash
# opensearch-ml-setup-e5.sh — 把多語 embedding 模型 multilingual-e5-small
# 註冊進 OpenSearch ml-commons。
#
# 為什麼要第二支腳本，而不是把 opensearch-ml-setup.sh 參數化：
# 兩者的**註冊路徑完全不同**。all-MiniLM-L6-v2 在 ml-commons 的官方
# pretrained 清單裡，`_register` 只要給 name+version 就會自己去
# artifacts.opensearch.org 抓。e5-small **不在那份清單**，必須走
# custom model：自己備妥 ONNX + tokenizer 打包成 zip、算 SHA-256、
# 開一個 OpenSearch 容器連得到的 HTTP 服務、再用 `url` 欄位註冊。
# 硬要塞進同一支腳本會變成兩條互不相干的分支。
#
# 用法：
#   bash scripts/opensearch-ml-setup-e5.sh
#   OPENSEARCH_URL=http://127.0.0.1:19200 bash scripts/opensearch-ml-setup-e5.sh
#
# 環境變數：
#   OPENSEARCH_URL   預設 http://127.0.0.1:19200
#   E5_CACHE_DIR     ONNX／tokenizer／zip 的暫存位置，預設 /tmp/osint-e5-model
#   E5_SERVE_ADDR    臨時 HTTP 服務要綁的位址，預設自動偵測 compose network gateway
#   E5_SERVE_PORT    預設 18080
#   ML_TIMEOUT_SECS  非同步工作輪詢上限，預設 900
#
# ---------------------------------------------------------------------------
# ⚠️ 資源需求：OpenSearch 容器 3 GB、JVM heap 1536m
# ---------------------------------------------------------------------------
# 這支腳本預設「e5-small 要和英文 MiniLM **並存**」。2026-09-12 實測：
#   * 2 GB 容器放第二個模型 → kernel cgroup OOM-kill（docker 的
#     .State.OOMKilled 會騙你說 false，要看 journalctl -k）
#   * 3 GB 容器但 heap 只有 1g → 兩個模型都起得來，但 heap 卡在 86–92 %
#     超過 jvm_heap_memory_threshold(85)，**每次推論都回 HTTP 429**
# 完整數據見 docs/developer/embedding.md。
#
# ⚠️ 用 int8 量化版不是為了省磁碟，是因為 FP32（470 MB）在 3 GB 容器
#    也會 OOM-kill。實測 int8 的檢索品質與 FP32 相同、延遲快一倍。

set -euo pipefail

OPENSEARCH_URL="${OPENSEARCH_URL:-http://127.0.0.1:19200}"
E5_CACHE_DIR="${E5_CACHE_DIR:-/tmp/osint-e5-model}"
E5_SERVE_PORT="${E5_SERVE_PORT:-18080}"
ML_TIMEOUT_SECS="${ML_TIMEOUT_SECS:-900}"

ML_MODEL_NAME="intfloat/multilingual-e5-small-int8"
ML_MODEL_VERSION="1.0.0"
EXPECTED_DIM=384

HF_REPO="intfloat/multilingual-e5-small"
# 直接寫死上游 LFS 宣告的 SHA-256，下載後比對。HuggingFace 的
# /api/models/<repo>/tree/main 會把這顆 hash 放在 lfs.oid，來源可查：
#   curl -s https://huggingface.co/api/models/intfloat/multilingual-e5-small/tree/main/onnx
ONNX_SHA256="dd476dd0c2514e9b9be83aeb3853fac0763e0bdf4a71645407587d77c48a2d88"
ONNX_REMOTE="onnx/model_qint8_avx512_vnni.onnx"
TOKENIZER_SHA256="0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39"

# 打包後 zip 的 SHA-256。**這是冪等檢查唯一可靠的鍵**（理由同
# opensearch-ml-setup.sh：ml-commons 不會存上游版本號）。
# 因為 build_zip() 固定了 zip 內每個 entry 的 date_time 與壓縮方式，
# 同樣的輸入一定打包出同樣的位元組，所以這顆 hash 是穩定的。
# 改 ONNX 檔或 tokenizer 時這個值要一起換。
ZIP_SHA256="e8f6bd1be427a518c2160f1742fd3b70a1a1e0c01b4a93edf98671c8128c9ff6"

log()  { printf '== %s\n' "$*"; }
fail() { printf '❌ %s\n' "$*" >&2; exit 1; }

command -v curl    >/dev/null 2>&1 || fail "找不到 curl。"
command -v python3 >/dev/null 2>&1 || fail "找不到 python3（解析 JSON、打包 zip、開臨時 HTTP 服務都要用）。"

HTTP_PID=""
cleanup() {
  if [[ -n "$HTTP_PID" ]] && kill -0 "$HTTP_PID" 2>/dev/null; then
    kill "$HTTP_PID" 2>/dev/null || true
    log "已關閉臨時 HTTP 服務（pid ${HTTP_PID}）"
  fi
}
trap cleanup EXIT

api() {
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
# 0. 確認對面真的是 OpenSearch
# --------------------------------------------------------------------------
# 與 opensearch-ml-setup.sh 同一個理由：本工作站的 9200 是 OpenCTI 的
# Elasticsearch，打錯埠號會對別人的叢集下 _cluster/settings（ADR-005）。
log "檢查 ${OPENSEARCH_URL} 的身分"
ROOT_JSON="$(api GET /)" || fail "連不上 ${OPENSEARCH_URL}。請先 make compose-up。"
DIST="$(jq_py "
import json,sys
print(json.load(sys.stdin).get('version',{}).get('distribution',''))
" <<<"$ROOT_JSON")"
[[ "$DIST" == "opensearch" ]] || fail "${OPENSEARCH_URL} 不是 OpenSearch（version.distribution='${DIST}'）。
   這台工作站的 9200 是 OpenCTI 的 Elasticsearch，ml-commons 的 API 在那裡不存在。
   本專案的 OpenSearch 在 19200。"
log "確認為 OpenSearch $(jq_py "import json,sys;print(json.load(sys.stdin)['version']['number'])" <<<"$ROOT_JSON")"

# --------------------------------------------------------------------------
# 1. 冪等檢查
# --------------------------------------------------------------------------
find_model() {
  api POST /_plugins/_ml/models/_search '{
    "size": 5,
    "_source": ["model_state", "model_content_hash_value"],
    "query": {"bool": {
      "must": [
        {"term": {"name.keyword": "'"${ML_MODEL_NAME}"'"}},
        {"term": {"model_content_hash_value": "'"${ZIP_SHA256}"'"}}
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
# 2. 非同步工作輪詢
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
        [[ "$err" == *"Duplicate deploy model task"* ]] && { log "已有同一個模型的 deploy 工作在跑，視為成功"; return 0; }
        fail "${what} 失敗（state=${state}）：${err}"
        ;;
    esac
    if (( waited >= ML_TIMEOUT_SECS )); then
      fail "${what} 超過 ${ML_TIMEOUT_SECS} 秒仍是 ${state}。
   ⚠️ 若 task 長期停在 RUNNING 但模型狀態是 DEPLOY_FAILED，多半是 OpenSearch
      **整個被 kernel OOM-kill 後重啟**，task 文件因此永遠停在 RUNNING。
      確認方式：docker inspect osint-core-opensearch-1 --format '{{.RestartCount}}'
                journalctl -k | grep 'Memory cgroup out of memory'
      （不要看 .State.OOMKilled，實測它在真的被 OOM-kill 時仍回報 false）"
    fi
    sleep 5
    waited=$(( waited + 5 ))
    printf '   %s… %ss（state=%s）\n' "$what" "$waited" "$state" >&2
  done
}

if [[ -n "$MODEL_ID" && "$MODEL_STATE" == "DEPLOYED" ]]; then
  log "已存在且已部署，跳過下載／註冊／部署：model_id=${MODEL_ID}"
else

# --------------------------------------------------------------------------
# 3. 備妥模型檔
# --------------------------------------------------------------------------
mkdir -p "$E5_CACHE_DIR"

verify_sha() {
  # verify_sha <檔案> <預期 SHA-256>；不符就回非 0（呼叫端決定重抓還是中止）
  local f="$1" want="$2" got
  [[ -f "$f" ]] || return 1
  got="$(python3 - "$f" <<'PY'
import hashlib, sys
h = hashlib.sha256()
with open(sys.argv[1], 'rb') as fh:
    for b in iter(lambda: fh.read(1 << 20), b''):
        h.update(b)
print(h.hexdigest())
PY
)"
  [[ "$got" == "$want" ]]
}

fetch() {
  # fetch <remote 相對路徑> <本地檔名> <預期 SHA-256>
  local remote="$1" local_name="$2" want="$3"
  local path="${E5_CACHE_DIR}/${local_name}"
  if verify_sha "$path" "$want"; then
    log "已快取且 SHA-256 相符，跳過下載：${local_name}"
    return 0
  fi
  log "下載 ${remote}（來源 HuggingFace ${HF_REPO}）"
  curl -sSL --fail -o "$path" \
    "https://huggingface.co/${HF_REPO}/resolve/main/${remote}" \
    || fail "下載 ${remote} 失敗。這台機器的 raw.githubusercontent.com 被 sinkhole，
   但 huggingface.co 實測可用；若是離線環境請自行把檔案放到 ${path}。"
  verify_sha "$path" "$want" \
    || fail "${local_name} 的 SHA-256 與上游宣告不符。
   上游可能改檔，或下載被中間人／proxy 竄改。**不要**直接改腳本裡的 hash 繞過，
   先確認 https://huggingface.co/${HF_REPO} 的 commit 記錄。"
  log "   SHA-256 驗證通過"
}

fetch "$ONNX_REMOTE" "model.onnx" "$ONNX_SHA256"
fetch "tokenizer.json" "tokenizer.json" "$TOKENIZER_SHA256"

# ml-commons 要的 zip 結構很簡單：根目錄放 ONNX 檔與 tokenizer.json。
# **entry 名稱必須是 model.onnx**（DJL 靠副檔名找模型），所以 int8 檔
# 在 zip 裡也叫 model.onnx。
build_zip() {
  local out="${E5_CACHE_DIR}/multilingual-e5-small-int8.zip"
  if verify_sha "$out" "$ZIP_SHA256"; then
    log "zip 已存在且 SHA-256 相符，跳過打包"
    return 0
  fi
  log "打包 zip"
  python3 - "$E5_CACHE_DIR" "$out" <<'PY'
import sys, zipfile
cache, out = sys.argv[1], sys.argv[2]
# 固定 date_time 與壓縮方式，讓同樣的輸入一定產生同樣的位元組。
# 不固定的話 zip 會內嵌打包當下的時間戳，每次 SHA-256 都不同，
# 冪等檢查就永遠命中不了（會變成每次都重新註冊一次）。
with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED) as z:
    for name in ('model.onnx', 'tokenizer.json'):
        zi = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
        zi.compress_type = zipfile.ZIP_DEFLATED
        zi.external_attr = 0o644 << 16
        with open(f'{cache}/{name}', 'rb') as f, z.open(zi, 'w') as o:
            while (chunk := f.read(1 << 20)):
                o.write(chunk)
PY
  verify_sha "$out" "$ZIP_SHA256" \
    || fail "打包出來的 zip SHA-256 與預期不符。
   多半是 python 版本的 zipfile 壓縮輸出有差異。請重新量一次並更新腳本裡的
   ZIP_SHA256，同時更新 docs/developer/embedding.md。"
  log "   zip SHA-256 驗證通過"
}
build_zip

# --------------------------------------------------------------------------
# 4. 開一個 OpenSearch 容器連得到的臨時 HTTP 服務
# --------------------------------------------------------------------------
# ml-commons 的 custom model 只能從 URL 抓，不能吃宿主路徑
#（plugins.ml_commons.allow_registering_model_via_local_file 是另一回事，
#  而且預設也是 false）。所以這裡自己開一個 http.server。
# 位址不能用 127.0.0.1——那在容器裡是容器自己。要綁 compose network 的 gateway。
if [[ -z "${E5_SERVE_ADDR:-}" ]]; then
  E5_SERVE_ADDR="$(docker network inspect osint-core-net \
    -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}' 2>/dev/null || true)"
  [[ -n "$E5_SERVE_ADDR" ]] || fail "抓不到 osint-core-net 的 gateway 位址。
   請先 make compose-up，或手動指定 E5_SERVE_ADDR。"
fi
log "在 ${E5_SERVE_ADDR}:${E5_SERVE_PORT} 開臨時 HTTP 服務"
python3 -m http.server "$E5_SERVE_PORT" --bind "$E5_SERVE_ADDR" \
  --directory "$E5_CACHE_DIR" >/dev/null 2>&1 &
HTTP_PID=$!
sleep 2
kill -0 "$HTTP_PID" 2>/dev/null || fail "臨時 HTTP 服務起不來，${E5_SERVE_PORT} 可能已被占用。"

ZIP_URL="http://${E5_SERVE_ADDR}:${E5_SERVE_PORT}/multilingual-e5-small-int8.zip"

# --------------------------------------------------------------------------
# 5. 叢集設定
# --------------------------------------------------------------------------
# allow_registering_model_via_url 預設 **false**，不開的話 custom model
# 一定註冊不了。only_run_on_ml_node 的理由見 opensearch-ml-setup.sh。
#
# ⚠️ 刻意**不設** plugins.ml_commons.native_memory_threshold：
# 那個設定在 2.19 的原始碼裡已被 comment out（PR #1015），設了不會生效，
# 寫在這裡只會讓下一個人以為 native memory 有保護。實際唯一生效的是
# jvm_heap_memory_threshold（預設 85）。詳見 docs/developer/embedding.md。
log "設定 plugins.ml_commons.*"
api PUT /_cluster/settings '{
  "persistent": {
    "plugins.ml_commons.only_run_on_ml_node": false,
    "plugins.ml_commons.allow_registering_model_via_url": true
  }
}' | jq_py "
import json,sys
d=json.load(sys.stdin)
if not d.get('acknowledged'):
    sys.exit('叢集設定未被接受：%s' % json.dumps(d, ensure_ascii=False))
print('   acknowledged')
"

# --------------------------------------------------------------------------
# 6. 註冊
# --------------------------------------------------------------------------
if [[ -z "$MODEL_ID" ]]; then
  log "註冊 ${ML_MODEL_NAME}（custom ONNX，約 84 MB）"
  # ⚠️ 不要放 "model_group_id": null —— ml-commons 會噴
  # "Can't get text on a VALUE_NULL"，而且 HTTP 500 看不出是哪個欄位。
  TASK_ID="$(api POST /_plugins/_ml/models/_register '{
    "name": "'"${ML_MODEL_NAME}"'",
    "version": "'"${ML_MODEL_VERSION}"'",
    "description": "multilingual-e5-small, 384-dim, ONNX int8, MEAN pooling, normalized",
    "model_format": "ONNX",
    "model_content_hash_value": "'"${ZIP_SHA256}"'",
    "model_config": {
      "model_type": "bert",
      "embedding_dimension": '"${EXPECTED_DIM}"',
      "framework_type": "sentence_transformers",
      "pooling_mode": "MEAN",
      "normalize_result": true,
      "all_config": "{\"architectures\":[\"BertModel\"],\"hidden_size\":384,\"max_position_embeddings\":512,\"model_type\":\"bert\",\"num_attention_heads\":12,\"num_hidden_layers\":12,\"tokenizer_class\":\"XLMRobertaTokenizer\",\"vocab_size\":250037}"
    },
    "url": "'"${ZIP_URL}"'"
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
  log "已註冊過，跳過：model_id=${MODEL_ID}（state=${MODEL_STATE}）"
fi

# --------------------------------------------------------------------------
# 7. 部署
# --------------------------------------------------------------------------
if [[ "$MODEL_STATE" == "DEPLOYED" ]]; then
  log "已部署，跳過 deploy"
else
  log "部署模型"
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

fi  # 冪等短路結束

# --------------------------------------------------------------------------
# 8. 冒煙測試：中文真的推得出有語意的向量
# --------------------------------------------------------------------------
# 這支腳本存在的理由就是中文，所以斷言**必須用中文**。
# （opensearch-ml-setup.sh 用英文斷言是因為 MiniLM 的中文區隔度只有 0.047，
#   拿來當斷言會是脆弱的測試；e5 沒有這個問題。）
#
# 注意 e5 的 cosine 基線很高（不相關的句子也有 0.83 左右），所以這裡不比
# 絕對數值，只斷言「同主題 > 不相關」以及維度正確。真正的品質證據是
# docs/developer/embedding.md 的小型檢索測試（中文 top-1 5/5）。
log "冒煙測試推論（中文）"
api POST "/_plugins/_ml/_predict/text_embedding/${MODEL_ID}" '{
  "text_docs": [
    "query: Microsoft 是一家科技公司",
    "query: 微軟是軟體大廠",
    "query: 今天天氣很好"
  ],
  "return_number": true,
  "target_response": ["sentence_embedding"]
}' | EXPECTED_DIM="$EXPECTED_DIM" jq_py "
import json, math, os, sys
expected = int(os.environ['EXPECTED_DIM'])
d = json.load(sys.stdin)
if d.get('status') == 429 or 'circuit_breaking_exception' in json.dumps(d):
    sys.exit('推論被斷路器擋下（HTTP 429）。這通常**不是**容器記憶體不足，'
             '而是 JVM heap 超過 jvm_heap_memory_threshold(85)。'
             '請確認 OPENSEARCH_JAVA_OPTS 是 -Xmx1536m 而不是 -Xmx1g。'
             '詳見 docs/developer/embedding.md。')
results = d.get('inference_results')
if not results:
    sys.exit('推論沒有回傳 inference_results：%s' % json.dumps(d, ensure_ascii=False)[:500])
vecs = [r['output'][0]['data'] for r in results]
for i, v in enumerate(vecs):
    if len(v) != expected:
        sys.exit('第 %d 段文字的向量是 %d 維，預期 %d 維。'
                 'index mapping 的 dimension 會跟著錯。' % (i, len(v), expected))
def cos(a, b):
    return sum(x*y for x, y in zip(a, b)) / (
        math.sqrt(sum(x*x for x in a)) * math.sqrt(sum(y*y for y in b)))
near, far = cos(vecs[0], vecs[1]), cos(vecs[0], vecs[2])
print('   維度 %d ✅' % expected)
print('   cos(同主題) = %.6f' % near)
print('   cos(不相關) = %.6f' % far)
if near <= far:
    sys.exit('中文語意排序錯誤：同主題 %.6f 沒有高於不相關 %.6f。' % (near, far))
print('   中文語意排序正確（差 %.6f）✅' % (near - far))
"

log "完成。model_id=${MODEL_ID}"
echo
echo "多語（含中文）embedding 模型："
echo "  OSINT__EMBEDDING__MULTILINGUAL_MODEL_ID=${MODEL_ID}"
echo "  模型：${ML_MODEL_NAME}（ONNX int8，${EXPECTED_DIM} 維）"
echo
echo "⚠️ 查詢與文件要分別加 'query: ' / 'passage: ' 前綴，理由見"
echo "   docs/developer/embedding.md。"
