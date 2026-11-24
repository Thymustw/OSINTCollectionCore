# syntax=docker/dockerfile:1.11
#
# osint-inference：e5-small int8 向量推論（ADR-016 D2，階段 5 M2-1）。
#   docker build -f services/inference/Dockerfile -t osint-core/osint-inference:0.1.0 services/inference
#
# base image tag + digest 雙重釘選（同 docker/Dockerfile 的慣例）。digest 2026-11-24 由 Docker Hub API 查得：
#   curl -s "https://hub.docker.com/v2/repositories/library/python/tags/?name=3.12-slim-bookworm" | jq -r '.results[0].digest'
ARG PYTHON_IMAGE=python:3.12-slim-bookworm@sha256:935c3c5b12357a7e10caa9c1e6ca5692344d97cf229832ef584be2cec9fc03df

# ---------------------------------------------------------------------------
# 模型檔：建置時下載並驗 sha256，不進 repo。hash 與 scripts/opensearch-ml-setup-e5.sh
# （ml-commons 部署的就是這兩個檔）一致，三處（含 app 的環境變數）一起改。
# ---------------------------------------------------------------------------
FROM ${PYTHON_IMAGE} AS model
ARG HF=https://huggingface.co/intfloat/multilingual-e5-small/resolve/main
ARG ONNX_SHA256=dd476dd0c2514e9b9be83aeb3853fac0763e0bdf4a71645407587d77c48a2d88
# gitleaks generic-api-key 會把變數名裡的「TOKENizer」當成 token 關鍵字；這是公開檔案的 sha256。
ARG TOKENIZER_SHA256=0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39 # gitleaks:allow
RUN python - <<'PY'
import hashlib, os, urllib.request
base = os.environ["HF"]
for remote, local, want in (
    ("onnx/model_qint8_avx512_vnni.onnx", "/model/model.onnx", os.environ["ONNX_SHA256"]),
    ("tokenizer.json", "/model/tokenizer.json", os.environ["TOKENIZER_SHA256"]),
):
    os.makedirs("/model", exist_ok=True)
    urllib.request.urlretrieve(f"{base}/{remote}", local)
    got = hashlib.sha256(open(local, "rb").read()).hexdigest()
    if got != want:
        raise SystemExit(f"{remote} sha256 不符：預期 {want}，實際 {got}")
    print(f"{remote}: sha256 OK")
# 驗過的 hash 寫成檔案給 app 啟動時再驗一次（ENV 不支援行尾註解，放不了 gitleaks:allow）。
with open("/model/SHA256SUMS", "w") as f:
    f.write(f"{os.environ['ONNX_SHA256']}  model.onnx\n{os.environ['TOKENIZER_SHA256']}  tokenizer.json\n")
PY

# ---------------------------------------------------------------------------
# 相依：requirements.lock 帶 hash，--require-hashes 拒絕任何沒列 hash 的套件（含傳遞相依）。
# ---------------------------------------------------------------------------
FROM ${PYTHON_IMAGE} AS deps
ENV PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_NO_CACHE_DIR=1
COPY requirements.lock /tmp/requirements.lock
RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-deps --require-hashes -r /tmp/requirements.lock

# ---------------------------------------------------------------------------
# runtime
# ---------------------------------------------------------------------------
FROM ${PYTHON_IMAGE} AS runtime
# HEALTHCHECK 用 python 自己打 /health，不另裝 curl（少一個套件就少一批 CVE）。
RUN groupadd --system --gid 10001 osint \
 && useradd --system --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent \
        --shell /usr/sbin/nologin osint \
 && mkdir -m 0555 -p /opt/model /app
COPY --from=deps /opt/venv /opt/venv
COPY --from=model --chown=root:root --chmod=444 /model/model.onnx /model/tokenizer.json /model/SHA256SUMS /opt/model/
COPY --chown=root:root --chmod=444 app/ /app/app/
RUN chmod 0555 /app/app
# HF_HUB_OFFLINE：tokenizers 硬相依 huggingface-hub；本服務只讀本機檔，禁止任何連網下載。
ENV PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    HF_HUB_OFFLINE=1 \
    INFERENCE_ONNX_PATH=/opt/model/model.onnx \
    INFERENCE_TOKENIZER_PATH=/opt/model/tokenizer.json \
    INFERENCE_SHA256SUMS=/opt/model/SHA256SUMS
WORKDIR /app
USER 10001:10001
EXPOSE 18091
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=6 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:18091/health',timeout=2).status==200 else 1)"
# 單一 worker 行程：模型只載一份；推論並行度由 app 內的 lock 與 INFERENCE_MAX_PENDING 控制。
# --limit-concurrency 擋住連線數暴增；--timeout-graceful-shutdown 讓 SIGTERM 時手上的請求做完。
ENTRYPOINT ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "18091", \
            "--workers", "1", "--limit-concurrency", "64", "--timeout-graceful-shutdown", "10", \
            "--no-server-header", "--log-level", "info"]
