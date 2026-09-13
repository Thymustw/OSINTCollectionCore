# syntax=docker/dockerfile:1.11
#
# OSINT Intelligence Core — 七個服務的 multi-stage build。
#
# 用法（從 repo 根目錄）：
#   docker build -f docker/Dockerfile --target osint-api -t osint-core/osint-api:0.1.0 .
# 七個 target：osint-api / osint-collector / osint-normalizer /
#              osint-deduplicator / osint-entity-worker / osint-indexer /
#              osint-graph-worker
#
# ---------------------------------------------------------------------------
# 為什麼用 BuildKit cache mount 而不是 cargo-chef
# ---------------------------------------------------------------------------
# cargo-chef 的做法是把相依樹「烘」成一層獨立的 image layer，好處是跨 build
# （甚至跨 CI run，只要有 layer cache）都能重用；代價是多一個 `cargo install
# cargo-chef` 步驟（或多釘一個第三方 base image），而且 recipe.json 對 24 個
# crate 的 path dependency workspace 要多一輪 prepare/cook。
#
# 這裡改用 `--mount=type=cache` 把 `$CARGO_HOME/registry` 與 `target/` 掛成
# BuildKit 快取：不需要任何額外工具、不會在 image 裡留下相依層，本機連續 build
# 的加速效果與 cargo-chef 相當。
#
# **已知取捨（不要誤以為有跨 run 快取）**：cache mount 存在 BuildKit 的 builder
# 快取裡，不是 image layer，所以 GitHub Actions 預設每個 job 都是冷的，CI 上
# 每次都會全量重編。V0.1 的 CI image build 本來就只跑一次，這個代價可以接受；
# 真的要省 CI 時間時應該加 `cache-from/cache-to: type=gha`，而不是換回 cargo-chef。
#
# 因為 target/ 在 cache mount 裡（build 結束就看不到了），binary 必須在**同一個
# RUN 指令內**複製到 /out，之後的 stage 才拿得到。
#
# ---------------------------------------------------------------------------
# Base image 釘選
# ---------------------------------------------------------------------------
# tag + digest 雙重釘選。tag 給人看，digest 才是真正決定拉到什麼的東西
# （tag 會被上游覆寫，digest 不會）。更新方式：
#   curl -s https://hub.docker.com/v2/repositories/library/rust/tags/<tag> | jq -r .digest
#   curl -s "https://hub.docker.com/v2/repositories/library/debian/tags/?name=bookworm-slim" | jq -r '.results[0].digest'
#
# builder 用 `rust:1.98.1-bookworm`（**不是 -slim**）：它以 buildpack-deps 為底，
# 已經帶 gcc/make/zlib1g-dev/git。rdkafka 走 mklove `./configure`（見 Cargo.toml
# 對 rdkafka 的註解，刻意不用 cmake-build），需要的就是這組工具鏈；換成 -slim
# 會在 rdkafka-sys 的 build script 掛掉。
ARG RUST_IMAGE=rust:1.98.1-bookworm@sha256:9a73a5088750b4c95158ab26629c854c3d6fc4b173cb7bc8079ad252d8ed7bfa
ARG RUNTIME_IMAGE=debian:bookworm-slim@sha256:88200866dfff7ea7f5cbcb6ec7c8a701889efe6fe859fe64d6990e4b07ea4171

# ===========================================================================
# builder
# ===========================================================================
FROM ${RUST_IMAGE} AS builder

WORKDIR /src

# 不在這裡設 RUSTFLAGS="-D warnings"：warning gate 已經由 CI 的 lint job
# （cargo clippy -D warnings）把關。在 image build 裡再設一次，只會讓
# 「容器建不起來」與「程式碼有 warning」兩種完全不同的故障共用同一個錯誤畫面。
#
# rust-toolchain.toml 寫的是 `channel = "stable"`，base image 內建的 1.98.1
# 就是 stable，rustup 不會另外下載 toolchain。
ENV CARGO_TERM_COLOR=never \
    CARGO_INCREMENTAL=0

COPY . .

# 一次把七個服務都編出來。--locked 確保用的是 committed 的 Cargo.lock，
# 而不是讓 cargo 在容器裡悄悄解析出另一組版本（那會讓 image 掃出來的相依
# 跟 cargo-deny/cargo-audit 看到的不一致）。
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --locked --release --workspace --bins \
 && mkdir -p /out \
 && for bin in osint-api osint-collector osint-normalizer \
               osint-deduplicator osint-entity-worker osint-indexer \
               osint-graph-worker; do \
        cp "target/release/$bin" "/out/$bin"; \
    done \
 && ls -l /out

# ===========================================================================
# runtime-base：七個服務共用的執行期底座
# ===========================================================================
#
# **為什麼是 debian:bookworm-slim 而不是 distroless**
#
# distroless/cc 的 CVE 面小得多，但它沒有 shell 也沒有 curl，容器就無法自己做
# HEALTHCHECK——而 SPEC §27 的 exit criteria 要求 compose 起來的服務看得到健康
# 狀態。要同時拿到兩者，得在每個 binary 裡加一個 `--health-check` 子指令當探針，
# 那是 V0.1 範圍外的程式改動。
#
# 目前的取捨：bookworm-slim + curl，攻擊面換健康可觀測性。
# **V1.0 要改掉**：加內建健康探針子指令後換 distroless/cc:nonroot。
# 這條記在 docs/operations/OPERATIONS.md。
FROM ${RUNTIME_IMAGE} AS runtime-base

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
 && rm -rf /var/lib/apt/lists/*

# non-root（DEVSECOPS 要求）。uid/gid 固定 10001，不靠 useradd 自動配號——
# 掛 volume 時宿主端要對得上，浮動的 uid 會讓權限問題只在某些機器上出現。
# --no-create-home：服務不需要家目錄，也不該有可寫的家目錄。
RUN groupadd --system --gid 10001 osint \
 && useradd --system --uid 10001 --gid 10001 \
        --no-create-home --home-dir /nonexistent \
        --shell /usr/sbin/nologin osint

# AppConfig::load() 從 cwd 往上找 `config/default.toml`（core-config/src/lib.rs
# 的 find_default_config）。WORKDIR 是 /app，所以設定要放 /app/config/。
# 容器內所有覆寫都走 OSINT__* 環境變數，這個檔只提供預設值。
# ⚠️ 目錄要**先自己建**，不能讓 COPY 順手建。
# 實測（2026-09-12）：只寫 `COPY --chmod=444 config/default.toml
# /app/config/default.toml`，BuildKit 會把 `--chmod` 一併套到它自動建出來的
# 父目錄 `/app/config`，結果是 `dr--r--r--`——目錄少了 execute bit，
# uid 10001 根本走不進去。六個容器全部在啟動時死在
# 「工作區根目錄找不到 config/default.toml」，而檔案其實就在那裡。
WORKDIR /app
RUN mkdir -m 0555 -p /app/config
COPY --chown=root:root --chmod=444 config/default.toml /app/config/default.toml

USER 10001:10001

# ===========================================================================
# 七個服務 stage
# ===========================================================================
#
# 每個 stage 只複製自己那一支 binary。HEALTHCHECK 一律打 127.0.0.1——
# 不管 bind 設成 0.0.0.0 還是 127.0.0.1 都通得過。
#
# ⚠️ **compose 必須把 bind 覆寫成 0.0.0.0**。config/default.toml 的預設是
# 127.0.0.1（本機開發刻意不對外），在容器裡那等於「只有容器自己連得到」，
# HEALTHCHECK 會過但別的容器與宿主都連不上。覆寫值見 docker-compose.yml。

FROM runtime-base AS osint-api
COPY --from=builder --chown=root:root --chmod=555 /out/osint-api /usr/local/bin/osint-api
EXPOSE 18080
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=12 \
    CMD curl -fsS http://127.0.0.1:18080/health || exit 1
ENTRYPOINT ["/usr/local/bin/osint-api"]

FROM runtime-base AS osint-collector
COPY --from=builder --chown=root:root --chmod=555 /out/osint-collector /usr/local/bin/osint-collector
EXPOSE 18081
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=12 \
    CMD curl -fsS http://127.0.0.1:18081/health || exit 1
ENTRYPOINT ["/usr/local/bin/osint-collector"]

FROM runtime-base AS osint-normalizer
COPY --from=builder --chown=root:root --chmod=555 /out/osint-normalizer /usr/local/bin/osint-normalizer
EXPOSE 18082
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=12 \
    CMD curl -fsS http://127.0.0.1:18082/health || exit 1
ENTRYPOINT ["/usr/local/bin/osint-normalizer"]

FROM runtime-base AS osint-deduplicator
COPY --from=builder --chown=root:root --chmod=555 /out/osint-deduplicator /usr/local/bin/osint-deduplicator
EXPOSE 18083
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=12 \
    CMD curl -fsS http://127.0.0.1:18083/health || exit 1
ENTRYPOINT ["/usr/local/bin/osint-deduplicator"]

FROM runtime-base AS osint-entity-worker
COPY --from=builder --chown=root:root --chmod=555 /out/osint-entity-worker /usr/local/bin/osint-entity-worker
EXPOSE 18084
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=12 \
    CMD curl -fsS http://127.0.0.1:18084/health || exit 1
ENTRYPOINT ["/usr/local/bin/osint-entity-worker"]

FROM runtime-base AS osint-indexer
COPY --from=builder --chown=root:root --chmod=555 /out/osint-indexer /usr/local/bin/osint-indexer
EXPOSE 18085
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=12 \
    CMD curl -fsS http://127.0.0.1:18085/health || exit 1
ENTRYPOINT ["/usr/local/bin/osint-indexer"]

FROM runtime-base AS osint-graph-worker
COPY --from=builder --chown=root:root --chmod=555 /out/osint-graph-worker /usr/local/bin/osint-graph-worker
EXPOSE 18086
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=12 \
    CMD curl -fsS http://127.0.0.1:18086/health || exit 1
ENTRYPOINT ["/usr/local/bin/osint-graph-worker"]
