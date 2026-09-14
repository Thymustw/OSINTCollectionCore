# OSINT Intelligence Core — 常用開發目標
# audit / secret-scan / docker-scan 依賴尚未納入 bootstrap 的外部工具；
# 目標先寫成直接呼叫，工具未安裝時應失敗而不是假裝通過。

.PHONY: help check build test lint fmt audit secret-scan secret-scan-canary docker-scan \
	compose-up compose-down compose-ps migrate-postgres migrate-sqlite \
	run-api run-collector run-normalizer run-deduplicator run-entity-worker \
	run-indexer rebuild-index rebuild-index-drop run-cli \
	run-graph-worker rebuild-graph rebuild-graph-drop \
	run-embedding-worker rebuild-embeddings rebuild-embeddings-drop \
	image-build image-scan image-prune image-ls \
	compose-up-full compose-down-full compose-ps-full \
	disk clean

export PATH := $(HOME)/.cargo/bin:$(PATH)
CARGO ?= cargo
SQLX ?= sqlx
COMPOSE ?= docker compose
COMPOSE_FILES := -f docker/docker-compose.yml -f docker/docker-compose.dev.yml
DATABASE_URL ?= postgres://osint:osint_dev@127.0.0.1:5432/osint_core
SQLITE_URL ?= sqlite:/home/bkup/OSINTCollectionCore/var/osint-local.sqlite?mode=rwc

help:
	@echo "可用目標："
	@echo "  make check             cargo check --workspace"
	@echo "  make build             cargo build --workspace"
	@echo "  make test              cargo test --workspace"
	@echo "  make lint              rustfmt --check + clippy -D warnings"
	@echo "  make fmt               cargo fmt --all"
	@echo "  make compose-up        啟動本機基礎建設（含 dev 資源限制）"
	@echo "  make compose-down      停止本機基礎建設"
	@echo "  make migrate-postgres  對本機 Postgres 跑 sqlx migrate"
	@echo "  make migrate-sqlite    對本機 SQLite 檔跑 sqlx migrate"
	@echo "  make audit             cargo audit（需已安裝 cargo-audit）"
	@echo "  make secret-scan       gitleaks detect（需已安裝 gitleaks）"
	@echo "  make secret-scan-canary 植入假 key 證明 gitleaks 真的抓得到（§30 Acceptance 1）"
	@echo "  make docker-scan       trivy fs docker/（需已安裝 trivy）"
	@echo "  make run-api           啟動 osint-api（需 JWT_SECRET 與 compose）"
	@echo "  make run-collector     啟動 osint-collector（需 compose 與 .env）"
	@echo "  make run-normalizer    啟動 osint-normalizer（需 compose 與 .env）"
	@echo "  make run-deduplicator  啟動 osint-deduplicator（需 compose 與 .env）"
	@echo "  make run-entity-worker 啟動 osint-entity-worker（需 compose 與 .env）"
	@echo "  make run-indexer       啟動 osint-indexer（需 compose 與 .env）"
	@echo "  make rebuild-index     從 PostgreSQL 補齊 OpenSearch 投影後結束"
	@echo "  make rebuild-index-drop 先刪 index 再從零重建（mapping 有破壞性變更時用）"
	@echo "  make run-graph-worker  啟動 osint-graph-worker（需 compose 與 .env）"
	@echo "  make rebuild-graph     從 PostgreSQL 補齊 Neo4j 圖投影後結束"
	@echo "  make rebuild-graph-drop 先清空 :Entity 再從零重建（Entity 已從 Postgres 刪掉時用）"
	@echo "  make run-embedding-worker 啟動 osint-embedding-worker（需 compose、.env、ml-commons 模型）"
	@echo "  make rebuild-embeddings 從 PostgreSQL 補齊 Document／Entity 向量後結束"
	@echo "  make rebuild-embeddings-drop 先刪 osint-entities 再從零重建（永不刪 osint-documents）"
	@echo "  make run-cli ARGS=...  跑 osint-cli 唯讀查詢，例:make run-cli ARGS=\"documents list\""
	@echo "  make image-build       建八個服務的容器 image（不 push）"
	@echo "  make image-scan        trivy image 掃八個 image（需已安裝 trivy）"
	@echo "  make image-ls          列出本專案的 image 與大小"
	@echo "  make image-prune       清掉 dangling layer 與 builder 快取"
	@echo "  make compose-up-full   基礎建設 + 八個應用服務（會先 build）"
	@echo "  make compose-down-full 停掉含應用服務的整套"
	@echo "  make compose-ps-full   含應用服務的狀態"
	@echo "  make disk              顯示 target/、.git、docker volume 的磁碟用量"
	@echo "  make clean             cargo clean（target/ 會長到數十 GB，定期清）"

# 2026-09-12 實測 target/ 曾長到 67 GB 把磁碟推到 90%。.cargo/config.toml 已關掉
# incremental 並降 debuginfo，全量 debug+test 建置約 2 GB；但每次 Cargo.toml 變動
# 仍會留下舊產物，超過 10 GB 就該 make clean。
disk:
	@echo "target/:"; du -sh target 2>/dev/null || echo "  (無)"
	@echo ".git:"; du -sh .git
	@echo "docker volumes（本專案）:"; docker volume ls --format '{{.Name}}' | grep '^osint-core_' | while read v; do \
		printf "  %s  %s\n" "$$(docker run --rm -v $$v:/v alpine du -sh /v 2>/dev/null | cut -f1)" "$$v"; done
	@echo "磁碟:"; df -h / | tail -1

clean:
	$(CARGO) clean

check:
	$(CARGO) check --workspace --all-targets

build:
	$(CARGO) build --workspace --all-targets

test:
	$(CARGO) test --workspace --all-targets

lint:
	$(CARGO) fmt --all -- --check
	$(CARGO) clippy --workspace --all-targets -- -D warnings

fmt:
	$(CARGO) fmt --all

audit:
	$(CARGO) audit

secret-scan:
	gitleaks detect --source . --no-banner

# SPEC §30 Acceptance 1：證明 secret scanner 不是裝飾品。
# 對 $(TMPDIR) 底下的拋棄式 repo 植入假 key，斷言 gitleaks 抓得到。
# 沒裝 gitleaks 就明確失敗，不假裝通過。GITLEAKS_BIN=/path 可指定路徑。
secret-scan-canary:
	bash scripts/secret-scan-canary.sh

docker-scan:
	trivy fs --scanners vuln,misconfig,secret docker

compose-up:
	$(COMPOSE) $(COMPOSE_FILES) up -d

compose-down:
	$(COMPOSE) $(COMPOSE_FILES) down

compose-ps:
	$(COMPOSE) $(COMPOSE_FILES) ps

# --- 容器化（Phase 7a）--------------------------------------------------
# 八個應用服務在 compose 的 `app` profile 底下，預設不啟動。
# 沒有 --profile app 的目標（compose-up / compose-down / compose-ps）行為不變。

IMAGE_TAG ?= 0.1.0
IMAGE_PREFIX ?= osint-core
SERVICES := osint-api osint-collector osint-normalizer \
	osint-deduplicator osint-entity-worker osint-indexer \
	osint-graph-worker osint-embedding-worker

# 只 build，不啟動。image 名稱固定成 $(IMAGE_PREFIX)/<服務>:$(IMAGE_TAG)，
# 所以重 build 會覆蓋同一個 tag（舊的變成 dangling，用 image-prune 清）。
# **不 push 任何 registry**：V0.1 只需要本機 build + CI build + scan。
image-build:
	$(COMPOSE) $(COMPOSE_FILES) --profile app build

image-ls:
	@docker images --format 'table {{.Repository}}\t{{.Tag}}\t{{.Size}}\t{{.CreatedSince}}' \
		| grep -E 'REPOSITORY|^$(IMAGE_PREFIX)/' || echo "（還沒有 $(IMAGE_PREFIX)/ 的 image，先跑 make image-build）"

# trivy 沒安裝就明確失敗，不要靜默跳過——「掃描通過」跟「根本沒掃」
# 在 CI log 裡長得一模一樣才是真正的風險。
#
# 兩段式，對齊 DEVSECOPS.md §13/§14：
#   1. CRITICAL（且有修補版本）→ exit 1，擋下。
#   2. HIGH → 只列出來供分流（§13：「High findings require explicit triage」），
#      不擋。分流結果寫進 docs/security/VULNERABILITY_TRIAGE.md。
# --ignore-unfixed：上游還沒出修補的 distro 套件擋了也沒有下一步可做，
# 那只會訓練大家去加 .trivyignore。
image-scan:
	@command -v trivy >/dev/null 2>&1 || { \
		echo "找不到 trivy。請先安裝（https://trivy.dev/latest/getting-started/installation/），"; \
		echo "或在 CI 上跑 image-scan job。這個目標不會在沒有掃描器的情況下假裝通過。"; \
		exit 1; }
	@for svc in $(SERVICES); do \
		echo "=== HIGH 分流清單（不擋）：$(IMAGE_PREFIX)/$$svc:$(IMAGE_TAG) ==="; \
		trivy image --scanners vuln --severity HIGH --ignore-unfixed \
			--exit-code 0 "$(IMAGE_PREFIX)/$$svc:$(IMAGE_TAG)" || exit 1; \
	done
	@for svc in $(SERVICES); do \
		echo "=== CRITICAL gate（會擋）：$(IMAGE_PREFIX)/$$svc:$(IMAGE_TAG) ==="; \
		trivy image --scanners vuln --severity CRITICAL --ignore-unfixed \
			--exit-code 1 "$(IMAGE_PREFIX)/$$svc:$(IMAGE_TAG)" || exit 1; \
	done

# build 完一定要跑。multi-stage 的 builder stage 是 buildpack-deps 底的
# rust image（>1.5 GB），加上 BuildKit 的 cargo/target cache mount，
# 不清的話每 build 一輪磁碟就往上跳好幾 GB（CLAUDE.md §15）。
#
# ⚠️ `docker image prune -f` 清的是**全機**的 dangling image。這台工作站上
# 還有 OpenCTI 那套，先確認那邊沒有正在依賴未 tag 的 image 再跑。
image-prune:
	docker image prune -f
	docker builder prune -f
	@df -h / | tail -1

compose-up-full:
	$(COMPOSE) $(COMPOSE_FILES) --profile app up -d --build --wait --wait-timeout 300

compose-down-full:
	$(COMPOSE) $(COMPOSE_FILES) --profile app down

compose-ps-full:
	$(COMPOSE) $(COMPOSE_FILES) --profile app ps

migrate-postgres:
	$(SQLX) migrate run --source migrations/postgres --database-url "$(DATABASE_URL)"

migrate-sqlite:
	mkdir -p var
	$(SQLX) migrate run --source migrations/sqlite --database-url "$(SQLITE_URL)"

run-api:
	$(CARGO) run -p core-api --bin osint-api

run-collector:
	$(CARGO) run -p collector --bin osint-collector

run-normalizer:
	$(CARGO) run -p normalizer --bin osint-normalizer

# SPEC §15 五階段去重。訂閱 object.normalized，寫 DuplicateGroup。
# 設計與已知限制見 docs/developer/deduplicator.md。
run-deduplicator:
	$(CARGO) run -p deduplicator --bin osint-deduplicator

# SPEC §17 entity extraction + §11／§12 relationship/evidence。訂閱 dedup.completed，
# 只處理 canonical（非重複）Document。V0.1 只有確定性規則，**沒有 AI/NER**。
# 抽取規則、public suffix 取捨與已知誤判見 docs/developer/entity-worker.md。
run-entity-worker:
	$(CARGO) run -p entity-worker --bin osint-entity-worker

# SPEC §18 搜尋投影。訂閱 entity.extracted，bulk 寫進 OpenSearch `osint-documents`。
# index mapping、analyzer 取捨、backpressure 與已知限制見 docs/developer/indexer.md。
run-indexer:
	$(CARGO) run -p indexer --bin osint-indexer

# 從 PostgreSQL 補齊投影（既有文件覆寫，**不刪**已經不該存在的文件）。
# PostgreSQL 是 truth，OpenSearch 是可重建的 projection（CLAUDE.md §5）。
rebuild-index:
	$(CARGO) run -p indexer --bin osint-indexer -- --rebuild

# 先刪掉 index 再從零重建。mapping 有破壞性變更（欄位改型別、analyzer 換掉）時
# **必須**用這個——OpenSearch 的 _mapping 只能新增欄位，不能改既有欄位的型別，
# 只跑 rebuild-index 的話舊欄位會繼續用舊型別而且完全不會報錯。
# ⚠️ 重建完成前搜尋會回較少的結果（或空結果）。
rebuild-index-drop:
	$(CARGO) run -p indexer --bin osint-indexer -- --rebuild --drop

# V0.2 Phase 2 §8/§9 圖投影。訂閱 relationship.changed 與 job.dispatched
# （只處理 job_type=graph_rebuild），只投影 Entity→Entity 的邊進 Neo4j。
# 細節、已知限制見 docs/developer/graph-worker.md。
run-graph-worker:
	$(CARGO) run -p graph-worker --bin osint-graph-worker

# 從 PostgreSQL 補齊投影（既有節點/邊覆寫，**不刪**已經不該存在的幽靈節點）。
rebuild-graph:
	$(CARGO) run -p graph-worker --bin osint-graph-worker -- --rebuild

# 先清空所有 :Entity 節點與邊（不碰 :ProjectionState）再從零重建。
# Entity 已從 Postgres 刪除、Neo4j 上還留著幽靈節點時必須用這個。
# ⚠️ 對本機共用 Neo4j 跑這個等於整個圖清空重來；跑之前確認沒有別人在用。
rebuild-graph-drop:
	$(CARGO) run -p graph-worker --bin osint-graph-worker -- --rebuild --drop

# V0.2 Phase 3 §11–§14 向量投影。訂閱 entity.extracted（與 indexer 同一 topic、
# 獨立 consumer group）。Document 向量疊加進 osint-documents；Entity 向量寫進
# osint-entities。推論在 OpenSearch ml-commons，本行程不載模型。
# 細節見 docs/developer/embedding-worker.md。
run-embedding-worker:
	$(CARGO) run -p embedding-worker --bin osint-embedding-worker

# 從 PostgreSQL 補齊向量（略過 find_embedding cache，因為 Postgres 沒存向量本體）。
rebuild-embeddings:
	$(CARGO) run -p embedding-worker --bin osint-embedding-worker -- --rebuild

# 先刪掉 osint-entities 再從零重建。mapping 有破壞性變更時必須用這個。
# **永不**刪 osint-documents——那個 index 是 indexer 的，本服務只疊加向量欄位。
rebuild-embeddings-drop:
	$(CARGO) run -p embedding-worker --bin osint-embedding-worker -- --rebuild --drop

# 本機唯讀查詢工具（直連 DB，不經 core-api）。用法見 docs/user/cli.md。
# ARGS 未給時跑 --help，而不是靜默什麼都不做。
run-cli:
	$(CARGO) run -q -p osint-cli --bin osint-cli -- $(if $(ARGS),$(ARGS),--help)
