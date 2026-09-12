# OSINT Intelligence Core — 常用開發目標
# audit / secret-scan / docker-scan 依賴尚未納入 bootstrap 的外部工具；
# 目標先寫成直接呼叫，工具未安裝時應失敗而不是假裝通過。

.PHONY: help check build test lint fmt audit secret-scan docker-scan \
	compose-up compose-down compose-ps migrate-postgres migrate-sqlite \
	run-api run-collector run-normalizer run-deduplicator run-entity-worker \
	run-indexer rebuild-index rebuild-index-drop run-cli \
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
	@echo "  make docker-scan       trivy fs docker/（需已安裝 trivy）"
	@echo "  make run-api           啟動 osint-api（需 JWT_SECRET 與 compose）"
	@echo "  make run-collector     啟動 osint-collector（需 compose 與 .env）"
	@echo "  make run-normalizer    啟動 osint-normalizer（需 compose 與 .env）"
	@echo "  make run-deduplicator  啟動 osint-deduplicator（需 compose 與 .env）"
	@echo "  make run-entity-worker 啟動 osint-entity-worker（需 compose 與 .env）"
	@echo "  make run-indexer       啟動 osint-indexer（需 compose 與 .env）"
	@echo "  make rebuild-index     從 PostgreSQL 補齊 OpenSearch 投影後結束"
	@echo "  make rebuild-index-drop 先刪 index 再從零重建（mapping 有破壞性變更時用）"
	@echo "  make run-cli ARGS=...  跑 osint-cli 唯讀查詢，例:make run-cli ARGS=\"documents list\""
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

docker-scan:
	trivy fs --scanners vuln,misconfig,secret docker

compose-up:
	$(COMPOSE) $(COMPOSE_FILES) up -d

compose-down:
	$(COMPOSE) $(COMPOSE_FILES) down

compose-ps:
	$(COMPOSE) $(COMPOSE_FILES) ps

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

# 本機唯讀查詢工具（直連 DB，不經 core-api）。用法見 docs/user/cli.md。
# ARGS 未給時跑 --help，而不是靜默什麼都不做。
run-cli:
	$(CARGO) run -q -p osint-cli --bin osint-cli -- $(if $(ARGS),$(ARGS),--help)
