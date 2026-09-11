# OSINT Intelligence Core — 常用開發目標
# audit / secret-scan / docker-scan 依賴尚未納入 bootstrap 的外部工具；
# 目標先寫成直接呼叫，工具未安裝時應失敗而不是假裝通過。

.PHONY: help check build test lint fmt audit secret-scan docker-scan \
	compose-up compose-down compose-ps migrate-postgres migrate-sqlite \
	run-api run-collector run-normalizer run-deduplicator run-entity-worker run-cli

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
	@echo "  make run-cli ARGS=...  跑 osint-cli 唯讀查詢，例:make run-cli ARGS=\"documents list\""

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

# 本機唯讀查詢工具（直連 DB，不經 core-api）。用法見 docs/user/cli.md。
# ARGS 未給時跑 --help，而不是靜默什麼都不做。
run-cli:
	$(CARGO) run -q -p osint-cli --bin osint-cli -- $(if $(ARGS),$(ARGS),--help)
