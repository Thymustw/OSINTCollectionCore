# OSINT Intelligence Core — 常用開發目標
# audit / secret-scan / docker-scan 依賴尚未納入 bootstrap 的外部工具；
# 目標先寫成直接呼叫，工具未安裝時應失敗而不是假裝通過。

.PHONY: help check build test lint fmt audit secret-scan docker-scan \
	compose-up compose-down compose-ps migrate-postgres migrate-sqlite

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
