#!/usr/bin/env bash
# 下載 e5（intfloat/multilingual-e5-small）的 tokenizer.json 並驗 sha256。
#
# 段落切分（crates/chunker）用它算 token 數；檔案 17 MB、上游公開，不進 repo。
# 用法：scripts/fetch-e5-tokenizer.sh [目的路徑]   （預設 models/e5-small/tokenizer.json）
#
# hash 與 scripts/opensearch-ml-setup-e5.sh 的 TOKENIZER_SHA256、
# crates/chunker/src/tokenizer.rs 的 E5_TOKENIZER_SHA256 是同一顆，三處要一起改。
set -euo pipefail

DEST="${1:-models/e5-small/tokenizer.json}"
URL="https://huggingface.co/intfloat/multilingual-e5-small/resolve/main/tokenizer.json"
WANT="0b44a9d7b51c3c62626640cda0e2c2f70fdacdc25bbbd68038369d14ebdf4c39" # gitleaks:allow

sha_of() { sha256sum "$1" | cut -d' ' -f1; }

if [[ -f "$DEST" && "$(sha_of "$DEST")" == "$WANT" ]]; then
  echo "== 已存在且 sha256 相符：$DEST"
  exit 0
fi

mkdir -p "$(dirname "$DEST")"
tmp="$(mktemp "${DEST}.XXXXXX")"
trap 'rm -f "$tmp"' EXIT
curl -sSL --fail -o "$tmp" "$URL"
got="$(sha_of "$tmp")"
if [[ "$got" != "$WANT" ]]; then
  echo "❌ tokenizer.json sha256 不符：預期 $WANT，實際 $got。不要改 hash 繞過，先確認上游 commit 記錄。" >&2
  exit 1
fi
mv "$tmp" "$DEST"
trap - EXIT
echo "== 下載完成，sha256 驗證通過：$DEST"
