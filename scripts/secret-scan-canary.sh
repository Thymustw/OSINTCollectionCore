#!/usr/bin/env bash
# secret-scan-canary.sh — 證明 secret scanner 真的會抓東西，而不是裝飾品。
#
# SPEC_V0.1.md §30 Acceptance 1「committed secret detection is tested」。
#
# 問題意識:一個永遠回報「clean」的 secret scanner，跟一個真的有在掃的 scanner，
# 在 CI log 裡長得一模一樣（CLAUDE.md「不報錯不等於正常」）。規則檔寫壞、
# binary 抓錯版本、參數打錯路徑，結果都是 exit 0 —— 綠燈，但零防護。
#
# 做法:在暫存目錄另外開一個拋棄式 git repo，塞一個「一定要被抓到」的假 secret，
# 然後**斷言 gitleaks 回傳非零**。抓不到就代表掃描器壞了，這支腳本會失敗。
#
# ⚠️ canary 字串刻意拆成 'AKIA' + 後 16 碼組合，**不以完整形式出現在任何
#    committed 檔案裡**。否則本 repo 真正的 gitleaks 掃描（與 GitHub 的
#    secret scanning）會抓到它自己，變成每次 CI 都紅燈。
#    要驗證這件事:  git log -p | grep -c 'AKIAZXCVBNMA'   必須是 0。
#
# ⚠️ 不要用 AWS 官方文件的範例 key（AKIA + IOSFODNN7EXAMPLE）當 canary。
#    2026-09-12 實測 gitleaks 8.28.0 對它回 exit 0「no leaks found」——內建
#    allowlist 刻意排除了那個舉世皆知的範例值（否則所有引用 AWS 文件的 repo
#    都會誤報）。用它當 canary 會讓這支腳本永遠判定「掃描器壞了」，是假紅燈。
#    這裡用的是 AKIA + 鍵盤序列，格式合法、明顯是假的、不在 allowlist 內。
#    同一版本實測：exit 1、leaks found: 1。
#
# 環境變數:
#   GITLEAKS_BIN  gitleaks 執行檔路徑（預設找 PATH 上的 gitleaks）
#   TMPDIR        暫存 repo 的位置。CI 上設成 $RUNNER_TEMP，確保不落在 repo 內。

set -euo pipefail

GITLEAKS_BIN="${GITLEAKS_BIN:-gitleaks}"

if ! command -v "$GITLEAKS_BIN" >/dev/null 2>&1; then
  echo "錯誤:找不到 gitleaks（GITLEAKS_BIN=$GITLEAKS_BIN）。" >&2
  echo "請先安裝 https://github.com/gitleaks/gitleaks/releases ，" >&2
  echo "或用 GITLEAKS_BIN=/path/to/gitleaks make secret-scan-canary 指定路徑。" >&2
  echo "這個目標不會在沒有掃描器的情況下假裝通過。" >&2
  exit 1
fi

# 別讓外部 config 影響判斷——要驗的是 gitleaks 的**內建規則**抓不抓得到。
unset GITLEAKS_CONFIG GITLEAKS_CONFIG_TOML

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/gitleaks-canary.XXXXXXXX")"
cleanup() { rm -rf "$WORKDIR"; }
trap cleanup EXIT

git -C "$WORKDIR" init -q
git -C "$WORKDIR" config user.email "canary@example.invalid"
git -C "$WORKDIR" config user.name "secret scan canary"
git -C "$WORKDIR" config commit.gpgsign false

# 字串在此組合，原始碼裡沒有完整的 key。
printf 'aws_access_key_id = "AKIA%s"\n' 'ZXCVBNMASDFGHJKL' > "$WORKDIR/canary.tf"

git -C "$WORKDIR" add canary.tf
git -C "$WORKDIR" commit -q -m "canary: known fake AWS key"

echo "== secret-scan canary:對暫存 repo $WORKDIR 跑 gitleaks =="
set +e
"$GITLEAKS_BIN" detect --source "$WORKDIR" --gitleaks-ignore-path "$WORKDIR" --no-banner
rc=$?
set -e

case "$rc" in
  1)
    echo "✅ canary 通過:gitleaks 偵測到植入的假 AWS key（exit $rc）。掃描器有在運作。"
    ;;
  0)
    echo "❌ secret scanner 沒有偵測到已知的 canary，掃描器可能壞了。" >&2
    echo "   已知一定會被抓的假 AWS access key 被 commit 進暫存 repo，" >&2
    echo "   gitleaks 卻回報 clean（exit 0）。這代表 secret-scan 這一關目前是裝飾品:" >&2
    echo "   真的有人 commit 憑證也不會被擋下來。" >&2
    echo "   請檢查:gitleaks 版本／內建規則是否被 config 覆寫／--source 路徑是否正確。" >&2
    exit 1
    ;;
  *)
    echo "❌ gitleaks 執行失敗（exit $rc），不是「有找到／沒找到」的結果。" >&2
    echo "   canary 無法判定掃描器是否正常，視同失敗。請看上面的 gitleaks 輸出。" >&2
    exit 1
    ;;
esac
