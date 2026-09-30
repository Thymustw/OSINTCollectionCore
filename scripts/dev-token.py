#!/usr/bin/env python3
"""產生一把本機開發用的 JWT，給第一次呼叫 API 用。

為什麼需要這支腳本：所有 /api/v1/* 都要 token，而發 token 的
`POST /api/v1/tokens` 本身要 admin 身分——第一把 token 只能在系統外
用 JWT_SECRET 自己簽。這支腳本就是做這件事。

用法：
    python3 scripts/dev-token.py                 # admin，1 小時
    python3 scripts/dev-token.py --role operator
    python3 scripts/dev-token.py --ttl 7200
    export TOKEN=$(python3 scripts/dev-token.py)

secret 的來源（依序）：--secret 參數 → 環境變數 JWT_SECRET →
docker-compose.yml 的本機開發預設值。

⚠️ 只給本機開發／測試用。正式環境的 JWT_SECRET 必須從 secret 管理機制帶入，
   不要用這裡的預設值，也不要把正式 secret 寫進指令歷史。
   長期給服務用的憑證請用 admin 身分呼叫 POST /api/v1/tokens 發 API token。
"""
import argparse
import os
import sys
import time
import uuid

# 與 docker/docker-compose.yml 的 JWT_SECRET 預設值一致（只適用本機／CI）。
DEV_DEFAULT_SECRET = "osint-dev-only-jwt-secret-32bytes-min"
# 與 config/default.toml 的 [auth].jwt_issuer 一致。不一致的話 API 會回 401。
ISSUER = "osint-core"

try:
    import jwt
except ImportError:
    sys.exit(
        "找不到 PyJWT。請先安裝：\n"
        "  pip install pyjwt\n"
        "（Debian/Ubuntu 也可以用 sudo apt install python3-jwt）"
    )


def main() -> None:
    p = argparse.ArgumentParser(description="產生本機開發用的 JWT")
    p.add_argument("--role", choices=["viewer", "operator", "admin"], default="admin",
                   help="viewer 只能讀、operator 可以寫、admin 可以發 token（預設 admin）")
    p.add_argument("--ttl", type=int, default=3600, help="有效秒數（預設 3600）")
    p.add_argument("--subject", default="local-dev", help="token 的使用者名稱，會記進稽核紀錄")
    p.add_argument("--secret", help="簽章用的 secret（預設讀 JWT_SECRET 環境變數）")
    args = p.parse_args()

    secret = args.secret or os.environ.get("JWT_SECRET") or DEV_DEFAULT_SECRET
    if len(secret) < 32:
        sys.exit(f"secret 只有 {len(secret)} 個字元，API 要求至少 32 個，簽出來的 token 會被拒絕。")

    now = int(time.time())
    token = jwt.encode(
        {
            "sub": args.subject,
            "role": args.role,  # 必須是小寫：viewer / operator / admin
            "iss": ISSUER,
            "iat": now,
            "exp": now + args.ttl,
            "jti": str(uuid.uuid4()),
        },
        secret,
        algorithm="HS256",
    )
    print(token)


if __name__ == "__main__":
    main()
