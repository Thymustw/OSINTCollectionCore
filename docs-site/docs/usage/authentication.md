# 取得 API 憑證

這一頁說明兩種 API 憑證的差異、怎麼取得，以及如何管理長期憑證的生命週期。

---

## 兩種憑證

所有 `/api/v1/*` 端點都需要在 `Authorization: Bearer <token>` 帶憑證。有兩種格式：

| | JWT | API token |
|---|---|---|
| 格式 | `eyJhbGci…` | `osint_<uuid>.<secret>` |
| 發行者 | 用 `JWT_SECRET` 自己簽（`dev-token.py`） | `POST /api/v1/tokens`（需 admin） |
| 有效期 | 短期（預設 1 小時） | 長期，可撤銷 |
| 適合 | 人工互動、本機測試 | 服務帳號、CI、腳本 |

---

## 角色與權限

系統有三個固定角色：

| 角色 | 能做什麼 |
|---|---|
| `viewer` | 讀取所有資料（文件、實體、原始證據、搜尋） |
| `operator` | viewer 的所有操作，加上：建立 Source、上傳匯入、發現、圖查詢 |
| `admin` | operator 的所有操作，加上：發行、列出、撤銷 API token |

角色是**嚴格超集**：operator 一定包含 viewer，admin 一定包含 operator。

---

## 本機開發：dev-token.py

本機開發最快的做法是用內附的腳本產生短期 JWT：

```bash
export TOKEN=$(python3 scripts/dev-token.py)
export API=http://127.0.0.1:18080/api/v1
```

確認有效：

```bash
curl -s $API/whoami -H "Authorization: Bearer $TOKEN"
```

```json
{"auth_method":"Jwt","role":"admin","subject":"local-dev"}
```

腳本支援幾個選項：

```bash
python3 scripts/dev-token.py --role operator  # 指定角色（預設 admin）
python3 scripts/dev-token.py --ttl 7200       # 指定有效秒數（預設 3600）
python3 scripts/dev-token.py --subject ci-bot # 指定識別名稱（會出現在稽核紀錄）
```

!!! warning "這把憑證只給本機開發用"
    它是用 compose 預設的開發密鑰簽的（`osint-dev-only-jwt-secret-32bytes-min`）。
    到期就重跑上面的 `export TOKEN=...`。
    正式環境絕對不能用這個密鑰，見下方「正式環境注意事項」。

---

## 發行長期 API token

給服務、CI 或腳本用的長期憑證，用 admin 身分呼叫 `POST /api/v1/tokens`：

```bash
curl -s -X POST $API/tokens \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"ci-importer","role":"operator","expires_in_days":30}'
```

```json
{
  "id": "00000000-0000-0000-0000-000000000000",
  "name": "ci-importer",
  "role": "operator",
  "created_by": "local-dev",
  "created_at": "2026-09-30T06:28:25.776688241Z",
  "expires_at": "2026-10-30T06:28:25.712614872Z",
  "token": "osint_00000000-0000-0000-0000-000000000000.EXAMPLE-SECRET-DO-NOT-USE...",
  "message": "請立刻保存 token：伺服器只留 argon2 雜湊，這個明文不會再出現第二次。..."
}
```

!!! danger "明文只出現這一次"
    回應裡的 `token` 欄位是**唯一**能看到明文的時機。伺服器只留 argon2id 雜湊，
    「再顯示一次」在結構上不可能。弄丟了就撤銷重發。

### 請求欄位說明

| 欄位 | 必填 | 說明 |
|---|---|---|
| `name` | 是 | 1–64 字元，不可含控制字元。會出現在稽核紀錄，請填看得出用途的名稱，例如 `ci-importer` |
| `role` | 是 | `viewer` / `operator` / `admin` |
| `expires_in_days` | 否 | 1–365。省略或填 `null` 代表不會自動到期（只能撤銷）；這個選擇會被稽核記下來 |

---

## 列出 API token

```bash
curl -s $API/tokens -H "Authorization: Bearer $TOKEN"
```

```json
{
  "items": [
    {
      "id": "00000000-0000-0000-0000-000000000000",
      "name": "ci-importer",
      "role": "operator",
      "created_by": "local-dev",
      "created_at": "2026-09-30T06:28:25.776688241Z",
      "expires_at": "2026-10-30T06:28:25.712614872Z",
      "last_used_at": null,
      "revoked_at": null,
      "active": true
    }
  ]
}
```

清單包含**已撤銷的**——撤銷紀錄本身就是要看的東西（「這把 token 是誰、在什麼時候撤銷的」）。

`active` 欄位是便利欄：等同 `revoked_at == null` 且有效期未到。

---

## 撤銷 API token

```bash
# TOKEN_ID 取自上方 GET /api/v1/tokens 或發行時的回應
curl -s -X DELETE $API/tokens/$TOKEN_ID \
  -H "Authorization: Bearer $TOKEN"
```

成功回 `204 No Content`。撤銷立刻生效——該 token 下一次打 API 會得到 401：

```json
{"error":"unauthorized","message":"API token 已撤銷。請改用新 token"}
```

找不到這個 id 會回 `404`（不是靜默成功）。

---

## 用 API token 確認身分

```bash
SERVICE_TOKEN="osint_00000000-0000-0000-0000-000000000000.EXAMPLE-SECRET-DO-NOT-USE..."
curl -s $API/whoami -H "Authorization: Bearer $SERVICE_TOKEN"
```

```json
{"auth_method":"ApiToken","role":"operator","subject":"token:ci-importer"}
```

`auth_method` 為 `ApiToken`，`subject` 為 `token:<name>`（名稱就是發行時填的 `name`）。

---

## 正式環境注意事項

`JWT_SECRET` 必須從 secret 管理機制帶入，不能使用開發預設值：

```bash
# 正確：從環境變數讀（SecretRef）
jwt_secret_ref = "env:JWT_SECRET"

# 錯誤：把 secret 寫進設定檔
# jwt_secret = "my-secret"    ← 這樣做會進版本庫
```

!!! warning "開發預設值只能在本機用"
    `config/default.toml` 指向 `env:JWT_SECRET`。compose 開發環境的預設值
    （`osint-dev-only-jwt-secret-32bytes-min`）只適用本機。
    正式環境請用 Vault、AWS Secrets Manager、Kubernetes Secret 等機制注入真正的密鑰，
    長度至少 32 字元。

---

## 常見錯誤

| 錯誤碼 | 意思 | 解法 |
|---|---|---|
| `401 unauthorized` | 沒帶 token，或 token 無效／已過期／已撤銷 | 重新取得或重發新 token |
| `403 forbidden` | 角色不夠（viewer 想寫） | 改用 operator 以上的憑證 |
| `400 bad_request` | `name` 欄位空白或超過 64 字元 | 填合法的名稱 |
| `400 bad_request` | `expires_in_days` 不在 1–365 之間 | 調整天數，或省略欄位（不會自動到期） |
