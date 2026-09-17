# STIX 2.1 匯入與匯出

## STIX 2.1 是什麼

STIX（Structured Threat Information eXpression）2.1 是資安情報界通用的交換格式。
你可以把其他工具產生的 STIX bundle 匯入到本系統，或把本系統已有的實體匯出成
STIX bundle 分享給其他平台。

## 本系統支援的物件型別

匯入時認識以下 STIX 物件型別：

| STIX 型別 | 條件 | 對應到 Core 的 EntityType |
|---|---|---|
| `identity` | `identity_class = individual` | Person |
| `identity` | `identity_class = organization` | Organization |
| `threat-actor` | — | ThreatActor |
| `malware` | — | Malware |
| `vulnerability` | — | Vulnerability |
| `indicator` | — | Indicator |
| `relationship` | — | Relationship |
| `domain-name` | — | Domain |
| `ipv4-addr` | — | Ip |
| `url` | — | Url |
| `email-addr` | — | Email |

`identity` 其他 `identity_class`（group／class／system 等）、以及 `x-` 開頭的 custom 物件，會被略過而不是回傳錯誤。

## 匯入 STIX Bundle

### 前置作業：建立 Source

每次匯入都要關聯到一個 Source。建一個 `source_type=stix_import` 的 Source：

```bash
curl -s -X POST http://127.0.0.1:18080/api/v1/sources \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "我的 STIX 來源",
    "source_type": "stix_import"
  }'
# 記下回應裡的 id 當作 SOURCE_ID
```

### 發起匯入

```bash
curl -s -X POST http://127.0.0.1:18080/api/v1/import/stix \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "source_id": "'$SOURCE_ID'",
    "bundle": {
      "type": "bundle",
      "id": "bundle--a1b2c3d4-0000-0000-0000-000000000001",
      "objects": [
        {
          "type": "identity",
          "id": "identity--a1b2c3d4-0000-0000-0000-000000000002",
          "name": "APT 範例組織",
          "identity_class": "organization"
        },
        {
          "type": "threat-actor",
          "id": "threat-actor--a1b2c3d4-0000-0000-0000-000000000003",
          "name": "APT-Example"
        },
        {
          "type": "relationship",
          "id": "relationship--a1b2c3d4-0000-0000-0000-000000000004",
          "relationship_type": "attributed-to",
          "source_ref": "threat-actor--a1b2c3d4-0000-0000-0000-000000000003",
          "target_ref": "identity--a1b2c3d4-0000-0000-0000-000000000002"
        }
      ]
    }
  }'
```

成功回 **202**，body 是 `stix_import` Job：

```json
{
  "id": "01935abc-...",
  "job_type": "stix_import",
  "status": "queued",
  ...
}
```

把這個 `id` 存起來，之後可以用 `GET /api/v1/jobs/{id}` 查進度。

### 常見 400 原因

| 情況 | 說明 |
|---|---|
| `source_id` 指到的 Source 不是 `stix_import` 類型 | 換一個正確類型的 Source |
| bundle 超過 50 MiB | 拆成多個小 bundle 分批匯入 |
| bundle 裡的物件數超過 10000 | 拆批 |
| bundle 格式不對（缺 `type`、`id` 不合法等） | 先用 STIX validator 確認格式 |

## 匯出 STIX Bundle

### 發起匯出

```bash
curl -s -X POST http://127.0.0.1:18080/api/v1/export/stix \
  -H "Authorization: Bearer $TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{
    "filter": {
      "entity_types": ["threat_actor", "malware"],
      "time_range": {
        "from": "2026-01-01T00:00:00Z",
        "to": "2026-12-31T23:59:59Z"
      }
    }
  }'
```

成功回 **202**，body 是 `stix_export` Job。把 Job `id` 存起來。

`filter` 裡的四個欄位都可以省略（省略等同「不限」，整表匯出）：

| 欄位 | 說明 |
|---|---|
| `entity_types` | 只匯出這些型別的實體，例如 `["threat_actor","ip"]`。省略代表全部型別 |
| `entity_ids` | 從這些特定實體 id 出發；搭配 `depth` 可以展開鄰居 |
| `time_range` | 只匯出 `first_seen`/`last_seen` 在此區間有交集的實體與關係 |
| `depth` | 從 `entity_ids` 出發，BFS 展開幾層（需要同時提供 `entity_ids`；省略則不展開） |

`entity_type` 的值使用 snake_case：`person`、`organization`、`threat_actor`、`malware`、`vulnerability`、`indicator`、`domain`、`ip`、`url`、`email`。

### 查詢匯出進度

```bash
curl -s http://127.0.0.1:18080/api/v1/jobs/$JOB_ID \
  -H "Authorization: Bearer $TOKEN"
```

`status` 是 `queued` 代表還在等待，`running` 代表正在進行，`completed` 代表完成。

### 取得匯出結果

Job 狀態變成 `completed` 後，用以下端點取得 STIX bundle JSON：

```bash
curl -s http://127.0.0.1:18080/api/v1/jobs/$JOB_ID/result \
  -H "Authorization: Bearer $TOKEN"
```

回傳的就是標準的 STIX 2.1 bundle JSON，可以直接存檔或傳給其他工具。

### `GET /jobs/{id}/result` 的狀態碼

| 碼 | 意思 |
|---|---|
| 200 | 成功，body 是 STIX bundle JSON |
| 404 | Job 不存在，或這個 Job 的 `job_type` 不是 `stix_export` |
| 409 | Job 尚未完成，訊息裡帶目前狀態（例如 `"status": "running"`） |
| 500 | Job 標記為 completed 但結果檔案遺失或格式損毀（請回報） |

## 關於 STIX id 的一件事

**匯出的 STIX id 跟你當初匯入時的不一樣，這是正常的。**

原因：多個 STIX bundle 匯入進來的相同實體（例如同一個 IP），在本系統裡會自動收斂成同一筆記錄。匯出時，這筆記錄會被指定一個由本系統決定的 canonical STIX id，而不是沿用任何一個原始的 STIX id。這確保匯出的 bundle 不會有「同一個實體有兩個 STIX id」的情況。

系統內部確實保留了原始 STIX id 的對照（存成 `namespace="stix"` 的 `EntityIdentifier`），但目前沒有任何 API 或 CLI 指令可以查詢它——這是已知的功能缺口，不是隱藏功能。

## 已知限制

1. **`x-osint-core-entity` 無法反向匯入**：本系統匯出 Account、Hostname、Repository、Hash、Software、Location 這六種型別時，會用自訂的 `x-osint-core-entity` STIX 物件格式，但其他工具通常不認識這個型別。這是單向的：匯出可以，但把含 `x-osint-core-entity` 的 bundle 再匯回本系統，這些物件會被略過。
2. **大量關係的實體可能有遺漏**：單一實體若參與超過 100 條關係，匯出時可能只包含其中一部分。這是查詢效能的設計限制。如果你的資料集有這樣的情況，建議分批匯出或聯絡系統管理員。
