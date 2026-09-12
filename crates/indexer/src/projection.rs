//! Document（+ entities + 溯源欄位）→ OpenSearch `_source`。
//!
//! # PostgreSQL 是 truth，這裡產生的是 projection
//!
//! 這個模組只做「把 canonical 資料攤平成可搜尋的形狀」，不做任何判斷。
//! 任何只存在於 index 而不存在於 PostgreSQL 的資訊，都會在 `--rebuild` 後消失——
//! 所以不要在這裡計算會被當成事實的東西。

use chrono::{DateTime, Utc};
use core_model::{Document, Entity};
use serde_json::{Map, Value, json};
use storage_core::SearchDocument;
use uuid::Uuid;

use crate::schema;

/// 寫進 index 的 entity 摘要。刻意不放 `description`／`attributes`：
/// 那些會讓每份文件的 `_source` 膨脹好幾倍，而搜尋用不到。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntity {
    pub entity_id: Uuid,
    pub entity_type: String,
    pub name: String,
    pub normalized_name: String,
}

impl IndexEntity {
    #[must_use]
    pub fn from_entity(entity: &Entity) -> Self {
        Self {
            entity_id: entity.id,
            // 用 serde 的字串（snake_case），與 entity-worker 寫進資料庫、
            // 以及 API 請求裡的 `entity.type` 是同一套。用 Debug 會變成 `Vulnerability`，
            // 於是 API 傳來的 `vulnerability` 永遠比不中，而且不會報錯。
            entity_type: serde_json::to_value(entity.entity_type)
                .ok()
                .and_then(|v| v.as_str().map(str::to_string))
                .unwrap_or_else(|| format!("{:?}", entity.entity_type).to_lowercase()),
            name: entity.name.clone(),
            normalized_name: entity.normalized_name.clone(),
        }
    }
}

/// 建立投影時要補進去、但不在 `Document` 上的欄位。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Provenance {
    pub raw_evidence_id: Option<Uuid>,
    pub source_id: Option<Uuid>,
    pub connector_id: Option<Uuid>,
}

/// 單一欄位寫進 index 的 byte 上限。
///
/// `documents.body` 沒有長度限制且內容來自外部（CLAUDE.md §5：external content is
/// untrusted）。不設限的話一份 50 MB 的正文會整份進 `_source`，
/// 每次搜尋回傳它、每次 rebuild 重送它。
pub const DEFAULT_MAX_FIELD_BYTES: usize = 256 * 1024;

/// 把一份 Document 攤平成 `_source`。
#[must_use]
pub fn build_body(
    document: &Document,
    provenance: Provenance,
    entities: &[IndexEntity],
    indexed_at: DateTime<Utc>,
    max_field_bytes: usize,
) -> Value {
    let mut map = Map::new();
    map.insert(schema::F_DOCUMENT_ID.into(), json!(document.id.to_string()));
    map.insert(
        schema::F_OBJECT_TYPE.into(),
        serde_json::to_value(document.object_type).unwrap_or(Value::Null),
    );
    map.insert("schema_version".into(), json!(document.schema_version));

    insert_text(
        &mut map,
        schema::F_TITLE,
        document.title.as_deref(),
        max_field_bytes,
    );
    insert_text(
        &mut map,
        schema::F_SUMMARY,
        document.summary.as_deref(),
        max_field_bytes,
    );
    insert_text(
        &mut map,
        schema::F_BODY,
        document.body.as_deref(),
        max_field_bytes,
    );
    insert_text(&mut map, "author", document.author.as_deref(), 1024);

    map.insert(schema::F_LANGUAGE.into(), json!(document.language));
    map.insert("labels".into(), json!(document.labels));
    map.insert(
        schema::F_SOURCE_ID.into(),
        json!(provenance.source_id.map(|id| id.to_string())),
    );
    map.insert(
        schema::F_CONNECTOR_ID.into(),
        json!(provenance.connector_id.map(|id| id.to_string())),
    );
    map.insert(
        schema::F_RAW_EVIDENCE_ID.into(),
        json!(provenance.raw_evidence_id.map(|id| id.to_string())),
    );
    map.insert("canonical_url".into(), json!(document.canonical_url));
    map.insert("source_url".into(), json!(document.source_url));

    map.insert(schema::F_PUBLISHED_AT.into(), json!(document.published_at));
    map.insert(schema::F_OBSERVED_AT.into(), json!(document.observed_at));
    map.insert(schema::F_COLLECTED_AT.into(), json!(document.collected_at));
    map.insert(
        schema::F_EFFECTIVE_DATE.into(),
        json!(effective_date(document)),
    );
    map.insert("indexed_at".into(), json!(indexed_at));

    map.insert(
        schema::F_DUPLICATE_OF.into(),
        json!(document.duplicate_of.map(|id| id.to_string())),
    );
    map.insert("confidence".into(), json!(document.confidence));

    map.insert(
        schema::F_ENTITIES.into(),
        Value::Array(
            entities
                .iter()
                .map(|entity| {
                    json!({
                        "entity_id": entity.entity_id.to_string(),
                        "entity_type": entity.entity_type,
                        "name": entity.name,
                        "normalized_name": entity.normalized_name,
                    })
                })
                .collect(),
        ),
    );
    map.insert("entity_count".into(), json!(entities.len()));

    Value::Object(map)
}

/// 從投影 `_source` 取回「這一份來源物件的時間戳」，給 projection checkpoint 算 lag 用。
///
/// # 為什麼是 `collected_at` 而不是 `effective_date` / `published_at`
///
/// lag 要回答的是「投影落後 canonical store 多久」。`published_at` 是**外部來源**
/// 宣稱的發布時間：匯入一篇 2019 年的文章時它是 2019 年，lag 會變成七年——
/// 那個數字與投影健不健康完全無關，只會讓門檻永遠是紅的。
/// `collected_at` 是本系統取得它的時間，是 `Document` 上最接近「何時進入 pipeline」
/// 的欄位（`Document` **沒有** `updated_at`，見 `core_model::Document`）。
///
/// 讀不到（欄位被改名、或投影是舊版寫的）時回 `None`，checkpoint 就不會前進——
/// 寧可讓 lag 停在 `None`（看得出來不對）也不要塞一個現在的時間戳假裝沒落後。
#[must_use]
pub fn source_timestamp(body: &Value) -> Option<DateTime<Utc>> {
    body.get(schema::F_COLLECTED_AT)
        .and_then(Value::as_str)
        .and_then(|raw| DateTime::parse_from_rfc3339(raw).ok())
        .map(|at| at.with_timezone(&Utc))
}

/// `published_at` 有值時用它，否則退回 `observed_at`。
///
/// 這一欄是**冗餘**的（兩個來源欄位都在 index 裡），存在的理由是
/// 「沒有發布時間的文件也要能被日期區間查到」。用 script query 在查詢時算 coalesce
/// 每次查詢都要對每份候選文件跑一次腳本；寫進 index 是一次性成本。
#[must_use]
pub fn effective_date(document: &Document) -> DateTime<Utc> {
    document.published_at.unwrap_or(document.observed_at)
}

fn insert_text(map: &mut Map<String, Value>, key: &str, value: Option<&str>, max_bytes: usize) {
    let value = value.map(|text| truncate_on_char_boundary(text, max_bytes).to_string());
    map.insert(key.to_string(), json!(value));
}

/// 截到最多 `max_bytes`，但不切破 UTF-8 字元。
fn truncate_on_char_boundary(text: &str, max_bytes: usize) -> &str {
    if text.len() <= max_bytes {
        return text;
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// 組出可直接送進 [`storage_core::SearchStore`] 的一筆文件。
///
/// **`_id` 一律是 `Document.id`。** 這就是冪等的全部：OpenSearch 的 index 動作
/// 對既有 `_id` 是覆寫（`_version` +1），不會產生第二筆 hit。同一則
/// `entity.extracted` 重送一百次，index 裡還是同一份文件。
#[must_use]
pub fn build_search_document(
    index: &str,
    document: &Document,
    provenance: Provenance,
    entities: &[IndexEntity],
    indexed_at: DateTime<Utc>,
    max_field_bytes: usize,
) -> SearchDocument {
    SearchDocument {
        index: index.to_string(),
        id: document.id.to_string(),
        body: build_body(document, provenance, entities, indexed_at, max_field_bytes),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_model::{DocumentType, EntityType};

    fn document() -> Document {
        Document {
            id: Uuid::now_v7(),
            object_type: DocumentType::Article,
            schema_version: "1".into(),
            title: Some("勒索軟體攻擊公告".into()),
            body: Some("正文 body".into()),
            summary: Some("摘要".into()),
            language: Some("zh".into()),
            author: Some("Bob".into()),
            published_at: None,
            modified_at: None,
            observed_at: Utc::now(),
            collected_at: Utc::now(),
            source_url: Some("https://example.invalid/a?utm_source=x".into()),
            canonical_url: Some("https://example.invalid/a".into()),
            normalized_content_hash: None,
            confidence: 0.8,
            labels: vec!["cti".into()],
            attributes: json!({}),
            external_key: None,
            simhash: None,
            duplicate_of: None,
        }
    }

    fn entity() -> Entity {
        Entity {
            id: Uuid::now_v7(),
            entity_type: EntityType::Vulnerability,
            name: "CVE-2026-0001".into(),
            normalized_name: "CVE-2026-0001".into(),
            description: None,
            confidence: 1.0,
            first_seen: Utc::now(),
            last_seen: Utc::now(),
            merged_into: None,
            attributes: json!({}),
        }
    }

    #[test]
    fn document_id_is_the_opensearch_id() {
        let doc = document();
        let search = build_search_document(
            "idx",
            &doc,
            Provenance::default(),
            &[],
            Utc::now(),
            DEFAULT_MAX_FIELD_BYTES,
        );
        assert_eq!(
            search.id,
            doc.id.to_string(),
            "_id 不是 Document.id 的話，重送事件會產生第二筆 hit"
        );
    }

    #[test]
    fn entity_type_uses_serde_name_not_debug() {
        let indexed = IndexEntity::from_entity(&entity());
        assert_eq!(
            indexed.entity_type, "vulnerability",
            "Debug 會給 `Vulnerability`，API 傳來的小寫值就永遠比不中，而且不會報錯"
        );
    }

    #[test]
    fn effective_date_falls_back_to_observed_at() {
        let mut doc = document();
        assert_eq!(effective_date(&doc), doc.observed_at);
        let published = Utc::now() - chrono::Duration::days(3);
        doc.published_at = Some(published);
        assert_eq!(effective_date(&doc), published);
    }

    #[test]
    fn source_timestamp_reads_back_what_the_projection_wrote() {
        // 這一條在防欄位改名：`source_timestamp` 讀不到就回 None，
        // checkpoint 於是永遠不前進、lag 永遠是 None——不會報錯，也不會有人發現。
        let doc = document();
        let body = build_body(&doc, Provenance::default(), &[], Utc::now(), 4096);
        assert_eq!(
            source_timestamp(&body),
            Some(doc.collected_at),
            "投影寫的 collected_at 必須被 source_timestamp 讀得回來"
        );
    }

    #[test]
    fn source_timestamp_of_an_unrelated_body_is_none() {
        assert_eq!(source_timestamp(&json!({"title": "x"})), None);
    }

    #[test]
    fn body_is_truncated_on_char_boundary() {
        let mut doc = document();
        doc.body = Some("勒".repeat(1000));
        let body = build_body(&doc, Provenance::default(), &[], Utc::now(), 64);
        let indexed = body.get("body").and_then(Value::as_str).unwrap();
        assert!(indexed.len() <= 64);
        // 「勒」是 3 byte。切在 byte 64 會切破字元；正確行為是退回 63。
        assert_eq!(indexed.chars().count(), 21);
    }

    #[test]
    fn provenance_fields_are_present_for_acceptance_e() {
        let raw = Uuid::now_v7();
        let source = Uuid::now_v7();
        let connector = Uuid::now_v7();
        let body = build_body(
            &document(),
            Provenance {
                raw_evidence_id: Some(raw),
                source_id: Some(source),
                connector_id: Some(connector),
            },
            &[],
            Utc::now(),
            DEFAULT_MAX_FIELD_BYTES,
        );
        assert_eq!(
            body.get("raw_evidence_id").and_then(Value::as_str),
            Some(raw.to_string().as_str()),
            "Acceptance E 從 search result 開始回溯，少了這一欄整條鏈就斷在第一步"
        );
        assert_eq!(
            body.get("source_id").and_then(Value::as_str),
            Some(source.to_string().as_str())
        );
        assert_eq!(
            body.get("connector_id").and_then(Value::as_str),
            Some(connector.to_string().as_str())
        );
    }

    #[test]
    fn entities_are_flattened_into_nested_objects() {
        let indexed = [IndexEntity::from_entity(&entity())];
        let body = build_body(
            &document(),
            Provenance::default(),
            &indexed,
            Utc::now(),
            DEFAULT_MAX_FIELD_BYTES,
        );
        assert_eq!(
            body.pointer("/entities/0/normalized_name")
                .and_then(Value::as_str),
            Some("CVE-2026-0001")
        );
        assert_eq!(body.get("entity_count").and_then(Value::as_u64), Some(1));
    }

    #[test]
    fn body_keys_are_all_declared_in_the_mapping() {
        // mapping 是 dynamic:strict。多一個沒宣告的欄位會讓**整批** bulk 被拒，
        // 而且只在對真 OpenSearch 跑時才看得到。這個測試把它擋在單元層級。
        let body = build_body(
            &document(),
            Provenance::default(),
            &[IndexEntity::from_entity(&entity())],
            Utc::now(),
            DEFAULT_MAX_FIELD_BYTES,
        );
        let mappings = schema::index_mappings();
        let declared = mappings
            .pointer("/properties")
            .and_then(Value::as_object)
            .expect("mapping properties");
        for key in body.as_object().expect("body 是 object").keys() {
            assert!(
                declared.contains_key(key),
                "`{key}` 沒有宣告在 index mapping 裡，dynamic:strict 會讓整批寫入失敗"
            );
        }
        // nested entity 的欄位也要對齊。
        let entity_props = mappings
            .pointer("/properties/entities/properties")
            .and_then(Value::as_object)
            .expect("entities properties");
        let first = body
            .pointer("/entities/0")
            .and_then(Value::as_object)
            .expect("第一個 entity");
        for key in first.keys() {
            assert!(
                entity_props.contains_key(key),
                "entities.`{key}` 沒有宣告在 mapping 裡"
            );
        }
    }
}
