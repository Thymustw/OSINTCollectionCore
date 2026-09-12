//! `/api/v1/objects`（SPEC §9／§19）。V0.1 的 canonical object 就是 Document。
//!
//! # `POST /objects` 回 501
//!
//! 這是**刻意的**，不是還沒做。直接 POST 一份 Document 會產生沒有 RawEvidence
//! 祖先的孤兒，違反 SPEC §14 的最低可追溯鏈
//! （Search Result → Canonical Object → Raw Evidence → Source → Connector）。
//! 要推資料進來請用 `POST /api/v1/import`，它走的是「先落地成不可變的
//! RawEvidence，再由 normalizer 產生 Document」的正確路徑。
//! 完整理由見 `docs/adr/ADR-006-no-direct-object-post.md`。
//!
//! # 預設排除重複
//!
//! `GET /objects` 預設不含 `duplicate_of` 有值的文件（SPEC §15／§16）。
//! 一篇報導被十個站轉載時，預設列表應該是一筆而不是十一筆。
//! 要看全部傳 `?include_duplicates=true`。

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use core_model::{
    Document, DocumentType, DuplicateGroup, Entity, EntityExtraction, Provenance, RawEvidence,
};
use core_security::{Permission, Principal};

use crate::error::ApiError;
use crate::extractors::ClientIp;
use crate::pagination::{CursorPage, Pagination};
use crate::resources::{
    AUDIT_OBJECT_CREATE, AuditEvent, LINK_PAGE, audit, rejected_metadata, storage_error, store,
};
use crate::state::AppState;

const RESOURCE: &str = "object";

#[derive(Debug, Clone, Deserialize)]
pub struct ListQuery {
    pub cursor: Option<String>,
    pub limit: Option<u32>,
    /// `article`／`web_page`／`post`／`message`／`report`／`file`／`advisory`。
    pub object_type: Option<DocumentType>,
    /// 預設 false：列表不含被判為重複的文件。
    #[serde(default)]
    pub include_duplicates: bool,
}

/// `GET /objects/{id}` 的明細。
///
/// 把「Document ← provenance ← RawEvidence」與去重、抽取結果串成一份。
/// 與 `osint-cli documents show` 是同一組查詢（同樣的 store 方法、同樣的上限），
/// 兩邊看到的東西必須一致——不一致會讓人以為 CLI 或 API 其中一邊漏資料。
#[derive(Debug, Clone, Serialize)]
pub struct ObjectDetail {
    #[serde(flatten)]
    pub document: Document,
    /// SPEC §14 的可追溯鏈：以這份 Document 為 subject 的溯源列（由舊到新）。
    pub provenance: Vec<Provenance>,
    /// provenance 指到的 RawEvidence（去重後）。鏈的下一站。
    pub raw_evidence: Vec<RawEvidence>,
    /// 這份被判成重複時，它所屬的 duplicate group（帶命中階段與相似度）。
    pub duplicate_group: Option<DuplicateGroup>,
    /// 這份是 canonical 時，指向它的 duplicate group。
    pub duplicates: Vec<DuplicateGroup>,
    pub duplicates_truncated: bool,
    /// 從這份 Document 抽出的 Entity（SPEC §17），含抽取紀錄本身。
    pub entities: Vec<ExtractedEntity>,
    pub entities_truncated: bool,
}

/// 一筆抽取紀錄 + 它指向的 Entity。
///
/// 兩個一起回而不是只回 Entity：`extraction` 帶的是「在哪個位置、用哪條規則、
/// 信心多少」，那是判斷這個抽取結果可不可信的依據。
#[derive(Debug, Clone, Serialize)]
pub struct ExtractedEntity {
    pub extraction: EntityExtraction,
    /// 有外鍵，理論上一定查得到。真的是 `null` 代表資料被外力改壞了，
    /// 所以不隱藏這一筆——讓它顯示成異常，而不是憑空消失。
    pub entity: Option<Entity>,
}

/// `GET /api/v1/objects`。viewer 以上。
pub async fn list_objects(
    State(state): State<AppState>,
    principal: Principal,
    Query(query): Query<ListQuery>,
) -> Result<Json<CursorPage<Document>>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let (after, limit) = Pagination {
        cursor: query.cursor.clone(),
        limit: query.limit,
    }
    .decode()?;
    let items = store
        .list_documents_filtered(query.object_type, query.include_duplicates, after, limit)
        .await
        .map_err(storage_error)?;
    Ok(Json(CursorPage::from_items(items, limit, |d| d.id)))
}

/// `GET /api/v1/objects/{id}`。
pub async fn get_object(
    State(state): State<AppState>,
    principal: Principal,
    Path(id): Path<Uuid>,
) -> Result<Json<ObjectDetail>, ApiError> {
    principal.role.require(Permission::Read)?;
    let store = store(&state)?;
    let document = store
        .get_document(id)
        .await
        .map_err(storage_error)?
        .ok_or_else(|| {
            ApiError::not_found(format!(
                "找不到 object `{id}`。請用 GET /api/v1/objects 確認 id；\
                 RawEvidence 要先經 normalizer 正規化才會產生 Document"
            ))
        })?;

    let provenance = store
        .list_provenance_by_subject(document.id)
        .await
        .map_err(storage_error)?;

    // 同一份 Document 可能有多列 provenance 指向同一筆 RawEvidence，去重後再查。
    let mut raw_evidence: Vec<RawEvidence> = Vec::new();
    for prov in &provenance {
        let Some(raw_id) = prov.raw_evidence_id else {
            continue;
        };
        if raw_evidence.iter().any(|e| e.id == raw_id) {
            continue;
        }
        if let Some(evidence) = store
            .get_raw_evidence(raw_id)
            .await
            .map_err(storage_error)?
        {
            raw_evidence.push(evidence);
        }
    }

    // 去重關係走 duplicate_groups 表而不是只看 documents.duplicate_of：
    // group 才帶得出 method 與 similarity（憑哪個 stage、多相似）。
    let duplicate_group = store
        .get_duplicate_group_by_member(document.id)
        .await
        .map_err(storage_error)?;
    let duplicates = if duplicate_group.is_some() {
        // 已經是別人的重複就不會同時是 canonical，省一次查詢。
        Vec::new()
    } else {
        store
            .list_duplicate_groups_by_canonical(document.id, LINK_PAGE)
            .await
            .map_err(storage_error)?
    };

    let extractions = store
        .list_entity_extractions_by_object(document.id, LINK_PAGE)
        .await
        .map_err(storage_error)?;
    let entities_truncated = extractions.len() as u32 >= LINK_PAGE;
    let mut entities = Vec::with_capacity(extractions.len());
    for extraction in extractions {
        let entity = store
            .get_entity(extraction.entity_id)
            .await
            .map_err(storage_error)?;
        entities.push(ExtractedEntity { extraction, entity });
    }

    Ok(Json(ObjectDetail {
        duplicates_truncated: duplicates.len() as u32 >= LINK_PAGE,
        document,
        provenance,
        raw_evidence,
        duplicate_group,
        duplicates,
        entities,
        entities_truncated,
    }))
}

/// `POST /api/v1/objects`：V0.1 **不實作**，回 501。
///
/// 仍然寫稽核：「有人一直試圖直接塞 Document 進來」本身就是要看得到的訊號
/// （通常代表有人在繞過 import 路徑，或把這支 API 當成寫入介面在整合）。
pub async fn create_object(
    State(state): State<AppState>,
    principal: Principal,
    ip: ClientIp,
) -> Result<StatusCode, ApiError> {
    principal.role.require(Permission::Write)?;
    let err = ApiError::new(
        StatusCode::NOT_IMPLEMENTED,
        "not_implemented",
        "V0.1 不支援直接建立 object：那會產生沒有 RawEvidence 祖先的孤兒，\
         違反 SPEC §14 的最低可追溯鏈。請改用 POST /api/v1/import 上傳原始內容\
         （manual／json／csv），它會落地成 RawEvidence 再由 normalizer 產生 Document。\
         完整理由見 docs/adr/ADR-006-no-direct-object-post.md",
    );
    audit(
        &state,
        &principal,
        &ip,
        AuditEvent {
            action: AUDIT_OBJECT_CREATE,
            resource_type: RESOURCE,
            resource_id: None,
            outcome: "rejected",
            metadata: rejected_metadata(&err, json!({ "reason": "not_implemented_by_design" })),
        },
    )
    .await;
    Err(err)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// query 的**預設值**。實際的 query string 解析由 `tests/resources_api_e2e.rs` 驗。
    #[test]
    fn duplicates_are_excluded_by_default() {
        let query: ListQuery = serde_json::from_str("{}").unwrap();
        assert!(
            !query.include_duplicates,
            "預設含重複會讓同一篇報導在列表裡出現十幾次"
        );
        assert!(query.object_type.is_none());

        let query: ListQuery =
            serde_json::from_str(r#"{"include_duplicates":true,"object_type":"advisory"}"#)
                .unwrap();
        assert!(query.include_duplicates);
        assert_eq!(query.object_type, Some(DocumentType::Advisory));
    }
}
