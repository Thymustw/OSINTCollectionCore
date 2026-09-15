//! SPEC §15／§17 Stage 5：semantic duplicate。
//!
//! 介面與降級路徑在這裡；生產實作是 [`crate::semantic_real::VectorSemanticDetector`]。
//! `UnsupportedSemanticDetector` 永遠回 [`SemanticOutcome::Unsupported`]——
//! ml-commons／OpenSearch／Redis 連不上時 `osint-deduplicator` 退回這條。
//!
//! 刻意留明確的 `Unsupported`，而不是「什麼都不做就往下走」：pipeline 要能
//! 回答「Stage 5 跑過沒有」。`dedup.completed` 與 provenance 都會帶上
//! `semantic_detector`，之後才能從歷史資料看出哪些 Document 是在沒有
//! Stage 5 的年代處理的。

use async_trait::async_trait;
use core_model::Document;

use crate::error::DeduplicatorError;

/// Stage 5 的判定結果。
#[derive(Debug, Clone, PartialEq)]
pub enum SemanticOutcome {
    /// 這個 detector 不支援語意判斷（V0.1 的唯一實作走這條）。
    Unsupported,
    /// 沒有語意上的重複。
    NoMatch,
    /// 命中：指向既有的 canonical。
    Hit {
        canonical_object_id: uuid::Uuid,
        similarity: f64,
        /// 這次判定實際用的模型名稱（MiniLM／e5 的穩定字串）。
        /// 寫進 `DuplicateGroup.model`，之後要審查或回滾誤判時才看得出是哪套模型。
        model: String,
    },
}

/// Stage 5 介面。V0.2 生產實作是 [`crate::VectorSemanticDetector`]。
///
/// 實作者注意：這裡不可以做「AI 失敗就讓整條 pipeline 失敗」的事。
/// CLAUDE.md §5 的硬性規則是 AI failure must not block base ingestion——
/// 判斷不出來就回 `NoMatch`／`Unsupported`，錯誤只該在真正的程式錯誤時回傳。
#[async_trait]
pub trait SemanticDuplicateDetector: Send + Sync {
    /// 給人看的名字，會寫進 provenance metadata。
    fn detector_id(&self) -> &'static str;

    async fn detect(&self, document: &Document) -> Result<SemanticOutcome, DeduplicatorError>;
}

/// V0.1 的預設實作：永遠回 [`SemanticOutcome::Unsupported`]。
#[derive(Debug, Clone, Copy, Default)]
pub struct UnsupportedSemanticDetector;

#[async_trait]
impl SemanticDuplicateDetector for UnsupportedSemanticDetector {
    fn detector_id(&self) -> &'static str {
        "unsupported"
    }

    async fn detect(&self, _document: &Document) -> Result<SemanticOutcome, DeduplicatorError> {
        Ok(SemanticOutcome::Unsupported)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use core_model::DocumentType;
    use serde_json::json;

    fn document() -> Document {
        Document {
            id: uuid::Uuid::now_v7(),
            object_type: DocumentType::Article,
            schema_version: "1".into(),
            title: Some("t".into()),
            body: None,
            summary: None,
            language: None,
            author: None,
            published_at: None,
            modified_at: None,
            observed_at: Utc::now(),
            collected_at: Utc::now(),
            source_url: None,
            canonical_url: None,
            normalized_content_hash: None,
            confidence: 0.8,
            labels: Vec::new(),
            attributes: json!({}),
            external_key: None,
            simhash: None,
            duplicate_of: None,
        }
    }

    #[tokio::test]
    async fn v0_1_detector_reports_unsupported_not_nomatch() {
        let detector = UnsupportedSemanticDetector;
        assert_eq!(detector.detector_id(), "unsupported");
        // 這個斷言就是 Stage 5 的驗收點：必須能區分「沒實作」與「查過但沒有重複」。
        assert_eq!(
            detector.detect(&document()).await.expect("不可失敗"),
            SemanticOutcome::Unsupported
        );
    }
}
