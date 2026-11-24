//! 段落（Chunk）capability（ADR-016 D5，階段 5 M1）。
//!
//! 段落是 PostgreSQL 裡的**衍生資料**：從文件的串接文字（[`crate::chunk_source_text`]）切出來，
//! 可以整批刪掉重切。向量是 OpenSearch 的投影（M2），不在這裡。
//!
//! 獨立 capability，不塞進 [`crate::traits::RelationalStore`]（理由同 [`crate::EventOutboxStore`]：
//! 那個 trait 已經有一百多個方法）。

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::StorageError;

/// 段落 id 的 UUID v5 namespace。**永久凍結**：改了等於所有段落 id 換掉，
/// M2 以後的向量索引、段落群組、共現邊出處都會對不上。
pub const CHUNK_NAMESPACE: Uuid = Uuid::from_u128(0x0199_5c31_7e20_7a55_8b4c_2d3e_6f70_80a1);

/// 段落 id ＝ v5(`CHUNK_NAMESPACE`, "{document_id}|{strategy}@{strategy_version}|{seq}")。
///
/// 同一份文件、同一個策略版本、同一個序號 → 同一個 id，重切冪等。策略或版本一換就是新 id
/// （舊段落在同一個交易裡被整批取代，見 [`ChunkStore::replace_document_chunks`]）。
#[must_use]
pub fn chunk_id(document_id: Uuid, strategy: &str, strategy_version: u32, seq: u32) -> Uuid {
    let key = format!("{document_id}|{strategy}@{strategy_version}|{seq}");
    Uuid::new_v5(&CHUNK_NAMESPACE, key.as_bytes())
}

/// `document_chunks` 的一列。
///
/// `start_char`／`end_char` 是**字元**（Unicode scalar value）偏移，對的是
/// [`crate::chunk_source_text`] 的輸出，不是 UTF-8 byte。不變式：
/// `chunk_source_text(doc).chars().skip(start).take(end - start) == text`。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DocumentChunk {
    pub id: Uuid,
    pub document_id: Uuid,
    /// 從 0 起算、連續。
    pub seq: u32,
    pub start_char: u32,
    pub end_char: u32,
    /// 段落原文（D5：存文字不只存 offset，body 重新正規化時才不會靜默錯位）。
    pub text: String,
    /// 給向量化與檢索加在段落前面的上下文（文件標題等，D12 chunk header）。
    /// **算在 token 數裡**，但不在 `text`／offset 範圍內。
    pub header: Option<String>,
    /// e5 tokenizer 計數（`header` + `text`，不含特殊 token）。
    pub token_count: u32,
    pub strategy: String,
    pub strategy_version: u32,
    /// M4 段落層級近似去重用。M1 一律 `None`。
    pub simhash: Option<i64>,
    /// M4 段落群組。M1 一律 `None`。
    pub group_id: Option<Uuid>,
    pub created_at: DateTime<Utc>,
}

/// 段落統計（`/ops` 與驗收用）。
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct ChunkStats {
    pub documents: u64,
    pub chunks: u64,
    pub max_token_count: u32,
}

#[async_trait]
pub trait ChunkStore: Send + Sync {
    /// 在**同一個交易**裡刪掉這份文件的所有段落、寫入 `chunks`。
    ///
    /// 重切時段落數可能變少；只 upsert 會留下多出來的舊段落，所以一律整批取代。
    /// `chunks` 必須全部屬於 `document_id`、`seq` 從 0 連續，否則回 `Configuration` 不寫入。
    /// 空的 `chunks` 等於刪光（文件沒有任何文字時）。
    async fn replace_document_chunks(
        &self,
        document_id: Uuid,
        chunks: &[DocumentChunk],
    ) -> Result<(), StorageError>;

    /// 依 `seq` 升序。
    async fn list_document_chunks(
        &self,
        document_id: Uuid,
    ) -> Result<Vec<DocumentChunk>, StorageError>;

    /// 依 document_id 升序分頁列出**有段落的**文件 id（`--rebuild` 與驗收掃描用）。
    /// `limit` 由 adapter 夾在 1..=1000。
    async fn list_chunked_document_ids(
        &self,
        after: Option<Uuid>,
        limit: u32,
    ) -> Result<Vec<Uuid>, StorageError>;

    async fn chunk_stats(&self) -> Result<ChunkStats, StorageError>;

    /// 這份文件屬於哪些 Collection（`collection_objects`），依 id 升序，最多 1000 個。
    ///
    /// 給段落投影帶 `collection_ids`（ADR-016 Q5）。放在這裡而不是 `RelationalStore`：
    /// 只有段落投影要「依物件反查集合」，加進那個一百多個方法的 trait 要改 7 個實作。
    async fn document_collection_ids(&self, document_id: Uuid) -> Result<Vec<Uuid>, StorageError>;
}

/// `list_chunked_document_ids` 的上限。
pub const CHUNK_LIST_LIMIT_MAX: u32 = 1000;

#[must_use]
pub fn clamp_chunk_list_limit(limit: u32) -> u32 {
    limit.clamp(1, CHUNK_LIST_LIMIT_MAX)
}

/// `replace_document_chunks` 的輸入檢查，postgres／sqlite 共用。
pub fn validate_chunks(document_id: Uuid, chunks: &[DocumentChunk]) -> Result<(), StorageError> {
    for (i, chunk) in chunks.iter().enumerate() {
        if chunk.document_id != document_id {
            return Err(StorageError::Configuration {
                message: format!(
                    "段落 {} 屬於文件 {}，不是 {document_id}；replace_document_chunks 只能寫同一份文件",
                    chunk.id, chunk.document_id
                ),
            });
        }
        if chunk.seq as usize != i {
            return Err(StorageError::Configuration {
                message: format!("段落序號必須從 0 連續，第 {i} 筆是 seq={}", chunk.seq),
            });
        }
        if chunk.end_char < chunk.start_char {
            return Err(StorageError::Configuration {
                message: format!(
                    "段落 {} 的 end_char {} < start_char {}",
                    chunk.id, chunk.end_char, chunk.start_char
                ),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_id_is_deterministic_and_strategy_sensitive() {
        let doc = Uuid::now_v7();
        assert_eq!(chunk_id(doc, "fixed", 1, 0), chunk_id(doc, "fixed", 1, 0));
        assert_ne!(chunk_id(doc, "fixed", 1, 0), chunk_id(doc, "fixed", 1, 1));
        assert_ne!(chunk_id(doc, "fixed", 1, 0), chunk_id(doc, "fixed", 2, 0));
        assert_ne!(chunk_id(doc, "fixed", 1, 0), chunk_id(doc, "whole", 1, 0));
        assert_eq!(chunk_id(doc, "fixed", 1, 0).get_version_num(), 5);
    }

    /// namespace 凍結：這個固定向量改了就代表有人動了 CHUNK_NAMESPACE 或 id 公式。
    #[test]
    fn chunk_id_formula_is_frozen() {
        let doc = Uuid::from_u128(0x0199_0000_0000_7000_8000_0000_0000_0001);
        assert_eq!(
            chunk_id(doc, "fixed", 1, 0).to_string(),
            Uuid::new_v5(&CHUNK_NAMESPACE, format!("{doc}|fixed@1|0").as_bytes()).to_string()
        );
        assert_eq!(
            CHUNK_NAMESPACE.to_string(),
            "01995c31-7e20-7a55-8b4c-2d3e6f7080a1"
        );
    }
}
