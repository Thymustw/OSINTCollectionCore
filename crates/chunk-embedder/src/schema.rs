//! `osint-chunks` index（ADR-016 D5：段落向量是 OpenSearch 投影，可從 PostgreSQL 重建）。
//!
//! `_id` ＝ 段落 id（`storage_core::chunk_id`），重送是覆寫不是新增。
//! k-NN 參數與實驗 5-2e 相同：lucene hnsw、384 維、cosinesimil、m=16、ef_construction=100。
//! 文字欄位沿用 `osint-documents` 的 analyzer 慣例（`standard`＋`.cjk` bigram＋應用端簡體副本），
//! 理由見 `indexer::schema` 的 analyzer 取捨；放進來是為了之後段落層級 BM25／混合檢索不必重建（M2 決定 3）。
//!
//! 改 mapping（加欄位以外的任何修改）要 `--rebuild --drop`。

use serde_json::{Value, json};

pub const DEFAULT_INDEX: &str = "osint-chunks";

pub const F_CHUNK_ID: &str = "chunk_id";
pub const F_DOCUMENT_ID: &str = "document_id";
pub const F_SEQ: &str = "seq";
pub const F_TEXT: &str = "text";
pub const F_TEXT_S: &str = "text_s";
pub const F_HEADER: &str = "header";
pub const F_EMBEDDING: &str = "embedding";
pub const F_MODEL: &str = "embedding_model";
pub const F_STRATEGY: &str = "strategy";
pub const F_STRATEGY_VERSION: &str = "strategy_version";
pub const F_TOKEN_COUNT: &str = "token_count";
pub const F_COLLECTION_IDS: &str = "collection_ids";
pub const F_OBJECT_TYPE: &str = "object_type";
pub const F_LANGUAGE: &str = "language";
/// 時間欄位：段落表沒有時間欄位（M1 決定 2），這裡冗餘一份文件的時間，讓段落檢索能直接過濾。
pub const F_PUBLISHED_AT: &str = "published_at";
pub const F_COLLECTED_AT: &str = "collected_at";
pub const F_TEXT_NORM_VERSION: &str = "text_norm_version";
pub const F_INDEXED_AT: &str = "indexed_at";

/// e5 的維度。換模型就換 index（向量空間不相通）。
pub const DIMENSIONS: u32 = 384;

#[must_use]
pub fn index_settings() -> Value {
    json!({
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "refresh_interval": "1s",
        // k-NN 只能在建立 index 時打開，事後 `_settings` 開不了（見 indexer::schema）。
        "index": { "knn": true },
    })
}

#[must_use]
pub fn index_mappings() -> Value {
    json!({
        // strict：型別不由第一筆資料決定；寫錯欄位名會被拒絕而不是悄悄多一個欄位。
        "dynamic": "strict",
        "properties": {
            F_CHUNK_ID: { "type": "keyword" },
            F_DOCUMENT_ID: { "type": "keyword" },
            F_SEQ: { "type": "integer" },
            F_TEXT: {
                "type": "text",
                "analyzer": "standard",
                "fields": { "cjk": { "type": "text", "analyzer": "cjk" } }
            },
            F_TEXT_S: { "type": "text", "analyzer": "cjk" },
            F_HEADER: {
                "type": "text",
                "analyzer": "standard",
                "fields": { "cjk": { "type": "text", "analyzer": "cjk" } }
            },
            F_EMBEDDING: {
                "type": "knn_vector",
                "dimension": DIMENSIONS,
                "method": {
                    "name": "hnsw",
                    "engine": "lucene",
                    "space_type": "cosinesimil",
                    "parameters": { "m": 16, "ef_construction": 100 }
                }
            },
            F_MODEL: { "type": "keyword" },
            F_STRATEGY: { "type": "keyword" },
            F_STRATEGY_VERSION: { "type": "integer" },
            F_TOKEN_COUNT: { "type": "integer" },
            // ADR-016 Q5：現在就放；之後才加要整個段落索引重建。
            F_COLLECTION_IDS: { "type": "keyword" },
            F_OBJECT_TYPE: { "type": "keyword" },
            F_LANGUAGE: { "type": "keyword" },
            F_PUBLISHED_AT: { "type": "date" },
            F_COLLECTED_AT: { "type": "date" },
            F_TEXT_NORM_VERSION: { "type": "keyword" },
            F_INDEXED_AT: { "type": "date" },
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping_is_strict_and_vector_matches_e5() {
        let m = index_mappings();
        assert_eq!(m["dynamic"], "strict");
        assert_eq!(m["properties"][F_EMBEDDING]["dimension"], 384);
        assert_eq!(
            m["properties"][F_EMBEDDING]["method"]["space_type"],
            "cosinesimil"
        );
        assert_eq!(m["properties"][F_COLLECTION_IDS]["type"], "keyword");
        assert_eq!(m["properties"][F_TEXT]["fields"]["cjk"]["analyzer"], "cjk");
        assert_eq!(index_settings()["index"]["knn"], true);
    }
}
