//! `osint-entities` index 的明確 mapping 與欄位名常數。
//!
//! Document 向量欄位（`embedding_en`／`embedding_multi`）定義在
//! [`indexer::schema`]，本模組不重複——兩邊各寫一份字面值遲早分岔，
//! 分岔之後 embedding-worker 會對 `osint-documents` 寫一個 mapping 裡
//! 沒有的欄位，被 `dynamic: strict` 以 400 拒絕。
//!
//! # dynamic mapping 是關閉的
//!
//! `"dynamic": "strict"`。理由同 indexer：猜錯的後果很難察覺。
//!
//! # `description_vector_en` 為什麼存在卻不寫
//!
//! Entity 沒有 `language` 欄位，V0.2 一律走多語 e5，只填
//! [`F_DESCRIPTION_VECTOR_MULTI`]。`en` 欄位先在 mapping 裡佔位子，
//! 等 NER 為 Entity 加上語言時才填——現在填 MiniLM 進去會讓未來
//! 真的有英文 Entity 時，兩種空間混在同一欄，而且**不會報錯**。
//! 兩個欄位的 `space_type` 都是 `cosinesimil`，與 documents 的
//! `embedding_en=l2` 不同：這裡沒有 MiniLM 資料，對齊 e5。

use serde_json::{Value, json};

/// 預設 Entity 向量 index 名稱。可用 `[embedding_worker].entities_index` 覆寫。
///
/// 刻意帶 `osint-` 前綴：與 `osint-documents` 同一套理由，避免撞名。
pub const DEFAULT_INDEX: &str = "osint-entities";

pub const F_ENTITY_ID: &str = "entity_id";
pub const F_ENTITY_TYPE: &str = "entity_type";
pub const F_NAME: &str = "name";
pub const F_NORMALIZED_NAME: &str = "normalized_name";
/// 保留給未來「Entity 有語言、走 MiniLM」的欄位。V0.2 **不寫入**。
pub const F_DESCRIPTION_VECTOR_EN: &str = "description_vector_en";
pub const F_DESCRIPTION_VECTOR_EN_MODEL_VERSION: &str = "description_vector_en_model_version";
/// e5-small（多語）向量。V0.2 Entity 唯一會填的向量欄位。
pub const F_DESCRIPTION_VECTOR_MULTI: &str = "description_vector_multi";
pub const F_DESCRIPTION_VECTOR_MULTI_MODEL_VERSION: &str = "description_vector_multi_model_version";

/// index settings。單節點開發叢集：1 shard、0 replica。
#[must_use]
pub fn index_settings() -> Value {
    json!({
        "number_of_shards": 1,
        // 0 replica 是因為 compose 是單節點；多節點部署要改成 1 以上，
        // 否則叢集健康會停在 yellow。這不是「不需要備援」的意思。
        "number_of_replicas": 0,
        "refresh_interval": "1s",
        // k-NN 必須在建立 index 時開啟。OpenSearch 的 `index.knn` 是
        // index-level setting，事後 `_settings` 打不開。
        "index": { "knn": true },
    })
}

fn lowercase_keyword() -> Value {
    json!({ "type": "keyword", "normalizer": "lowercase" })
}

/// `osint-entities` 的完整欄位對映。
#[must_use]
pub fn index_mappings() -> Value {
    json!({
        "dynamic": "strict",
        "properties": {
            F_ENTITY_ID: { "type": "keyword" },
            F_ENTITY_TYPE: lowercase_keyword(),
            // 顯示用。不靠它做過濾——過濾走 normalized_name。
            F_NAME: { "type": "keyword" },
            F_NORMALIZED_NAME: lowercase_keyword(),
            // 兩個 knn_vector 都是 384、hnsw、lucene、cosinesimil。
            // en 與 multi 對齊：V0.2 沒有 MiniLM 的 Entity 資料，
            // 未來若 language tagging 把 en 改走 MiniLM，再考慮改 l2
            // （那是破壞性 mapping 變更，必須 --rebuild --drop）。
            F_DESCRIPTION_VECTOR_EN: {
                "type": "knn_vector",
                "dimension": 384,
                "method": { "name": "hnsw", "engine": "lucene", "space_type": "cosinesimil" }
            },
            F_DESCRIPTION_VECTOR_EN_MODEL_VERSION: { "type": "keyword" },
            F_DESCRIPTION_VECTOR_MULTI: {
                "type": "knn_vector",
                "dimension": 384,
                "method": { "name": "hnsw", "engine": "lucene", "space_type": "cosinesimil" }
            },
            F_DESCRIPTION_VECTOR_MULTI_MODEL_VERSION: { "type": "keyword" },
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mapping_is_strict() {
        assert_eq!(
            index_mappings().get("dynamic").and_then(Value::as_str),
            Some("strict"),
            "關掉 dynamic mapping 是刻意的；打開會讓型別由第一筆資料決定"
        );
    }

    #[test]
    fn knn_vector_fields_are_384_cosine() {
        let mappings = index_mappings();
        for field in [F_DESCRIPTION_VECTOR_EN, F_DESCRIPTION_VECTOR_MULTI] {
            assert_eq!(
                mappings
                    .pointer(&format!("/properties/{field}/type"))
                    .and_then(Value::as_str),
                Some("knn_vector"),
                "{field} 不是 knn_vector"
            );
            assert_eq!(
                mappings
                    .pointer(&format!("/properties/{field}/dimension"))
                    .and_then(Value::as_u64),
                Some(384),
                "{field} 維度不是 384"
            );
            assert_eq!(
                mappings
                    .pointer(&format!("/properties/{field}/method/engine"))
                    .and_then(Value::as_str),
                Some("lucene"),
                "{field} 的 engine 不是 lucene"
            );
            assert_eq!(
                mappings
                    .pointer(&format!("/properties/{field}/method/space_type"))
                    .and_then(Value::as_str),
                Some("cosinesimil"),
                "{field} 的 space_type 不是 cosinesimil（Entity 兩欄都對齊 e5）"
            );
        }
        for field in [
            F_DESCRIPTION_VECTOR_EN_MODEL_VERSION,
            F_DESCRIPTION_VECTOR_MULTI_MODEL_VERSION,
        ] {
            assert_eq!(
                mappings
                    .pointer(&format!("/properties/{field}/type"))
                    .and_then(Value::as_str),
                Some("keyword"),
                "{field} 不是 keyword"
            );
        }
    }

    #[test]
    fn index_settings_enable_knn() {
        assert_eq!(
            index_settings()
                .pointer("/index/knn")
                .and_then(Value::as_bool),
            Some(true),
            "index.knn 必須在建立 index 時開啟；事後無法動態打開"
        );
    }

    #[test]
    fn identity_fields_are_keywords() {
        let mappings = index_mappings();
        for field in [F_ENTITY_ID, F_NAME] {
            assert_eq!(
                mappings
                    .pointer(&format!("/properties/{field}/type"))
                    .and_then(Value::as_str),
                Some("keyword"),
                "{field} 不是 keyword"
            );
        }
    }
}
