//! `osint-documents` index 的明確 mapping、欄位名常數與搜尋欄位權重。
//!
//! # 為什麼 mapping、欄位常數與查詢語法同住一個 crate
//!
//! 這三件事必須一起改。analyzer 換了、sub-field 改名、boost 調整、新增可過濾欄位——
//! 任何一項只改一邊都**不會報錯**，只會讓搜尋悄悄變得不準（查不到、或全部命中）。
//! 放在同一個模組讓它們在同一次編輯裡被看見；core-api 與 osint-cli 都引用這裡的常數，
//! 沒有任何一方自己寫死欄位字串。
//!
//! # dynamic mapping 是關閉的
//!
//! `"dynamic": "strict"`。寫進未宣告的欄位會被 OpenSearch 以 400
//! `strict_dynamic_mapping_exception` 拒絕，而不是讓它用第一筆資料猜型別。
//! 猜錯的後果很難察覺：一個本來該是 `date` 的欄位被猜成 `text`，range 過濾
//! 不會報錯，只會回錯的結果。

use serde_json::{Value, json};
use storage_core::SearchField;

/// 預設 index 名稱。可用 `[indexer].index` 覆寫。
///
/// 刻意帶 `osint-` 前綴：`OPENSEARCH_URL` 有可能被指到與別的堆疊共用的叢集，
/// 一個叫 `documents` 的 index 名字太通用，撞名時是直接寫進別人的資料。
pub const DEFAULT_INDEX: &str = "osint-documents";

// --- 欄位名。所有查詢與索引路徑都用這些常數，不要在別處寫字串字面值。 ---
pub const F_DOCUMENT_ID: &str = "document_id";
pub const F_OBJECT_TYPE: &str = "object_type";
pub const F_TITLE: &str = "title";
pub const F_SUMMARY: &str = "summary";
pub const F_BODY: &str = "body";
pub const F_LANGUAGE: &str = "language";
pub const F_SOURCE_ID: &str = "source_id";
pub const F_CONNECTOR_ID: &str = "connector_id";
pub const F_RAW_EVIDENCE_ID: &str = "raw_evidence_id";
pub const F_PUBLISHED_AT: &str = "published_at";
pub const F_OBSERVED_AT: &str = "observed_at";
/// collector 取得這一份的時間。
///
/// 除了顯示之外，它還是 **projection checkpoint 的來源時間戳**
/// （`ProjectionCheckpoint::last_source_at`，見 `crate::service`）。
/// 改掉這個欄位名要同時改 `projection::source_timestamp`，否則 lag 會靜默變成 None。
pub const F_COLLECTED_AT: &str = "collected_at";
/// `published_at` 有值時等於它，否則等於 `observed_at`。date range 的預設過濾欄位。
pub const F_EFFECTIVE_DATE: &str = "effective_date";
pub const F_DUPLICATE_OF: &str = "duplicate_of";
pub const F_ENTITIES: &str = "entities";
pub const F_ENTITY_TYPE: &str = "entities.entity_type";
pub const F_ENTITY_NORMALIZED_NAME: &str = "entities.normalized_name";
/// MiniLM（英文）向量。與 [`F_EMBEDDING_MULTI`] 分欄位：兩個模型維度都是 384，
/// 但向量空間不相通，混同一個欄位做 k-NN 會得到無意義鄰居。
pub const F_EMBEDDING_EN: &str = "embedding_en";
/// 寫入 [`F_EMBEDDING_EN`] 時用的模型版本（內容雜湊，不是 ml-commons 的 `"1"`）。
/// 查詢／稽核不用回頭查 PostgreSQL 就能看出這份文件的向量是否過期。
pub const F_EMBEDDING_EN_MODEL_VERSION: &str = "embedding_en_model_version";
/// e5-small（多語／中文）向量。理由見 [`F_EMBEDDING_EN`]。
pub const F_EMBEDDING_MULTI: &str = "embedding_multi";
pub const F_EMBEDDING_MULTI_MODEL_VERSION: &str = "embedding_multi_model_version";

/// `search_after` 的排序鍵尾巴。**必須是唯一欄位**，否則同分文件翻頁會漏或重複。
pub const F_SORT_TIEBREAK: &str = F_DOCUMENT_ID;

/// date range 可以指定的時間欄位。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DateField {
    /// `published_at` 有值時用它，否則退回 `observed_at`。**預設**。
    ///
    /// 為什麼不直接用 `published_at`：很多來源（static web、部分 REST API、手動匯入）
    /// 根本沒有發布時間。用 `published_at` 當預設會讓那些文件在**任何**日期區間查詢裡
    /// 都消失——使用者看到的是「這個來源沒資料」，而不是「這個來源沒有發布時間」。
    #[default]
    Effective,
    /// 只看 `published_at`。沒有發布時間的文件不會出現在結果裡。
    Published,
    /// 只看 `observed_at`（本系統第一次看到它的時間）。
    Observed,
}

impl DateField {
    #[must_use]
    pub fn field_name(self) -> &'static str {
        match self {
            Self::Effective => F_EFFECTIVE_DATE,
            Self::Published => F_PUBLISHED_AT,
            Self::Observed => F_OBSERVED_AT,
        }
    }
}

/// 全文查詢要掃的欄位與權重。
///
/// 每個文字欄位都出現兩次：`x`（standard analyzer）與 `x.cjk`（cjk bigram）。
/// 兩者權重相同——同一段內容在兩個 analyzer 下各算一次分，
/// 給 `.cjk` 較低權重只會讓中文查詢的排序比英文差，沒有道理。
#[must_use]
pub fn full_text_fields() -> Vec<SearchField> {
    vec![
        SearchField::new(F_TITLE, 3.0),
        SearchField::new(format!("{F_TITLE}.cjk"), 3.0),
        SearchField::new(F_SUMMARY, 2.0),
        SearchField::new(format!("{F_SUMMARY}.cjk"), 2.0),
        SearchField::new(F_BODY, 1.0),
        SearchField::new(format!("{F_BODY}.cjk"), 1.0),
    ]
}

/// highlight 要產生片段的欄位。只取 title／summary／body 的 standard 版本；
/// `.cjk` 的命中靠 `require_field_match: false` 折回母欄位（見 storage-opensearch）。
#[must_use]
pub fn highlight_fields() -> Vec<String> {
    vec![F_TITLE.into(), F_SUMMARY.into(), F_BODY.into()]
}

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
        // index-level setting，事後 `_settings` 打不開——對既有
        // `osint-documents` 加 knn_vector 欄位一定要 `--rebuild --drop`。
        // 2026-09-14 在 2.19.6 實測：扁平的 shards／replicas 與巢狀
        // `"index": {"knn": true}` 可以並存，叢集會把它收成 `index.knn=true`。
        "index": { "knn": true },
    })
}

/// 一個同時支援英文與中日韓的文字欄位。
///
/// # analyzer 取捨（compose 的 image 是原版 `opensearchproject/opensearch:2.19.6`，
/// 不裝任何外部 plugin）
///
/// * `standard`：對英文正確斷詞；對中文會退化成**單字**切分。
///   只用它的話查「勒索軟體」會被拆成四個字，用 OR 比對等同查「勒 OR 索 OR 軟 OR 體」，
///   幾乎任何中文文件都命中。
/// * `cjk`（Lucene 內建，OpenSearch core 自帶，已在 2.19.6 實測可用）：
///   CJK 字元做 **bigram**，非 CJK 走 standard 的規則。「勒索軟體」→
///   `勒索`／`索軟`／`軟體`，精確度遠優於單字。
/// * 沒有選 `analysis-icu` / `analysis-smartcn`：都需要 `opensearch-plugin install`，
///   等於自建 image 並在每次升級時重裝。V0.1 不接受這個維運成本。
///
/// 已知代價：bigram 不是真正的斷詞，「軟體攻擊」會在「防毒軟體。攻擊者…」這種
/// 跨句邊界的地方產生假命中（`體攻` bigram 不會，但 phrase 之外的 term 查詢會）。
/// 精確比對請用引號做 phrase 查詢。
fn text_field(extra: Option<Value>) -> Value {
    let mut fields = json!({
        "cjk": { "type": "text", "analyzer": "cjk" }
    });
    if let (Some(extra), Some(map)) = (extra, fields.as_object_mut()) {
        if let Some(extra) = extra.as_object() {
            for (key, value) in extra {
                map.insert(key.clone(), value.clone());
            }
        }
    }
    json!({
        "type": "text",
        "analyzer": "standard",
        "fields": fields,
    })
}

/// 大小寫不敏感的 keyword 欄位。
///
/// `lowercase` 是 OpenSearch 內建 normalizer（2.19.6 實測可用，不需要自訂 analysis
/// 設定）。索引與查詢兩端都折成小寫，但 `_source` 保留原始寫法——
/// 所以顯示出來的還是 `CVE-2026-0001`，只有比對是小寫。
fn lowercase_keyword() -> Value {
    json!({ "type": "keyword", "normalizer": "lowercase" })
}

/// `osint-documents` 的完整欄位對映。
#[must_use]
pub fn index_mappings() -> Value {
    json!({
        // 未宣告的欄位一律拒絕，不要讓 OpenSearch 自己猜型別。
        "dynamic": "strict",
        "properties": {
            F_DOCUMENT_ID: { "type": "keyword" },
            F_OBJECT_TYPE: lowercase_keyword(),
            "schema_version": { "type": "keyword" },

            F_TITLE: text_field(Some(json!({
                "keyword": { "type": "keyword", "ignore_above": 256 }
            }))),
            F_SUMMARY: text_field(None),
            F_BODY: text_field(None),
            "author": text_field(Some(json!({
                "keyword": { "type": "keyword", "ignore_above": 256 }
            }))),

            F_LANGUAGE: lowercase_keyword(),
            "labels": { "type": "keyword" },
            F_SOURCE_ID: { "type": "keyword" },
            F_CONNECTOR_ID: { "type": "keyword" },
            // Acceptance E 的起點：每個 hit 都要帶得回 RawEvidence。
            F_RAW_EVIDENCE_ID: { "type": "keyword" },
            "canonical_url": { "type": "keyword" },
            // 只是要顯示，不需要被查詢或聚合。index:false 省下倒排索引空間，
            // 而且避免有人誤以為可以用 URL 做過濾（keyword 是整串精確比對，
            // 對 URL 幾乎永遠比不中）。
            "source_url": { "type": "keyword", "index": false },

            F_PUBLISHED_AT: { "type": "date" },
            F_OBSERVED_AT: { "type": "date" },
            "collected_at": { "type": "date" },
            F_EFFECTIVE_DATE: { "type": "date" },
            "indexed_at": { "type": "date" },

            // 正常情況下永遠是 null（duplicate 不進 index，見 crate 說明）。
            // 保留欄位是為了讓「排除 duplicate」的過濾條件有東西可以掛，
            // 而且萬一有 duplicate 漏進來，這裡看得出來。
            F_DUPLICATE_OF: { "type": "keyword" },
            "confidence": { "type": "float" },

            // nested 而不是扁平陣列：查「type=vulnerability 且 name=CVE-2026-0001」時，
            // 扁平陣列會命中「有某個漏洞、也有某個叫 CVE-2026-0001 的東西」的文件，
            // 兩個條件不保證落在同一個 entity 上。
            F_ENTITIES: {
                "type": "nested",
                "properties": {
                    "entity_id": { "type": "keyword" },
                    "entity_type": lowercase_keyword(),
                    "name": text_field(None),
                    // 大小寫由 normalizer 折疊，不是由呼叫端猜。
                    // entity-worker 的正規化規則因型別而異（CVE 轉大寫、
                    // domain／email／hash 轉小寫），搜尋端沒有辦法重現那套規則；
                    // 用 lowercase normalizer 之後兩邊都折成小寫，
                    // 使用者打 `cve-2026-0001` 或 `CVE-2026-0001` 都查得到。
                    "normalized_name": lowercase_keyword(),
                }
            },
            "entity_count": { "type": "integer" },

            // k-NN 欄位。兩個模型維度都是 384，但空間不相通，所以分欄位。
            //
            // engine: lucene — 2026-09-14 在 OpenSearch 2.19.6 實測可用
            // （`PUT` mapping 回 200）。官方映像
            // `opensearchproject/opensearch:2.19.6` 內建 `opensearch-knn`
            // 2.19.6.0，**未跑** `opensearch-ml-setup.sh` 的乾淨容器
            // （本機另起埠 19210）同樣能建 index 並查出最近鄰。
            // 選 lucene 而不是 faiss／nmslib：純 Java、knn 子句內的
            // `filter` 原生可用、不需要額外 native library。本機／CI
            // 都是單節點小索引，不需要近似搜尋的效能優勢。
            //
            // space_type：
            // * `embedding_en`（MiniLM）用 `l2`——上游宣告就是 l2
            //   （`docs/developer/embedding.md` §2.2）。
            // * `embedding_multi`（e5）用 `cosinesimil`——e5 已
            //   `normalize_result: true`，正規化向量下 cosine 與內積等價，
            //   cosine 更直接對應「方向相近」。
            F_EMBEDDING_EN: {
                "type": "knn_vector",
                "dimension": 384,
                "method": { "name": "hnsw", "engine": "lucene", "space_type": "l2" }
            },
            F_EMBEDDING_EN_MODEL_VERSION: { "type": "keyword" },
            F_EMBEDDING_MULTI: {
                "type": "knn_vector",
                "dimension": 384,
                "method": { "name": "hnsw", "engine": "lucene", "space_type": "cosinesimil" }
            },
            F_EMBEDDING_MULTI_MODEL_VERSION: { "type": "keyword" },
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
    fn text_fields_have_a_cjk_subfield() {
        let mappings = index_mappings();
        for field in [F_TITLE, F_SUMMARY, F_BODY] {
            let path = format!("/properties/{field}/fields/cjk/analyzer");
            assert_eq!(
                mappings.pointer(&path).and_then(Value::as_str),
                Some("cjk"),
                "{field} 少了 cjk sub-field，中文查詢會退化成單字比對"
            );
        }
    }

    #[test]
    fn entities_are_nested_not_object() {
        assert_eq!(
            index_mappings()
                .pointer("/properties/entities/type")
                .and_then(Value::as_str),
            Some("nested"),
            "改成 object 會讓 entity 的型別與名稱條件互相脫鉤"
        );
    }

    #[test]
    fn date_fields_are_dates() {
        let mappings = index_mappings();
        for field in [F_PUBLISHED_AT, F_OBSERVED_AT, F_EFFECTIVE_DATE] {
            assert_eq!(
                mappings
                    .pointer(&format!("/properties/{field}/type"))
                    .and_then(Value::as_str),
                Some("date"),
                "{field} 不是 date，range 過濾會變成字串比較且不會報錯"
            );
        }
    }

    #[test]
    fn full_text_fields_cover_both_analyzers() {
        let names: Vec<String> = full_text_fields().into_iter().map(|f| f.name).collect();
        for field in [F_TITLE, F_SUMMARY, F_BODY] {
            assert!(names.contains(&field.to_string()), "缺 {field}");
            assert!(names.contains(&format!("{field}.cjk")), "缺 {field}.cjk");
        }
    }

    #[test]
    fn every_full_text_field_exists_in_the_mapping() {
        // 查一個 mapping 裡沒有的欄位不會報錯，只會永遠查不到東西。
        let mappings = index_mappings();
        for field in full_text_fields() {
            let (base, sub) = match field.name.split_once('.') {
                Some((base, sub)) => (base.to_string(), Some(sub.to_string())),
                None => (field.name.clone(), None),
            };
            let path = match &sub {
                Some(sub) => format!("/properties/{base}/fields/{sub}"),
                None => format!("/properties/{base}"),
            };
            assert!(
                mappings.pointer(&path).is_some(),
                "full_text_fields 的 `{}` 在 mapping 裡不存在",
                field.name
            );
        }
    }

    #[test]
    fn date_field_default_is_effective() {
        assert_eq!(DateField::default(), DateField::Effective);
        assert_eq!(DateField::default().field_name(), F_EFFECTIVE_DATE);
    }

    #[test]
    fn knn_vector_fields_are_384() {
        let mappings = index_mappings();
        for field in [F_EMBEDDING_EN, F_EMBEDDING_MULTI] {
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
        }
        assert_eq!(
            mappings
                .pointer(&format!("/properties/{F_EMBEDDING_EN}/method/space_type"))
                .and_then(Value::as_str),
            Some("l2")
        );
        assert_eq!(
            mappings
                .pointer(&format!(
                    "/properties/{F_EMBEDDING_MULTI}/method/space_type"
                ))
                .and_then(Value::as_str),
            Some("cosinesimil")
        );
        for field in [
            F_EMBEDDING_EN_MODEL_VERSION,
            F_EMBEDDING_MULTI_MODEL_VERSION,
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
}
