//! 對本機共用 OpenSearch 的 ml-commons embedding conformance。
//!
//! **唯讀查詢＋推論。** 不要 undeploy／delete 任何模型——這台機器的兩個模型
//! 是 Phase 0 手動驗證過的，弄壞了要重跑整段下載＋部署。

use std::time::Instant;

use storage_core::conformance::{
    assert_opensearch_identity, load_workspace_dotenv, required_env, verify_not_opencti_search,
};
use storage_core::{
    EmbeddingKind, EmbeddingProvider, EmbeddingRequest, HealthProvider, embedding_content_hash,
};
use storage_opensearch::{
    E5_CONTENT_HASH, E5_MODEL_NAME, MINILM_CONTENT_HASH, MINILM_MODEL_NAME,
    MlCommonsEmbeddingProvider,
};

fn req(text: &str, kind: EmbeddingKind, lang: Option<&str>) -> EmbeddingRequest {
    EmbeddingRequest {
        text: text.into(),
        kind,
        language: lang.map(str::to_string),
    }
}

#[tokio::test]
async fn ml_commons_embedding_conformance() {
    load_workspace_dotenv();
    let url = required_env("OPENSEARCH_URL").expect("OPENSEARCH_URL");
    let _parsed = verify_not_opencti_search(&url).expect("URL 格式或本機嚴格模式檢查失敗");

    let connect_started = Instant::now();
    let provider = match MlCommonsEmbeddingProvider::connect(&url).await {
        Ok(p) => p,
        Err(err) => panic!(
            "連不上 ml-commons 或模型不是 DEPLOYED：{err}\n\
             請先確認 docker 容器 osint-core-opensearch-1 是 healthy，\
             並已跑過 scripts/opensearch-ml-setup.sh 與 scripts/opensearch-ml-setup-e5.sh"
        ),
    };
    eprintln!("connect 耗時 {:?}", connect_started.elapsed());

    let health = provider.health().await.expect("health");
    assert!(health.healthy, "{}", health.message);
    eprintln!("health: {}", health.message);

    let en_ref = provider.model_for(Some("en"));
    assert_eq!(en_ref.model, MINILM_MODEL_NAME);
    assert_eq!(en_ref.model_version, minilm_hash());
    assert_eq!(en_ref.dimensions, 384);
    assert_eq!(provider.dimensions_for(Some("en")), 384);

    let zh_ref = provider.model_for(Some("zh"));
    assert_eq!(zh_ref.model, E5_MODEL_NAME);
    assert_eq!(zh_ref.model_version, E5_CONTENT_HASH);
    assert_eq!(zh_ref.dimensions, 384);

    let unknown_ref = provider.model_for(None);
    assert_eq!(
        unknown_ref.model, E5_MODEL_NAME,
        "語言未知必須走多語模型，不能默認英文 MiniLM"
    );
    assert_eq!(provider.dimensions(), provider.dimensions_for(None));

    let en_text = "Microsoft is a technology company";
    let en_started = Instant::now();
    let en = provider
        .embed(&req(en_text, EmbeddingKind::Query, Some("en")))
        .await
        .expect("英文 embed");
    let en_elapsed = en_started.elapsed();
    assert_eq!(en.model, MINILM_MODEL_NAME);
    assert_eq!(en.model_version, minilm_hash());
    assert_eq!(en.dimensions, 384);
    assert_eq!(en.vector.len(), 384);
    assert_eq!(en.content_hash, embedding_content_hash(en_text));
    eprintln!(
        "英文 MiniLM：model={} version={} dim={} 耗時 {:?}",
        en.model, en.model_version, en.dimensions, en_elapsed
    );

    let zh_text = "微軟是一家科技公司";
    let zh_started = Instant::now();
    let zh = provider
        .embed(&req(zh_text, EmbeddingKind::Query, Some("zh")))
        .await
        .expect("中文 embed");
    let zh_elapsed = zh_started.elapsed();
    assert_eq!(zh.model, E5_MODEL_NAME);
    assert_eq!(zh.model_version, E5_CONTENT_HASH);
    assert_eq!(zh.dimensions, 384);
    assert_eq!(zh.vector.len(), 384);
    assert_eq!(zh.content_hash, embedding_content_hash(zh_text));
    eprintln!(
        "中文 e5：model={} version={} dim={} 耗時 {:?}",
        zh.model, zh.model_version, zh.dimensions, zh_elapsed
    );

    let en_again = provider
        .embed(&req(en_text, EmbeddingKind::Query, Some("en")))
        .await
        .expect("英文 embed 第二次");
    assert_eq!(
        en.content_hash, en_again.content_hash,
        "同一段文字算兩次 content_hash 必須一樣，這是 re-generate 的基礎保證"
    );
    assert_eq!(en.content_hash, embedding_content_hash(en_text));

    let zh_passage = provider
        .embed(&req(zh_text, EmbeddingKind::Passage, Some("zh")))
        .await
        .expect("中文 passage");
    assert_eq!(
        zh.content_hash, zh_passage.content_hash,
        "hash 打在原文，不含 query:/passage: 前綴"
    );
    assert_ne!(
        zh.vector, zh_passage.vector,
        "e5 的 query:/passage: 前綴必須讓兩端向量不同"
    );

    let mixed_started = Instant::now();
    let mixed = provider
        .embed_batch(&[
            req("hello world", EmbeddingKind::Query, Some("en")),
            req("你好世界", EmbeddingKind::Query, Some("zh")),
            req(
                "another english sentence",
                EmbeddingKind::Passage,
                Some("en-US"),
            ),
            req("未知語言也該走 e5", EmbeddingKind::Passage, None),
        ])
        .await
        .expect("混合語言批次");
    let mixed_elapsed = mixed_started.elapsed();
    assert_eq!(mixed.len(), 4);
    assert_eq!(
        mixed[0].model, MINILM_MODEL_NAME,
        "第 0 筆英文必須走 MiniLM"
    );
    assert_eq!(mixed[1].model, E5_MODEL_NAME, "第 1 筆中文必須走 e5");
    assert_eq!(
        mixed[2].model, MINILM_MODEL_NAME,
        "第 2 筆 en-US 必須走 MiniLM"
    );
    assert_eq!(mixed[3].model, E5_MODEL_NAME, "第 3 筆未知語言必須走 e5");
    assert_eq!(mixed[0].vector.len(), 384);
    assert_eq!(mixed[1].vector.len(), 384);
    assert_eq!(mixed[0].content_hash, embedding_content_hash("hello world"));
    assert_eq!(mixed[1].content_hash, embedding_content_hash("你好世界"));
    eprintln!(
        "混合語言批次 4 筆（2 MiniLM + 2 e5）耗時 {:?}",
        mixed_elapsed
    );

    let empty = provider.embed_batch(&[]).await.expect("空批次");
    assert!(empty.is_empty());
}

fn minilm_hash() -> String {
    std::env::var("ML_MODEL_SHA256")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| MINILM_CONTENT_HASH.to_string())
}

/// 連線後確認對面真的是 OpenSearch，避免打到 OpenCTI 的 9200。
#[tokio::test]
async fn ml_commons_target_is_opensearch() {
    load_workspace_dotenv();
    let url = required_env("OPENSEARCH_URL").expect("OPENSEARCH_URL");
    let _parsed = verify_not_opencti_search(&url).expect("URL 格式");
    let store = storage_opensearch::OpenSearchStore::connect(&url).expect("client");
    let info = store.cluster_info().await.expect("GET /");
    assert_opensearch_identity(&info).expect("必須是 OpenSearch 不是 Elasticsearch");
}
