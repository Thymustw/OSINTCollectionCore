//! SPEC §26 Acceptance F：**consumer crash 後重送 event 不建立 duplicate canonical object。**
//!
//! # 這裡在驗的是一條從來沒有被測過的推論鏈
//!
//! deduplicator 與 entity-worker 仍是 **write-then-claim**（先寫資料，最後才佔
//! provenance 的 unique claim；理由見 `docs/developer/collector-normalizer.md`）。
//! 這個順序刻意接受一個 crash window：資料寫完、claim 還沒寫就死掉，重跑會再寫一次。
//! 文件裡寫的是「重跑產生的重複由下游收斂」——靠決定性的 v5 id 不產生第二列。
//! **那整條推論在 Phase 7b 之前沒有任何測試。**
//!
//! **normalizer 從 V0.2 Phase 0e 起改用交易**（`TransactionalStore`），
//! 它已經沒有 write-then-claim 的 crash window——所以下面第 1 個測試驗的東西也換了：
//! 不再是「重複被 dedup 收斂」，而是「上一輪沒 commit 就等於什麼都沒發生」。
//!
//! 模擬 crash 的手法依測試而異：
//! * 交易版（normalizer）→ 真的開一個交易寫進去，然後**不 commit 就 drop**。
//! * write-then-claim（dedup／entity-worker）→ 用 SQL 直接刪掉 claim，
//!   那就是「claim 前 crash」這個狀態的定義。
//!
//! 兩者都重送**真正發出去的那一則事件 payload**，不是自己組一個形狀相近的 JSON。
//!
//! 需要本機 Docker：Postgres／MinIO／Redpanda。不打外網。

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use core_events::{EventProducer, EventTopic};
use core_model::{
    Document, DocumentType, DuplicateGroup, EntityExtraction, Provenance, Relationship,
};
use core_observability::MetricsRegistry;
use deduplicator::{ACTION_DEDUPLICATED, DedupOutcome};
use entity_worker::{ACTION_ENTITY_EXTRACTED, ExtractOutcome};
use normalizer::{ACTION_NORMALIZED, NormalizeOutcome};
use storage_core::{RelationalStore, TransactionalStore};
use uuid::Uuid;

/// 查關聯資料時一次取幾筆。測試資料遠小於這個數，取滿代表資料有問題。
const PAGE: u32 = 200;

/// 新建的 consumer group 等 partition 指派的時間。
///
/// 這個 sleep 是既有 e2e 的慣例（`normalizer/tests/e2e.rs`）：subscribe 之後
/// 立刻 produce 的話，第一次 rebalance 可能還沒完成。
const ASSIGN_WAIT: Duration = Duration::from_millis(1_500);

// ---------------------------------------------------------------------------
// 1. normalizer：交易化之後的 crash 重送
// ---------------------------------------------------------------------------

/// collect → 模擬「上一輪的交易沒 commit 就死掉」（真的開一個交易寫 Document +
/// `derived_from` + `normalized` claim，然後不 commit 直接 drop）→ 重送同一則
/// `raw.collected` → normalize。
///
/// 斷言：
/// * 沒 commit 的那一輪**什麼都沒留下**（Document 與 claim 都不在）
/// * 重送得到乾淨的 `Created`，這筆 RawEvidence 只衍生出**一份** Document
/// * RawEvidence **恰好一筆**——重送事件不會讓證據變兩份
/// * 這份 Document 是 canonical（`duplicate_of IS NULL`），而且沒有東西要 dedup 收
///
/// # 為什麼不再測「刪掉 claim 之後重跑會產生兩份」
///
/// 那個狀態在交易版之後**不可能出現**。Document 與 claim 現在同進同出，
/// 用 SQL 刪掉 claim 只是人工製造一個系統自己寫不出來的狀態，
/// 測它等於在驗一條已經不存在的推論。
#[tokio::test]
async fn acceptance_f_normalizer_uncommitted_replay_leaves_exactly_one_document() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();

    // platform + guid 都帶 run id：dedup Stage 1 的鍵因此只會命中這次 run 的資料，
    // 不會比對到前幾次測試留下的 Document。
    let platform = format!("acc-f-{}", run.simple());
    let guid = format!("acc-f-guid-{}", run.simple());
    let link = format!("http://127.0.0.1/acc-f/{}", run.simple());
    let feed_url = serve_body(rss_body(
        &guid,
        &link,
        &format!("Advisory {}", run.simple()),
        &article(run),
    ))
    .await;
    let source = seed_source(&stack.pg, Some(&platform), Some(&feed_url)).await;
    let connector = seed_connector(&stack.pg, &source, &feed_url).await;

    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "acceptance-f").expect("producer"));
    let raw_probe = probe_consumer(&stack.brokers, EventTopic::RawCollected);
    tokio::time::sleep(ASSIGN_WAIT).await;

    let collector = collector_runner(&stack, producer.clone());
    let raw_evidence_id = collect_once(&collector, &connector).await;
    // 這裡拿到的是 collector 真的發出去的那一則 envelope。後面「重送」用的就是它。
    let collected =
        wait_for_event(&raw_probe, "raw_evidence_id", &raw_evidence_id.to_string()).await;
    assert_eq!(collected.event_type, EventTopic::RawCollected.as_str());

    let metrics = MetricsRegistry::new();
    let normalizer = normalizer(&stack, metrics.clone());

    // ---- 模擬 crash：上一輪的交易寫完了，但還沒 commit 就整個 process 死掉 ----
    //
    // 這裡走的是**真的交易**，不是 SQL 手動製造狀態：normalizer 現在就是這樣寫的
    // （Document + derived_from + claim 同一個 tx）。不 commit 就 drop，
    // sqlx 會排一個 ROLLBACK——「crash 之後 DB 剩下什麼」就是這個。
    let ghost = ghost_document(raw_evidence_id, &guid);
    let ghost_id = ghost.id;
    {
        let tx = stack.pg.begin().await.expect("開交易");
        let db = tx.store();
        db.put_document(&ghost).await.expect("交易內寫 Document");
        db.put_provenance(&ghost_claim(raw_evidence_id, ghost_id))
            .await
            .expect("交易內佔 claim");
        drop(tx); // 沒有 commit
    }

    assert!(
        stack
            .pg
            .get_document(ghost_id)
            .await
            .expect("query")
            .is_none(),
        "沒 commit 的交易不該留下 Document。留下來的話後面的斷言全部沒有意義"
    );
    assert!(
        documents_of_raw_evidence(&stack.pg, raw_evidence_id)
            .await
            .is_empty(),
        "沒 commit 的交易不該留下任何衍生 Document"
    );
    assert!(
        stack
            .pg
            .list_provenance_by_raw_evidence(raw_evidence_id)
            .await
            .expect("query provenance")
            .iter()
            .all(|p| p.action != ACTION_NORMALIZED),
        "沒 commit 的交易不該留下 normalized claim"
    );

    // ---- 重送同一則 raw.collected ----
    let replayed = normalizer
        .handle_payload(&collected.payload)
        .await
        .expect("重送後正規化");
    let NormalizeOutcome::Created {
        document_ids: replay_ids,
    } = replayed
    else {
        panic!(
            "上一輪沒 commit 等於沒發生過，重送必須是乾淨的 Created，實際 {replayed:?}。\
             若這裡是 AlreadyDone，代表回滾沒有把 claim 收掉"
        );
    };
    assert_eq!(replay_ids.len(), 1, "fixture 只有一則 item");
    let doc_id = replay_ids[0];

    // ---- 真正的驗收條件：只有一份 Document，沒有東西要收斂 ----
    let derived = documents_of_raw_evidence(&stack.pg, raw_evidence_id).await;
    assert_eq!(
        derived,
        vec![doc_id],
        "交易版之下這筆 RawEvidence 只能衍生出一份 Document。\
         出現兩份代表回滾沒生效，write-then-claim 的重複代價又回來了"
    );

    let dedup = deduplicator(&stack, metrics.clone());
    let outcome = dedup.dedup_document(doc_id).await.expect("dedup");
    assert!(
        matches!(outcome, DedupOutcome::Canonical { .. }),
        "沒有第二份可比對，這一份就是 canonical，實際 {outcome:?}"
    );
    let doc = stack
        .pg
        .get_document(doc_id)
        .await
        .expect("query")
        .expect("Document 還在");
    assert_eq!(
        doc.duplicate_of, None,
        "重送之後 duplicate_of IS NULL 的必須恰好是這一筆。\
         這就是 Acceptance F 的「不建立 duplicate canonical object」"
    );

    // RawEvidence 不可變且只有一筆：重送事件不會讓證據變兩份。
    assert_eq!(
        count_raw_evidence(&stack.pg, source.id).await,
        1,
        "重送 raw.collected 不該產生第二筆 RawEvidence——證據是 collector 寫的，不是事件寫的"
    );

    // claim 恰好一列（unique index 保證；回滾掉的那一列不算）。
    let claims = stack
        .pg
        .list_provenance_by_raw_evidence(raw_evidence_id)
        .await
        .expect("query provenance");
    assert_eq!(
        claims
            .iter()
            .filter(|p| p.action == ACTION_NORMALIZED)
            .count(),
        1,
        "normalized claim 只能有一列（unique index 保證），實際 {claims:?}"
    );
}

/// 「上一輪沒 commit」那個交易裡要寫的 Document。
///
/// 欄位刻意跟 normalizer 產出的那一份對齊（`attributes.raw_evidence_id` 是
/// `documents_of_raw_evidence` 的查詢鍵），這樣「它有沒有留下來」才問得準。
fn ghost_document(raw_evidence_id: Uuid, guid: &str) -> Document {
    Document {
        id: Uuid::now_v7(),
        object_type: DocumentType::Article,
        schema_version: "1".into(),
        title: Some(format!("ghost {guid}")),
        body: None,
        summary: None,
        language: None,
        author: None,
        published_at: None,
        modified_at: None,
        observed_at: chrono::Utc::now(),
        collected_at: chrono::Utc::now(),
        source_url: None,
        canonical_url: None,
        normalized_content_hash: None,
        confidence: 0.8,
        labels: Vec::new(),
        attributes: serde_json::json!({ "raw_evidence_id": raw_evidence_id }),
        external_key: None,
        simhash: None,
        duplicate_of: None,
    }
}

/// 同一個交易裡的 `normalized` claim。回滾之後它必須跟著消失，
/// 否則 normalizer 會永遠回報 `AlreadyDone` 卻沒有任何 Document。
fn ghost_claim(raw_evidence_id: Uuid, document_id: Uuid) -> Provenance {
    Provenance {
        id: Uuid::now_v7(),
        subject_id: document_id,
        action: ACTION_NORMALIZED.into(),
        parent_id: Some(raw_evidence_id),
        raw_evidence_id: Some(raw_evidence_id),
        processor: "acceptance-f".into(),
        processor_version: "0".into(),
        timestamp: chrono::Utc::now(),
        metadata: serde_json::json!({ "document_ids": [document_id], "item_count": 1 }),
    }
}

// ---------------------------------------------------------------------------
// 2. deduplicator 的 crash window
// ---------------------------------------------------------------------------

/// 兩次採集同一篇文章 → A（canonical）、B（重複，會建一個 DuplicateGroup）
/// → 刪掉 B 的 `deduplicated` claim → 重送同一則 `object.normalized`。
///
/// 斷言：DuplicateGroup 不會多出第二列，group id 也不變
/// （`duplicate_group_id` 是 UUID v5，重跑算出同一個 id → upsert 同一列）。
#[tokio::test]
async fn acceptance_f_deduplicator_crash_replay_does_not_duplicate_the_group() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();

    let platform = format!("acc-f-dd-{}", run.simple());
    let guid = format!("acc-f-dd-guid-{}", run.simple());
    let link = format!("http://127.0.0.1/acc-f-dd/{}", run.simple());
    let feed_url = serve_body(rss_body(
        &guid,
        &link,
        &format!("Advisory {}", run.simple()),
        &article(run),
    ))
    .await;
    let source = seed_source(&stack.pg, Some(&platform), Some(&feed_url)).await;
    let connector = seed_connector(&stack.pg, &source, &feed_url).await;

    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "acceptance-f-dd").expect("producer"));
    let normalized_probe = probe_consumer(&stack.brokers, EventTopic::ObjectNormalized);
    tokio::time::sleep(ASSIGN_WAIT).await;

    let metrics = MetricsRegistry::new();
    let collector = collector_runner(&stack, producer.clone());
    let normalizer = normalizer_with_producer(&stack, metrics.clone(), producer.clone());

    // 同一個假 server 抓兩次：兩筆 RawEvidence、兩份內容完全相同的 Document。
    let raw_a = collect_once(&collector, &connector).await;
    let event_a = normalize_and_capture(&normalizer, &normalized_probe, raw_a).await;
    let raw_b = collect_once(&collector, &connector).await;
    let event_b = normalize_and_capture(&normalizer, &normalized_probe, raw_b).await;
    let a = single_document(&event_a);
    let b = single_document(&event_b);
    assert_ne!(a, b, "兩次採集應該產生兩份 Document");

    let dedup = deduplicator(&stack, metrics.clone());
    let first_a = dedup
        .handle_payload(&event_a.payload)
        .await
        .expect("dedup A");
    assert!(
        matches!(first_a.as_slice(), [DedupOutcome::Canonical { .. }]),
        "先到的是 canonical，實際 {first_a:?}"
    );
    let first_b = dedup
        .handle_payload(&event_b.payload)
        .await
        .expect("dedup B");
    let group_id = match first_b.as_slice() {
        [
            DedupOutcome::Duplicate {
                canonical_object_id,
                group_id,
                ..
            },
        ] => {
            assert_eq!(*canonical_object_id, a, "B 應該指向 A");
            *group_id
        }
        other => panic!("B 應該被判成重複，實際 {other:?}"),
    };

    let before = duplicate_groups_of(&stack.pg, a).await;
    assert_eq!(
        before.len(),
        1,
        "一份重複文件應該只有一個 group，實際 {before:?}"
    );

    // ---- 模擬 crash：group 與 Document 都寫好了，claim 還沒寫 ----
    let deleted = delete_claim(&stack.pg, b, ACTION_DEDUPLICATED).await;
    assert_eq!(
        deleted, 1,
        "應該剛好刪掉一列 deduplicated claim。刪到 0 列代表沒有進入 crash window"
    );

    // ---- 重送同一則 object.normalized ----
    let replayed = dedup
        .handle_payload(&event_b.payload)
        .await
        .expect("重送 object.normalized");
    assert!(
        matches!(replayed.as_slice(), [DedupOutcome::Duplicate { .. }]),
        "claim 不在了，應該真的重跑一次並再次判成重複，實際 {replayed:?}"
    );

    let after = duplicate_groups_of(&stack.pg, a).await;
    assert_eq!(
        after.len(),
        1,
        "重跑不可以長出第二個 DuplicateGroup——靠的是 v5 group id 的 upsert，不是 claim。\
         實際 {after:?}"
    );
    assert_eq!(
        after[0].id, group_id,
        "group id 必須與第一次相同（UUID v5 由 member document id 推導）"
    );
    assert_eq!(after[0].member_object_id, Some(b));

    let doc_b = stack
        .pg
        .get_document(b)
        .await
        .expect("query")
        .expect("B 還在");
    assert_eq!(doc_b.duplicate_of, Some(a), "重跑不該改變指向");
    let doc_a = stack
        .pg
        .get_document(a)
        .await
        .expect("query")
        .expect("A 還在");
    assert!(
        doc_a.duplicate_of.is_none(),
        "canonical 不可以在重跑之後變成別人的重複"
    );
}

// ---------------------------------------------------------------------------
// 3. entity-worker 的 crash window
// ---------------------------------------------------------------------------

/// collect → normalize → dedup（canonical，發出 `dedup.completed`）→ 抽取
/// → 刪掉 `entity_extracted` claim → 重送同一則 `dedup.completed`。
///
/// 斷言：Entity（id 集合）、EntityExtraction、Relationship 的數量都不變。
/// 這裡靠的是四種衍生 id 都是 UUID v5（由自然鍵推導）+ 依主鍵 upsert。
#[tokio::test]
async fn acceptance_f_entity_worker_crash_replay_keeps_entity_counts() {
    let stack = connect_stack().await;
    let run = Uuid::now_v7();

    let platform = format!("acc-f-ew-{}", run.simple());
    let guid = format!("acc-f-ew-guid-{}", run.simple());
    let link = format!("http://127.0.0.1/acc-f-ew/{}", run.simple());
    let feed_url = serve_body(rss_body(
        &guid,
        &link,
        &format!("Advisory {}", run.simple()),
        &article(run),
    ))
    .await;
    let source = seed_source(&stack.pg, Some(&platform), Some(&feed_url)).await;
    let connector = seed_connector(&stack.pg, &source, &feed_url).await;

    let producer =
        Arc::new(EventProducer::connect(&stack.brokers, "acceptance-f-ew").expect("producer"));
    let normalized_probe = probe_consumer(&stack.brokers, EventTopic::ObjectNormalized);
    let dedup_probe = probe_consumer(&stack.brokers, EventTopic::DedupCompleted);
    tokio::time::sleep(ASSIGN_WAIT).await;

    let metrics = MetricsRegistry::new();
    let collector = collector_runner(&stack, producer.clone());
    let normalizer = normalizer_with_producer(&stack, metrics.clone(), producer.clone());
    let dedup = deduplicator_with_producer(&stack, metrics.clone(), producer.clone());

    let raw_id = collect_once(&collector, &connector).await;
    let normalized = normalize_and_capture(&normalizer, &normalized_probe, raw_id).await;
    let document_id = single_document(&normalized);

    let outcomes = dedup
        .handle_payload(&normalized.payload)
        .await
        .expect("dedup");
    assert!(
        matches!(outcomes.as_slice(), [DedupOutcome::Canonical { .. }]),
        "這份是這次 run 唯一的一份，應該是 canonical，實際 {outcomes:?}"
    );
    // deduplicator 真的發出去的那一則 dedup.completed。
    let completed = wait_for_event(&dedup_probe, "document_id", &document_id.to_string()).await;
    assert_eq!(completed.payload["is_duplicate"], serde_json::json!(false));

    let worker = entity_worker(&stack, metrics.clone());
    let first = worker
        .handle_payload(&completed.payload)
        .await
        .expect("第一次抽取");
    let ExtractOutcome::Extracted {
        entity_count,
        extraction_count,
        relationship_count,
        ..
    } = first
    else {
        panic!("預期 Extracted，得到 {first:?}");
    };
    assert!(
        entity_count > 0 && extraction_count > 0 && relationship_count > 0,
        "fixture 內含 CVE／domain／IP／email／hash，不該抽不到東西：\
         entity={entity_count} extraction={extraction_count} relationship={relationship_count}"
    );

    let before_extractions = extractions_of(&stack.pg, document_id).await;
    let before_relationships = relationships_of(&stack.pg, document_id).await;
    let before_entities = entity_ids(&before_extractions);

    // ---- 模擬 crash：Entity／Extraction／Relationship 都寫好了，claim 還沒寫 ----
    let deleted = delete_claim(&stack.pg, document_id, ACTION_ENTITY_EXTRACTED).await;
    assert_eq!(
        deleted, 1,
        "應該剛好刪掉一列 entity_extracted claim。刪到 0 列代表沒有進入 crash window"
    );

    // ---- 重送同一則 dedup.completed ----
    let replayed = worker
        .handle_payload(&completed.payload)
        .await
        .expect("重送 dedup.completed");
    assert!(
        matches!(replayed, ExtractOutcome::Extracted { .. }),
        "claim 不在了，應該真的重跑一次，實際 {replayed:?}"
    );

    let after_extractions = extractions_of(&stack.pg, document_id).await;
    let after_relationships = relationships_of(&stack.pg, document_id).await;
    let after_entities = entity_ids(&after_extractions);

    assert_eq!(
        before_extractions.len(),
        after_extractions.len(),
        "重跑不可產生重複的 EntityExtraction——靠的是 v5 id + 主鍵 upsert，不是 claim"
    );
    assert_eq!(
        before_relationships.len(),
        after_relationships.len(),
        "重跑不可產生重複的 Relationship"
    );
    assert_eq!(
        before_entities, after_entities,
        "重跑必須落在同一組 Entity 上（自然鍵 → v5 id）"
    );
}

// ---------------------------------------------------------------------------
// 測試自己的小工具
// ---------------------------------------------------------------------------

/// 正規化一筆 RawEvidence，並取回它**實際發出去**的那一則 `object.normalized`。
async fn normalize_and_capture(
    normalizer: &normalizer::Normalizer,
    probe: &core_events::EventConsumer,
    raw_evidence_id: Uuid,
) -> core_events::EventEnvelope {
    let outcome = normalizer
        .normalize_raw(raw_evidence_id)
        .await
        .expect("normalize");
    assert!(
        matches!(outcome, NormalizeOutcome::Created { .. }),
        "預期 Created，得到 {outcome:?}"
    );
    wait_for_event(probe, "raw_evidence_id", &raw_evidence_id.to_string()).await
}

/// 從 `object.normalized` 的 payload 取出唯一的 document id。
fn single_document(envelope: &core_events::EventEnvelope) -> Uuid {
    let ids = envelope.payload["document_ids"]
        .as_array()
        .expect("document_ids 必須是陣列");
    assert_eq!(ids.len(), 1, "fixture 只有一則 item，實際 {ids:?}");
    ids[0]
        .as_str()
        .and_then(|s| Uuid::parse_str(s).ok())
        .expect("document_ids 的元素必須是 UUID 字串")
}

async fn duplicate_groups_of(
    pg: &storage_postgres::PostgresCanonicalStore,
    canonical: Uuid,
) -> Vec<DuplicateGroup> {
    pg.list_duplicate_groups_by_canonical(canonical, PAGE)
        .await
        .expect("query duplicate groups")
}

async fn extractions_of(
    pg: &storage_postgres::PostgresCanonicalStore,
    document_id: Uuid,
) -> Vec<EntityExtraction> {
    let rows = pg
        .list_entity_extractions_by_object(document_id, PAGE)
        .await
        .expect("query extractions");
    assert!(
        (rows.len() as u32) < PAGE,
        "取滿了一頁（{PAGE}）代表資料量超出這個測試的假設，筆數比較會失真"
    );
    rows
}

async fn relationships_of(
    pg: &storage_postgres::PostgresCanonicalStore,
    document_id: Uuid,
) -> Vec<Relationship> {
    let rows = pg
        .list_relationships_by_object(document_id, PAGE)
        .await
        .expect("query relationships");
    assert!(
        (rows.len() as u32) < PAGE,
        "取滿了一頁（{PAGE}）代表資料量超出這個測試的假設，筆數比較會失真"
    );
    rows
}

fn entity_ids(extractions: &[EntityExtraction]) -> std::collections::BTreeSet<Uuid> {
    extractions.iter().map(|e| e.entity_id).collect()
}
