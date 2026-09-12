//! SPEC §26 Acceptance F：**consumer crash 後重送 event 不建立 duplicate canonical object。**
//!
//! # 這裡在驗的是一條從來沒有被測過的推論鏈
//!
//! 三個 consumer 都是 **write-then-claim**（先寫資料，最後才佔 provenance 的
//! unique claim；理由見 `docs/developer/collector-normalizer.md`）。這個順序刻意
//! 接受一個 crash window：資料寫完、claim 還沒寫就死掉，重跑會再寫一次。
//!
//! 文件裡寫的是「重跑產生的重複由下游收斂」——normalizer 的重複 Document 由
//! deduplicator 收掉，deduplicator 與 entity-worker 的重跑則靠決定性的 v5 id
//! 不產生第二列。**那整條推論在 Phase 7b 之前沒有任何測試。**
//!
//! 每個測試都用 SQL 直接刪掉 claim 來模擬 crash window（那是那個狀態的定義），
//! 然後重送**真正發出去的那一則事件 payload**，不是自己組一個形狀相近的 JSON。
//!
//! 需要本機 Docker：Postgres／MinIO／Redpanda。不打外網。

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use core_events::{EventProducer, EventTopic};
use core_model::{DuplicateGroup, EntityExtraction, Relationship};
use core_observability::MetricsRegistry;
use deduplicator::{ACTION_DEDUPLICATED, DedupOutcome};
use entity_worker::{ACTION_ENTITY_EXTRACTED, ExtractOutcome};
use normalizer::{ACTION_NORMALIZED, NormalizeOutcome};
use storage_core::RelationalStore;
use uuid::Uuid;

/// 查關聯資料時一次取幾筆。測試資料遠小於這個數，取滿代表資料有問題。
const PAGE: u32 = 200;

/// 新建的 consumer group 等 partition 指派的時間。
///
/// 這個 sleep 是既有 e2e 的慣例（`normalizer/tests/e2e.rs`）：subscribe 之後
/// 立刻 produce 的話，第一次 rebalance 可能還沒完成。
const ASSIGN_WAIT: Duration = Duration::from_millis(1_500);

// ---------------------------------------------------------------------------
// 1. normalizer 的 crash window
// ---------------------------------------------------------------------------

/// collect → normalize（D1）→ 刪掉 `normalized` claim（= claim 前 crash 的狀態）
/// → 重送同一則 `raw.collected` → normalize（D2）→ deduplicator 對兩份都跑。
///
/// 斷言：
/// * D1、D2 是兩份**不同**的 Document（crash window 真的產生了重複）
/// * 其中 `duplicate_of IS NULL` 的**恰好一筆**，另一筆指向它
/// * RawEvidence **恰好一筆**——重送事件不會讓證據變兩份
#[tokio::test]
async fn acceptance_f_normalizer_crash_replay_converges_to_one_canonical() {
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

    let first = normalizer
        .handle_payload(&collected.payload)
        .await
        .expect("第一次正規化");
    let NormalizeOutcome::Created { document_ids } = first else {
        panic!("預期 Created，得到 {first:?}");
    };
    assert_eq!(document_ids.len(), 1, "fixture 只有一則 item");
    let d1 = document_ids[0];

    // ---- 模擬 crash：Document 已經寫進 DB，claim 還沒寫就死掉 ----
    let deleted = delete_claim(&stack.pg, d1, ACTION_NORMALIZED).await;
    assert_eq!(
        deleted, 1,
        "應該剛好刪掉一列 normalized claim（subject 是第一份 Document 的 id）。\
         刪到 0 列代表這個測試根本沒有進入 crash window，後面的斷言全部沒有意義"
    );

    // ---- 重送同一則 raw.collected ----
    let replayed = normalizer
        .handle_payload(&collected.payload)
        .await
        .expect("重送後重新正規化");
    let NormalizeOutcome::Created {
        document_ids: replay_ids,
    } = replayed
    else {
        panic!(
            "claim 不在了，normalizer 應該真的重跑一次並產生新的 Document，實際 {replayed:?}。\
             若這裡是 AlreadyDone，代表 crash window 的前提已經不成立"
        );
    };
    assert_eq!(replay_ids.len(), 1);
    let d2 = replay_ids[0];
    assert_ne!(
        d1, d2,
        "Document id 是 UUID v7，重跑必然產生新的 id——這就是 write-then-claim 的已知代價"
    );

    let derived = documents_of_raw_evidence(&stack.pg, raw_evidence_id).await;
    assert_eq!(
        derived.len(),
        2,
        "crash window 應該留下兩份 Document（要收斂的就是這個），實際 {derived:?}"
    );

    // ---- deduplicator 對兩份都跑 ----
    let dedup = deduplicator(&stack, metrics.clone());
    let o1 = dedup.dedup_document(d1).await.expect("dedup D1");
    let o2 = dedup.dedup_document(d2).await.expect("dedup D2");
    assert!(
        matches!(o1, DedupOutcome::Canonical { .. }),
        "先處理的那份應該是 canonical，實際 {o1:?}"
    );
    match &o2 {
        DedupOutcome::Duplicate {
            canonical_object_id,
            ..
        } => assert_eq!(
            *canonical_object_id, d1,
            "D2 應該指向 D1（同一個 platform + external_id，Stage 1）"
        ),
        other => panic!("D2 應該被判成重複，實際 {other:?}"),
    }

    // ---- 真正的驗收條件 ----
    let doc1 = stack
        .pg
        .get_document(d1)
        .await
        .expect("query")
        .expect("D1 還在");
    let doc2 = stack
        .pg
        .get_document(d2)
        .await
        .expect("query")
        .expect("D2 還在");
    let canonicals: Vec<Uuid> = [&doc1, &doc2]
        .iter()
        .filter(|d| d.duplicate_of.is_none())
        .map(|d| d.id)
        .collect();
    assert_eq!(
        canonicals,
        vec![d1],
        "crash + 重送之後，duplicate_of IS NULL 的必須恰好是一筆，而且是先到的那一份。\
         這就是 Acceptance F 的「不建立 duplicate canonical object」"
    );
    assert_eq!(doc2.duplicate_of, Some(d1), "另一筆必須指向它");

    // RawEvidence 不可變且只有一筆：重送事件不會讓證據變兩份。
    assert_eq!(
        count_raw_evidence(&stack.pg, source.id).await,
        1,
        "重送 raw.collected 不該產生第二筆 RawEvidence——證據是 collector 寫的，不是事件寫的"
    );

    // claim 也回到只有一列（重跑寫了新的那一列）。
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
