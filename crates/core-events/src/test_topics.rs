//! 測試用：每個測試一個一次性 topic namespace（KNOWN_GAPS §26 後續）。
//!
//! 以前 e2e 直接用邏輯 topic（`raw.collected`…），執行中的開發服務也會吃到測試事件：
//! normalizer 在開發 DB 找不到測試 DB 裡的 raw_evidence，寫進 `failed_events`
//! （2026-11-17 實測跑一輪全部測試 +44 列）；測試的 consumer group 也從來不刪。
//!
//! 現在：
//!
//! 1. [`TestTopics::create`] 產生 namespace `osinttest-<uuidv7>`；從它拿的 producer／consumer／
//!    lag 探針都走 `osinttest-<uuidv7>.<邏輯名稱>`，執行中的服務訂不到；
//! 2. consumer group 用 [`TestTopics::group`] 命名（同一前綴），清理只看前綴；
//! 3. drop（含 panic 展開）時刪掉這個 namespace 的所有 topic 與 group；
//! 4. SIGKILL 時 Drop 不會跑，所以每次建立先清掃逾時 30 分鐘的 `osinttest-*` topic 與 Empty group。
//!
//! 只動 `osinttest-` 開頭的名稱：正式 topic 沒有 namespace，不可能被這裡刪到。

use std::sync::Mutex;
use std::time::Duration;

use rdkafka::ClientConfig;
use rdkafka::admin::{AdminClient, AdminOptions};
use rdkafka::client::DefaultClientContext;
use uuid::Uuid;

use crate::error::EventError;
use crate::kafka::{EventConsumer, EventProducer};
use crate::lag::GroupLagProbe;
use crate::namespace::TopicNamespace;

/// 一次性 namespace 與 group 的共同前綴。
pub const TEST_TOPIC_PREFIX: &str = "osinttest-";
const STALE_AFTER: Duration = Duration::from_secs(30 * 60);
const ADMIN_TIMEOUT: Duration = Duration::from_secs(10);

/// 一個測試專用的 topic namespace。drop 時刪掉它的 topic 與 group
/// （`OSINT_TEST_KEEP_TOPICS=1` 時保留，方便除錯）。
#[must_use = "不綁成變數會立刻 drop，topic 剛建就被刪"]
pub struct TestTopics {
    brokers: String,
    namespace: TopicNamespace,
    id: Uuid,
    groups: Mutex<Vec<String>>,
}

impl TestTopics {
    /// # Panics
    /// broker 位址不是本機時 panic——只對本機 Redpanda 建／刪 topic。
    pub fn create(brokers: &str) -> Self {
        Self::try_create(brokers)
            .unwrap_or_else(|err| panic!("建立一次性 topic namespace 失敗：{err}"))
    }

    pub fn try_create(brokers: &str) -> Result<Self, EventError> {
        let local = brokers.split(',').all(|b| {
            let host = b.trim().rsplit_once(':').map_or(b.trim(), |(h, _)| h);
            matches!(host, "127.0.0.1" | "localhost" | "[::1]" | "::1")
        });
        if !local {
            return Err(EventError::Configuration {
                message: format!("TestTopics 只對本機 Redpanda 建／刪 topic，brokers=`{brokers}`"),
            });
        }
        let id = Uuid::now_v7();
        let namespace = TopicNamespace::new(format!("{TEST_TOPIC_PREFIX}{}", id.simple()))?;
        let brokers = brokers.to_string();
        let sweep_brokers = brokers.clone();
        // 清掃失敗不擋測試；在獨立執行緒跑，不管呼叫端是哪種 runtime。
        let _ = std::thread::spawn(move || {
            if let Err(err) = block_on_admin(&sweep_brokers, sweep_stale) {
                eprintln!("TestTopics：清掃舊的 {TEST_TOPIC_PREFIX}* 失敗：{err}");
            }
        })
        .join();
        Ok(Self {
            brokers,
            namespace,
            id,
            groups: Mutex::new(Vec::new()),
        })
    }

    #[must_use]
    pub fn namespace(&self) -> &TopicNamespace {
        &self.namespace
    }

    #[must_use]
    pub fn brokers(&self) -> &str {
        &self.brokers
    }

    /// 這個 namespace 底下的 consumer group 名稱：`osinttest-<uuid>-<purpose>`。
    /// 同一個 purpose 回同一個名稱（測試要「斷線重連同一個 group」時用）。
    #[must_use]
    pub fn group(&self, purpose: &str) -> String {
        let name = format!("{}-{purpose}", self.namespace.as_str());
        if let Ok(mut groups) = self.groups.lock()
            && !groups.contains(&name)
        {
            groups.push(name.clone());
        }
        name
    }

    pub fn producer(&self, source_service: &str) -> Result<EventProducer, EventError> {
        EventProducer::connect_namespaced(&self.brokers, source_service, self.namespace.clone())
    }

    /// `purpose` 決定 group 名稱（見 [`Self::group`]）；`topics` 是邏輯名稱。
    pub fn consumer(&self, purpose: &str, topics: &[&str]) -> Result<EventConsumer, EventError> {
        let group = self.group(purpose);
        EventConsumer::connect_namespaced(&self.brokers, &group, topics, self.namespace.clone())
    }

    pub fn lag_probe(&self, timeout: Duration) -> Result<GroupLagProbe, EventError> {
        GroupLagProbe::new_namespaced(&self.brokers, timeout, self.namespace.clone())
    }

    /// 邏輯名稱 → 這個 namespace 的 broker 名稱（斷言或 rpk 除錯用）。
    #[must_use]
    pub fn broker_topic(&self, logical: &str) -> String {
        self.namespace.broker_name(logical).into_owned()
    }
}

impl Drop for TestTopics {
    fn drop(&mut self) {
        if std::env::var("OSINT_TEST_KEEP_TOPICS").is_ok_and(|v| v == "1") {
            eprintln!(
                "TestTopics：OSINT_TEST_KEEP_TOPICS=1，保留 `{}.*`",
                self.namespace.as_str()
            );
            return;
        }
        let brokers = self.brokers.clone();
        let prefix = format!("{}.", self.namespace.as_str());
        let groups = self.groups.lock().map(|g| g.clone()).unwrap_or_default();
        let id = self.id;
        // 理由同 storage_postgres::TestDatabase：Drop 是同步的，在新 OS thread 開
        // current-thread runtime，不重用測試的 runtime。
        let joined = std::thread::spawn(move || {
            block_on_admin(&brokers, move |admin| {
                Box::pin(async move {
                    delete_groups(&admin, &groups).await;
                    let topics = list_topics(&admin, |name| name.starts_with(&prefix))?;
                    delete_topics(&admin, &topics).await
                })
            })
        })
        .join();
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(err)) => eprintln!(
                "TestTopics：刪 `{TEST_TOPIC_PREFIX}{}` 失敗：{err}。下次建立時會清掃逾時 {} 分鐘的",
                id.simple(),
                STALE_AFTER.as_secs() / 60
            ),
            Err(_) => eprintln!(
                "TestTopics：清理執行緒 panic，`{TEST_TOPIC_PREFIX}{}` 可能留下",
                id.simple()
            ),
        }
    }
}

type Admin = AdminClient<DefaultClientContext>;
type AdminFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), String>> + Send>>;

fn block_on_admin(brokers: &str, job: impl FnOnce(Admin) -> AdminFuture) -> Result<(), String> {
    let admin: Admin = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .map_err(|err| format!("建立 AdminClient 失敗：{err}"))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|err| format!("建立 tokio runtime 失敗：{err}"))?;
    runtime.block_on(job(admin))
}

fn list_topics(admin: &Admin, keep: impl Fn(&str) -> bool) -> Result<Vec<String>, String> {
    let metadata = admin
        .inner()
        .fetch_metadata(None, ADMIN_TIMEOUT)
        .map_err(|err| format!("讀 topic 清單失敗：{err}"))?;
    Ok(metadata
        .topics()
        .iter()
        .map(|t| t.name().to_string())
        .filter(|name| name.starts_with(TEST_TOPIC_PREFIX) && keep(name))
        .collect())
}

async fn delete_topics(admin: &Admin, topics: &[String]) -> Result<(), String> {
    // 再擋一次：只刪測試前綴。
    let refs: Vec<&str> = topics
        .iter()
        .map(String::as_str)
        .filter(|t| t.starts_with(TEST_TOPIC_PREFIX))
        .collect();
    if refs.is_empty() {
        return Ok(());
    }
    let opts = AdminOptions::new().operation_timeout(Some(ADMIN_TIMEOUT));
    let results = admin
        .delete_topics(&refs, &opts)
        .await
        .map_err(|err| format!("delete_topics 失敗：{err}"))?;
    for result in results {
        if let Err((topic, code)) = result
            && code != rdkafka::types::RDKafkaErrorCode::UnknownTopicOrPartition
        {
            return Err(format!("刪 topic `{topic}` 失敗：{code:?}"));
        }
    }
    Ok(())
}

/// 刪 group 失敗只記錄：consumer 還沒關（`NonEmptyGroup`）時刪不掉，交給下次清掃。
async fn delete_groups(admin: &Admin, groups: &[String]) {
    let refs: Vec<&str> = groups
        .iter()
        .map(String::as_str)
        .filter(|g| g.starts_with(TEST_TOPIC_PREFIX))
        .collect();
    if refs.is_empty() {
        return;
    }
    let opts = AdminOptions::new().operation_timeout(Some(ADMIN_TIMEOUT));
    match admin.delete_groups(&refs, &opts).await {
        Ok(results) => {
            for result in results {
                if let Err((group, code)) = result
                    && code != rdkafka::types::RDKafkaErrorCode::GroupIdNotFound
                {
                    eprintln!("TestTopics：刪 group `{group}` 失敗：{code:?}（下次清掃再刪）");
                }
            }
        }
        Err(err) => eprintln!("TestTopics：delete_groups 失敗：{err}"),
    }
}

/// `osinttest-<uuidv7 simple>[.-]…` → 是否早於 [`STALE_AFTER`]。不是這個格式就當成不舊（不碰）。
fn is_stale(name: &str) -> bool {
    let Some(rest) = name.strip_prefix(TEST_TOPIC_PREFIX) else {
        return false;
    };
    let Some(hex) = rest.get(..32) else {
        return false;
    };
    let Some(ts) = Uuid::parse_str(hex).ok().and_then(|id| id.get_timestamp()) else {
        return false;
    };
    let (secs, nanos) = ts.to_unix();
    let created = Duration::new(secs, nanos);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    now.saturating_sub(created) > STALE_AFTER
}

fn sweep_stale(admin: Admin) -> AdminFuture {
    Box::pin(async move {
        let groups: Vec<String> = admin
            .inner()
            .fetch_group_list(None, ADMIN_TIMEOUT)
            .map_err(|err| format!("讀 group 清單失敗：{err}"))?
            .groups()
            .iter()
            .filter(|g| g.state() == "Empty" && is_stale(g.name()))
            .map(|g| g.name().to_string())
            .collect();
        delete_groups(&admin, &groups).await;
        let topics = list_topics(&admin, is_stale)?;
        delete_topics(&admin, &topics).await
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn staleness_reads_uuidv7_and_ignores_other_names() {
        let fresh = format!(
            "{TEST_TOPIC_PREFIX}{}.raw.collected",
            Uuid::now_v7().simple()
        );
        assert!(!is_stale(&fresh));
        let old_ts = uuid::Timestamp::from_unix_time(1_600_000_000, 0, 0, 0);
        let old = format!(
            "{TEST_TOPIC_PREFIX}{}-normalizer",
            Uuid::new_v7(old_ts).simple()
        );
        assert!(is_stale(&old));
        for other in [
            "raw.collected",
            "osint-normalizer",
            "osinttest-short",
            "osinttest-",
        ] {
            assert!(!is_stale(other), "`{other}` 不該被當成可清的舊名稱");
        }
    }

    #[test]
    fn rejects_non_local_brokers() {
        assert!(TestTopics::try_create("redpanda.example:9092").is_err());
        assert!(TestTopics::try_create("127.0.0.1:9092,192.0.2.1:9092").is_err());
    }

    #[test]
    fn group_names_share_the_namespace_prefix() {
        // 不連 broker：清掃會失敗但只記錄。
        let topics = TestTopics {
            brokers: "127.0.0.1:1".into(),
            namespace: TopicNamespace::new("osinttest-x").expect("ns"),
            id: Uuid::now_v7(),
            groups: Mutex::new(Vec::new()),
        };
        assert_eq!(topics.group("normalizer"), "osinttest-x-normalizer");
        assert_eq!(topics.group("normalizer"), "osinttest-x-normalizer");
        assert_eq!(topics.groups.lock().expect("lock").len(), 1);
        assert_eq!(
            topics.broker_topic("raw.collected"),
            "osinttest-x.raw.collected"
        );
        std::mem::forget(topics); // 不要讓 Drop 去連不存在的 broker
    }
}
