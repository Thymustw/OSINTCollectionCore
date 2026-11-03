//! AI 呼叫額度（「櫃台扣額度」）。
//!
//! ai-gateway **不依賴**任何 storage crate：這裡只定義帳戶、抽象後端、以及
//! 扣額邏輯。PostgreSQL 的實作在 `resolver::RelationalAiBudget`，由 core-api／
//! stix-worker 在組裝時注入。
//!
//! # fail-closed
//!
//! Collection 帳戶在額度後端失敗（例如 PostgreSQL 連不上）時**拒絕放行**。
//! 預算是成本防線，看不見用量時放行等於把防線拆掉。
//!
//! 目前唯一的 P0 生產呼叫端是 AutoApproval，帳戶固定為 [`AiAccount::System`]，
//! 不走 collection 額度，因此 P0 互動路徑不會因為額度 DB 失敗而全掛。
//! 若未來有 P0 也用 Collection 帳戶，fail-closed 會讓該互動在 DB 異常時被拒——
//! 那是刻意的，不要改成 fail-open。

use std::collections::HashMap;
use std::future::Future;
use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{NaiveDate, Utc};
use uuid::Uuid;

use crate::AiGatewayError;

/// 這次 AI 呼叫要記到哪個帳戶。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AiAccount {
    /// Discovery（或其他知道 collection 的路徑）。
    ///
    /// 扣該 collection 的 `daily_ai_budget`；若 `run_id` 有值，再扣行程內的
    /// `max_ai_calls_per_run`。
    ///
    /// `run_id` 計數只活在本行程：前提是一個 Discovery run 是單一 job、跑在
    /// 單一行程裡。行程重啟後 per-run 計數歸零，daily 額度仍由資料庫守住。
    Collection {
        collection_id: Uuid,
        run_id: Option<Uuid>,
    },
    /// 系統內部呼叫。目前唯一生產呼叫端是 AutoApproval（entity resolution）。
    /// 不扣任何 collection 額度，只受全域 admission／並發上限約束。
    System,
}

/// 額度查詢／扣除的抽象。用於 `dyn AiQuota`，所以走 `async_trait`。
#[async_trait]
pub trait AiQuota: Send + Sync {
    /// 嘗試為這次呼叫扣 1 次額度。成功才准許後續 admission／HTTP。
    async fn try_consume(&self, account: &AiAccount) -> Result<(), AiGatewayError>;
}

/// 測試用：所有帳戶都放行。生產組裝不該用這個——Collection 帳戶會變成無額度。
#[derive(Debug, Clone, Copy, Default)]
pub struct UnlimitedQuota;

#[async_trait]
impl AiQuota for UnlimitedQuota {
    async fn try_consume(&self, _account: &AiAccount) -> Result<(), AiGatewayError> {
        Ok(())
    }
}

/// 未注入額度後端時的預設：System 放行，Collection fail-closed。
///
/// 避免組裝端漏接 `StoreAiQuota` 時，Collection 帳戶靜默變成無額度。
pub(crate) struct FailClosedIfCollection;

#[async_trait]
impl AiQuota for FailClosedIfCollection {
    async fn try_consume(&self, account: &AiAccount) -> Result<(), AiGatewayError> {
        match account {
            AiAccount::System => Ok(()),
            AiAccount::Collection { collection_id, .. } => Err(AiGatewayError::QuotaUnavailable {
                message: format!(
                    "AI 閘道未注入額度後端，已拒絕 collection `{collection_id}` 的呼叫\
                     （fail-closed，避免在看不見用量時放行）。請在組裝 OpenAiCompatibleLlmProvider \
                     時傳入 StoreAiQuota"
                ),
            }),
        }
    }
}

/// 一個 collection 的 AI 額度上限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AiBudgetLimits {
    pub daily_ai_budget: i64,
    pub max_ai_calls_per_run: i32,
}

/// [`CollectionAiBudget::try_consume_daily`] 的結果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DailyConsume {
    pub allowed: bool,
    pub used_after: i64,
}

/// 讀取／扣除 collection 日額度的後端。實作放在呼叫端 crate，不要讓
/// ai-gateway 依賴 storage。
///
/// 這個 trait 不當 `dyn` 用，但 [`StoreAiQuota::try_consume`] 走 `async_trait`
/// 需要 `Send` Future，所以這裡用 RPIT + `Send` 而不是裸 `async fn`。
pub trait CollectionAiBudget: Send + Sync {
    /// 沒有設定列時回 `None`——[`StoreAiQuota`] 會改用與
    /// `CollectionBudget::conservative_default` 相同的數字（`daily_ai_budget=500`、
    /// `max_ai_calls_per_run=50`）。後端也可以自己套用預設後回 `Some`。
    fn load_limits(
        &self,
        collection_id: Uuid,
    ) -> impl Future<Output = Result<Option<AiBudgetLimits>, AiGatewayError>> + Send;

    fn try_consume_daily(
        &self,
        collection_id: Uuid,
        date: NaiveDate,
        by: i64,
        budget: i64,
    ) -> impl Future<Output = Result<DailyConsume, AiGatewayError>> + Send;
}

/// 對齊 `core_model::CollectionBudget::conservative_default`。
/// ai-gateway 不依賴 core-model，這兩個數字必須跟那邊保持一致。
const DEFAULT_DAILY_AI_BUDGET: i64 = 500;
const DEFAULT_MAX_AI_CALLS_PER_RUN: i32 = 50;

/// 以 [`CollectionAiBudget`] 為後端的額度實作。
///
/// per-run 計數放行程內（`Mutex<HashMap>`）：一個 Discovery run 是單一 job、
/// 單一行程，不跨行程持久化。若這個前提被打破（例如同一 `run_id` 被兩個
/// worker 同時跑），per-run 上限會各自計算、可能合計超額——那時才需要改成
/// 寫進資料庫，不要在前提仍成立時先做持久化。
pub struct StoreAiQuota<B> {
    backend: B,
    per_run: Mutex<HashMap<Uuid, i32>>,
}

impl<B: CollectionAiBudget> StoreAiQuota<B> {
    #[must_use]
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            per_run: Mutex::new(HashMap::new()),
        }
    }

    fn try_reserve_run(map: &mut HashMap<Uuid, i32>, run_id: Uuid, max: i32) -> Result<i32, i32> {
        let used = map.entry(run_id).or_insert(0);
        if *used >= max {
            return Err(*used);
        }
        *used += 1;
        Ok(*used)
    }

    fn release_run(map: &mut HashMap<Uuid, i32>, run_id: Uuid) {
        if let Some(used) = map.get_mut(&run_id) {
            *used = used.saturating_sub(1);
        }
    }
}

#[async_trait]
impl<B: CollectionAiBudget> AiQuota for StoreAiQuota<B> {
    async fn try_consume(&self, account: &AiAccount) -> Result<(), AiGatewayError> {
        let AiAccount::Collection {
            collection_id,
            run_id,
        } = account
        else {
            return Ok(());
        };

        let limits = match self.backend.load_limits(*collection_id).await? {
            Some(limits) => limits,
            None => AiBudgetLimits {
                daily_ai_budget: DEFAULT_DAILY_AI_BUDGET,
                max_ai_calls_per_run: DEFAULT_MAX_AI_CALLS_PER_RUN,
            },
        };

        // 先佔 per-run 再扣 daily：daily 是持久化計數，扣了就拿不回來。
        // 若先扣 daily 再發現 per-run 已滿，會白白耗掉 1 次日額度。
        // （偏離派工「先 daily 再 per-run」的字面順序，理由是避免額度洩漏。）
        let reserved_run = if let Some(run_id) = *run_id {
            let mut map = self.per_run.lock().unwrap_or_else(|err| err.into_inner());
            match Self::try_reserve_run(&mut map, run_id, limits.max_ai_calls_per_run) {
                Ok(_) => Some(run_id),
                Err(used) => {
                    return Err(AiGatewayError::QuotaExceeded {
                        message: format!(
                            "本 run 已達 max_ai_calls_per_run（已用 {used}/{}）。\
                             這個上限是單次 Discovery job 的 AI 呼叫數，不會因為重試而重置；\
                             請等下次 run，或把該 collection 的 max_ai_calls_per_run 調高",
                            limits.max_ai_calls_per_run
                        ),
                    });
                }
            }
        } else {
            None
        };

        let date = Utc::now().date_naive();
        let consume = match self
            .backend
            .try_consume_daily(*collection_id, date, 1, limits.daily_ai_budget)
            .await
        {
            Ok(consume) => consume,
            Err(err) => {
                if let Some(run_id) = reserved_run {
                    let mut map = self.per_run.lock().unwrap_or_else(|e| e.into_inner());
                    Self::release_run(&mut map, run_id);
                }
                return Err(err);
            }
        };

        if consume.allowed {
            return Ok(());
        }

        if let Some(run_id) = reserved_run {
            let mut map = self.per_run.lock().unwrap_or_else(|err| err.into_inner());
            Self::release_run(&mut map, run_id);
        }
        Err(AiGatewayError::QuotaExceeded {
            message: format!(
                "今日 AI 呼叫已達 daily_ai_budget（已用 {}/{}）。\
                 SPEC_V0.3 §21 E 要求超過額度後停止新增 AI request，不會重試。\
                 請等 UTC 日期切換，或把該 collection 的 daily_ai_budget 調高",
                consume.used_after, limits.daily_ai_budget
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    struct MemoryBudget {
        limits: HashMap<Uuid, AiBudgetLimits>,
        used: Mutex<HashMap<Uuid, i64>>,
        fail_load: bool,
        fail_consume: bool,
        consume_calls: AtomicU64,
    }

    impl MemoryBudget {
        fn new() -> Self {
            Self {
                limits: HashMap::new(),
                used: Mutex::new(HashMap::new()),
                fail_load: false,
                fail_consume: false,
                consume_calls: AtomicU64::new(0),
            }
        }
    }

    impl CollectionAiBudget for MemoryBudget {
        async fn load_limits(
            &self,
            collection_id: Uuid,
        ) -> Result<Option<AiBudgetLimits>, AiGatewayError> {
            if self.fail_load {
                return Err(AiGatewayError::QuotaUnavailable {
                    message: "模擬資料庫讀取失敗".into(),
                });
            }
            Ok(self.limits.get(&collection_id).copied())
        }

        async fn try_consume_daily(
            &self,
            collection_id: Uuid,
            _date: NaiveDate,
            by: i64,
            budget: i64,
        ) -> Result<DailyConsume, AiGatewayError> {
            self.consume_calls.fetch_add(1, Ordering::SeqCst);
            if self.fail_consume {
                return Err(AiGatewayError::QuotaUnavailable {
                    message: "模擬資料庫扣額失敗".into(),
                });
            }
            let mut used = self.used.lock().expect("memory budget");
            let current = used.entry(collection_id).or_insert(0);
            if *current + by > budget {
                return Ok(DailyConsume {
                    allowed: false,
                    used_after: *current,
                });
            }
            *current += by;
            Ok(DailyConsume {
                allowed: true,
                used_after: *current,
            })
        }
    }

    fn col(id: Uuid) -> AiAccount {
        AiAccount::Collection {
            collection_id: id,
            run_id: None,
        }
    }

    fn col_run(id: Uuid, run: Uuid) -> AiAccount {
        AiAccount::Collection {
            collection_id: id,
            run_id: Some(run),
        }
    }

    #[tokio::test]
    async fn system_account_does_not_touch_backend() {
        let backend = MemoryBudget::new();
        let quota = StoreAiQuota::new(backend);
        quota
            .try_consume(&AiAccount::System)
            .await
            .expect("System 必須放行");
        assert_eq!(quota.backend.consume_calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn collection_stops_at_daily_limit() {
        let id = Uuid::now_v7();
        let mut backend = MemoryBudget::new();
        backend.limits.insert(
            id,
            AiBudgetLimits {
                daily_ai_budget: 2,
                max_ai_calls_per_run: 50,
            },
        );
        let quota = StoreAiQuota::new(backend);
        quota.try_consume(&col(id)).await.expect("第 1 次");
        quota.try_consume(&col(id)).await.expect("第 2 次");
        let err = quota
            .try_consume(&col(id))
            .await
            .expect_err("第 3 次必須被拒");
        assert!(
            matches!(err, AiGatewayError::QuotaExceeded { .. }),
            "期望 QuotaExceeded，實際 {err:?}"
        );
        assert!(err.to_string().contains("daily_ai_budget"), "{err}");
        assert_eq!(quota.backend.consume_calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn two_collections_do_not_share_budget() {
        let a = Uuid::now_v7();
        let b = Uuid::now_v7();
        let mut backend = MemoryBudget::new();
        let limits = AiBudgetLimits {
            daily_ai_budget: 1,
            max_ai_calls_per_run: 50,
        };
        backend.limits.insert(a, limits);
        backend.limits.insert(b, limits);
        let quota = StoreAiQuota::new(backend);
        quota.try_consume(&col(a)).await.expect("A 第 1 次");
        quota
            .try_consume(&col(a))
            .await
            .expect_err("A 額度 1，第 2 次應被拒");
        quota
            .try_consume(&col(b))
            .await
            .expect("B 不該被 A 的用量影響");
    }

    #[tokio::test]
    async fn per_run_limit_is_in_process() {
        let id = Uuid::now_v7();
        let run = Uuid::now_v7();
        let mut backend = MemoryBudget::new();
        backend.limits.insert(
            id,
            AiBudgetLimits {
                daily_ai_budget: 100,
                max_ai_calls_per_run: 2,
            },
        );
        let quota = StoreAiQuota::new(backend);
        quota.try_consume(&col_run(id, run)).await.expect("run 1");
        quota.try_consume(&col_run(id, run)).await.expect("run 2");
        let err = quota
            .try_consume(&col_run(id, run))
            .await
            .expect_err("run 第 3 次");
        assert!(matches!(err, AiGatewayError::QuotaExceeded { .. }));
        assert!(err.to_string().contains("max_ai_calls_per_run"), "{err}");
        // per-run 先擋，不該再打 daily。
        assert_eq!(quota.backend.consume_calls.load(Ordering::SeqCst), 2);
        // 同一個 collection、不同 run 不受影響。
        let other = Uuid::now_v7();
        quota
            .try_consume(&col_run(id, other))
            .await
            .expect("另一個 run 應放行");
    }

    #[tokio::test]
    async fn load_failure_is_fail_closed_and_not_quota_exceeded() {
        let id = Uuid::now_v7();
        let mut backend = MemoryBudget::new();
        backend.fail_load = true;
        let quota = StoreAiQuota::new(backend);
        let err = quota
            .try_consume(&col(id))
            .await
            .expect_err("DB 失敗必須拒");
        assert!(
            matches!(err, AiGatewayError::QuotaUnavailable { .. }),
            "期望 QuotaUnavailable，實際 {err:?}"
        );
        assert!(!matches!(err, AiGatewayError::Transient { .. }));
    }

    #[tokio::test]
    async fn consume_failure_releases_per_run_reservation() {
        let id = Uuid::now_v7();
        let run = Uuid::now_v7();
        let mut backend = MemoryBudget::new();
        backend.limits.insert(
            id,
            AiBudgetLimits {
                daily_ai_budget: 10,
                max_ai_calls_per_run: 1,
            },
        );
        backend.fail_consume = true;
        let quota = StoreAiQuota::new(backend);
        let err = quota
            .try_consume(&col_run(id, run))
            .await
            .expect_err("consume 失敗");
        assert!(matches!(err, AiGatewayError::QuotaUnavailable { .. }));
        // 釋放後，修掉失敗再呼叫應能再佔到唯一的 per-run 名額。
        // 這裡後端仍 fail_consume，所以改用直接看 map：名額必須歸還。
        let map = quota.per_run.lock().expect("per_run");
        assert_eq!(map.get(&run).copied().unwrap_or(0), 0);
    }

    #[tokio::test]
    async fn missing_budget_uses_conservative_default_of_500() {
        let id = Uuid::now_v7();
        let backend = MemoryBudget::new(); // limits 沒有這筆 → None
        let quota = StoreAiQuota::new(backend);
        for _ in 0..500 {
            quota
                .try_consume(&col(id))
                .await
                .expect("保守預設 daily_ai_budget=500，前 500 次應放行");
        }
        let err = quota
            .try_consume(&col(id))
            .await
            .expect_err("第 501 次應被拒");
        assert!(matches!(err, AiGatewayError::QuotaExceeded { .. }));
        assert!(err.to_string().contains("500"), "{err}");
    }

    #[tokio::test]
    async fn unlimited_quota_allows_collection() {
        let quota = UnlimitedQuota;
        quota
            .try_consume(&col(Uuid::now_v7()))
            .await
            .expect("UnlimitedQuota 放行 Collection");
    }

    #[tokio::test]
    async fn fail_closed_if_collection_rejects_collection_allows_system() {
        let quota = FailClosedIfCollection;
        quota
            .try_consume(&AiAccount::System)
            .await
            .expect("System 放行");
        let err = quota
            .try_consume(&col(Uuid::now_v7()))
            .await
            .expect_err("未注入後端時 Collection 必須 fail-closed");
        assert!(matches!(err, AiGatewayError::QuotaUnavailable { .. }));
    }

    #[tokio::test]
    async fn concurrent_per_run_does_not_exceed() {
        let id = Uuid::now_v7();
        let run = Uuid::now_v7();
        let mut backend = MemoryBudget::new();
        backend.limits.insert(
            id,
            AiBudgetLimits {
                daily_ai_budget: 100,
                max_ai_calls_per_run: 3,
            },
        );
        let quota = Arc::new(StoreAiQuota::new(backend));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let quota = Arc::clone(&quota);
            handles.push(tokio::spawn(async move {
                quota.try_consume(&col_run(id, run)).await
            }));
        }
        let mut ok = 0usize;
        let mut denied = 0usize;
        for handle in handles {
            match handle.await.expect("join") {
                Ok(()) => ok += 1,
                Err(AiGatewayError::QuotaExceeded { .. }) => denied += 1,
                Err(err) => panic!("不該出現其他錯誤：{err:?}"),
            }
        }
        assert_eq!(ok, 3, "per-run=3，8 個並發應剛好 3 個成功，實際 {ok}");
        assert_eq!(denied, 5);
    }
}
