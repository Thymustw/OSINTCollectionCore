//! 把 [`storage_core::RelationalStore`] 接到 [`ai_gateway::CollectionAiBudget`]。
//!
//! 放在 resolver 而不是 ai-gateway 或 storage-postgres：ai-gateway 不能依賴
//! storage crate（會把 reqwest 拉進 storage，也違反「閘道不認得後端」）；
//! storage-postgres 也不該依賴 ai-gateway。core-api 與 stix-worker 都已經
//! 依賴 resolver，組裝點從這裡拿 [`RelationalAiBudget`] 即可。

use ai_gateway::{AiBudgetLimits, AiGatewayError, CollectionAiBudget, DailyConsume};
use chrono::{NaiveDate, Utc};
use core_model::CollectionBudget;
use storage_core::{RelationalStore, StorageError};
use uuid::Uuid;

/// 以任意 [`RelationalStore`] 實作 collection AI 額度後端。
#[derive(Clone)]
pub struct RelationalAiBudget<S> {
    store: S,
}

impl<S> RelationalAiBudget<S> {
    #[must_use]
    pub fn new(store: S) -> Self {
        Self { store }
    }
}

impl<S: RelationalStore> CollectionAiBudget for RelationalAiBudget<S> {
    async fn load_limits(
        &self,
        collection_id: Uuid,
    ) -> Result<Option<AiBudgetLimits>, AiGatewayError> {
        match self.store.get_collection_budget(collection_id).await {
            Ok(Some(budget)) => Ok(Some(AiBudgetLimits {
                daily_ai_budget: budget.daily_ai_budget,
                max_ai_calls_per_run: budget.max_ai_calls_per_run,
            })),
            Ok(None) => {
                // 沒有設定列時套用與 `CollectionBudget::conservative_default`
                // 相同的數字，回 Some 讓 StoreAiQuota 不必再猜一次。
                let fallback = CollectionBudget::conservative_default(collection_id, Utc::now());
                Ok(Some(AiBudgetLimits {
                    daily_ai_budget: fallback.daily_ai_budget,
                    max_ai_calls_per_run: fallback.max_ai_calls_per_run,
                }))
            }
            Err(err) => Err(map_store_error(err)),
        }
    }

    async fn try_consume_daily(
        &self,
        collection_id: Uuid,
        date: NaiveDate,
        by: i64,
        budget: i64,
    ) -> Result<DailyConsume, AiGatewayError> {
        match self
            .store
            .try_consume_daily_ai_budget(collection_id, date, by, budget)
            .await
        {
            Ok(consumption) => Ok(DailyConsume {
                allowed: consumption.allowed,
                used_after: consumption.used_after,
            }),
            Err(err) => Err(map_store_error(err)),
        }
    }
}

fn map_store_error(err: StorageError) -> AiGatewayError {
    AiGatewayError::QuotaUnavailable {
        message: format!(
            "讀取或扣除 collection AI 額度失敗：{err}。\
             預算是成本防線，看不見用量時拒絕放行（fail-closed）。\
             請檢查 PostgreSQL 是否可用後再重試"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ai_gateway::{AiAccount, AiQuota, StoreAiQuota};
    use chrono::Utc;
    use core_model::{Collection, CollectionBudget};
    use storage_core::RelationalStore;
    use storage_sqlite::SqliteEmbeddedStore;
    use uuid::Uuid;

    async fn open_store() -> SqliteEmbeddedStore {
        let path = std::env::temp_dir().join(format!("osint-ai-quota-{}.sqlite", Uuid::now_v7()));
        let store = SqliteEmbeddedStore::connect(&path)
            .await
            .expect("開 sqlite");
        store.migrate().await.expect("migrate");
        store
    }

    async fn seed_collection(store: &SqliteEmbeddedStore) -> Uuid {
        let id = Uuid::now_v7();
        let now = Utc::now();
        store
            .put_collection(&Collection {
                id,
                name: "quota-test".into(),
                workspace_id: None,
                description: None,
                status: "active".into(),
                priority: 0,
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("put collection");
        id
    }

    #[tokio::test]
    async fn sqlite_backend_stops_at_daily_budget() {
        let store = open_store().await;
        let collection_id = seed_collection(&store).await;
        let now = Utc::now();
        store
            .put_collection_budget(&CollectionBudget {
                collection_id,
                max_candidates_per_run: 10,
                max_requests_per_run: 10,
                max_ai_calls_per_run: 50,
                max_depth: 1,
                daily_request_budget: 100,
                daily_ai_budget: 2,
                created_at: now,
                updated_at: now,
            })
            .await
            .expect("put budget");

        let quota = StoreAiQuota::new(RelationalAiBudget::new(store.clone()));
        let account = AiAccount::Collection {
            collection_id,
            run_id: None,
        };
        quota.try_consume(&account).await.expect("第 1 次");
        quota.try_consume(&account).await.expect("第 2 次");
        let err = quota
            .try_consume(&account)
            .await
            .expect_err("第 3 次必須被拒");
        assert!(matches!(err, AiGatewayError::QuotaExceeded { .. }));

        let usage = store
            .get_daily_usage(collection_id, Utc::now().date_naive())
            .await
            .expect("usage");
        assert_eq!(usage.ai_calls_used, 2);
        assert_eq!(usage.requests_used, 0, "AI 額度不該動到 request 計數");
    }

    #[tokio::test]
    async fn missing_budget_row_uses_conservative_default() {
        let store = open_store().await;
        let collection_id = seed_collection(&store).await;
        let limits = RelationalAiBudget::new(store)
            .load_limits(collection_id)
            .await
            .expect("load")
            .expect("沒設定列也必須回 Some（套用保守預設）");
        let fallback = CollectionBudget::conservative_default(collection_id, Utc::now());
        assert_eq!(limits.daily_ai_budget, fallback.daily_ai_budget);
        assert_eq!(limits.max_ai_calls_per_run, fallback.max_ai_calls_per_run);
    }
}
