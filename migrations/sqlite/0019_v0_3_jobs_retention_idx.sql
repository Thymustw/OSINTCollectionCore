-- jobs 保留期限用的複合索引 (type, status, completed_at)。
-- 查詢條件與 postgres 版相同；SQLite 沒有 CONCURRENTLY，一般 CREATE INDEX 即可。
-- 時間欄位選 completed_at 的理由見 postgres 同名 migration。
CREATE INDEX IF NOT EXISTS idx_jobs_retention ON jobs ("type", status, completed_at);
