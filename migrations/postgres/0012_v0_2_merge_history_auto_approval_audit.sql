-- V0.2 Phase 1 / ADR-012：AI 輔助自動核准（AutoApprovalEvaluator）的稽核欄位。
--
-- `NULL` = 人工 merge（向下相容，既有列全部是 NULL）。
-- 非 NULL 時是完整判定依據：觸發的方法、分數、當時生效的門檻快照，
-- 以及（若走過 LLM 中間帶判斷）完整的 prompt、原始回應、解析結果、延遲。
-- 見 docs/adr/ADR-012-ai-assisted-auto-approval.md 與 crates/resolver/src/auto_approval.rs。
ALTER TABLE merge_history ADD COLUMN auto_approval_audit JSONB DEFAULT NULL;

COMMENT ON COLUMN merge_history.auto_approval_audit IS
  '自動核准的完整稽核記錄。NULL = 人工 merge。非 NULL 時 operator 一律是 "resolver:auto_confirm" 或 "stix_import:auto_confirm" 之類的服務識別字串，不是 JWT subject。';
