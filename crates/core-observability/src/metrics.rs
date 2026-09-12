//! 記憶體 metrics registry。對齊 SPEC §24 的計數名稱，先不做遠端 exporter。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// 執行緒安全的簡易計數器／量測值。
#[derive(Debug, Clone, Default)]
pub struct MetricsRegistry {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    collected_total: AtomicU64,
    raw_bytes: AtomicU64,
    duplicate_total: AtomicU64,
    documents_examined: AtomicU64,
    failed_jobs: AtomicU64,
    connector_errors: AtomicU64,
    queue_depth: AtomicU64,
    processing_latency_ms_sum: AtomicU64,
    processing_latency_count: AtomicU64,
    search_latency_ms_sum: AtomicU64,
    search_latency_count: AtomicU64,
    extra: Mutex<BTreeMap<String, u64>>,
}

impl MetricsRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn inc_collected(&self, n: u64) {
        self.inner.collected_total.fetch_add(n, Ordering::Relaxed);
    }

    pub fn add_raw_bytes(&self, n: u64) {
        self.inner.raw_bytes.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_duplicate(&self, n: u64) {
        self.inner.duplicate_total.fetch_add(n, Ordering::Relaxed);
    }

    /// 去重階段實際檢查過幾份 Document。[`Self::duplicate_rate`] 的分母。
    ///
    /// **不要用 `collected_total` 當分母**：那是採集端的計數，跟 deduplicator
    /// 不在同一個行程裡，在 deduplicator 的 registry 裡永遠是 0——
    /// 重複率會靜默變成恆等於 0，看起來就像「完全沒有重複」。
    pub fn inc_documents_examined(&self, n: u64) {
        self.inner
            .documents_examined
            .fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_failed_jobs(&self, n: u64) {
        self.inner.failed_jobs.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_connector_errors(&self, n: u64) {
        self.inner.connector_errors.fetch_add(n, Ordering::Relaxed);
    }

    pub fn set_queue_depth(&self, n: u64) {
        self.inner.queue_depth.store(n, Ordering::Relaxed);
    }

    pub fn observe_processing_latency_ms(&self, ms: u64) {
        self.inner
            .processing_latency_ms_sum
            .fetch_add(ms, Ordering::Relaxed);
        self.inner
            .processing_latency_count
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn observe_search_latency_ms(&self, ms: u64) {
        self.inner
            .search_latency_ms_sum
            .fetch_add(ms, Ordering::Relaxed);
        self.inner
            .search_latency_count
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 任意額外計數。名稱只接受 `[a-z0-9_]`。
    pub fn inc(&self, name: &str, n: u64) {
        if !is_metric_name(name) {
            tracing::warn!(name, "忽略不合法的 metric 名稱");
            return;
        }
        let Ok(mut extra) = self.inner.extra.lock() else {
            return;
        };
        *extra.entry(name.to_string()).or_insert(0) += n;
    }

    /// Prometheus text exposition（非正式 parser，給 `/metrics` 用）。
    #[must_use]
    pub fn render_prometheus(&self) -> String {
        let mut out = String::new();
        push_counter(
            &mut out,
            "osint_collected_total",
            self.inner.collected_total.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "osint_raw_bytes_total",
            self.inner.raw_bytes.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "osint_duplicate_total",
            self.inner.duplicate_total.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "osint_documents_examined_total",
            self.inner.documents_examined.load(Ordering::Relaxed),
        );
        // SPEC §24 的 `duplicate rate`。算好再輸出而不是叫看板自己相除：
        // 分母是哪一個計數器是**這裡**的定義，散到每個儀表板去各算各的遲早分岔。
        push_gauge_f64(&mut out, "osint_duplicate_rate", self.duplicate_rate());
        push_counter(
            &mut out,
            "osint_failed_jobs_total",
            self.inner.failed_jobs.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "osint_connector_errors_total",
            self.inner.connector_errors.load(Ordering::Relaxed),
        );
        push_gauge(
            &mut out,
            "osint_queue_depth",
            self.inner.queue_depth.load(Ordering::Relaxed),
        );
        let proc_sum = self.inner.processing_latency_ms_sum.load(Ordering::Relaxed);
        let proc_count = self.inner.processing_latency_count.load(Ordering::Relaxed);
        push_counter(&mut out, "osint_processing_latency_ms_sum", proc_sum);
        push_counter(&mut out, "osint_processing_latency_count", proc_count);
        let search_sum = self.inner.search_latency_ms_sum.load(Ordering::Relaxed);
        let search_count = self.inner.search_latency_count.load(Ordering::Relaxed);
        push_counter(&mut out, "osint_search_latency_ms_sum", search_sum);
        push_counter(&mut out, "osint_search_latency_count", search_count);
        if let Ok(extra) = self.inner.extra.lock() {
            for (name, value) in extra.iter() {
                push_counter(&mut out, name, *value);
            }
        }
        out
    }

    /// SPEC §24 的 duplicate rate = `duplicate_total / documents_examined`。
    ///
    /// 還沒檢查過任何 Document 時回 0——那是「沒有資料可以算」，不是「重複率是 0」。
    /// 分母刻意**不是** `collected_total`：見 [`Self::inc_documents_examined`]。
    #[must_use]
    pub fn duplicate_rate(&self) -> f64 {
        let examined = self.inner.documents_examined.load(Ordering::Relaxed);
        if examined == 0 {
            0.0
        } else {
            self.inner.duplicate_total.load(Ordering::Relaxed) as f64 / examined as f64
        }
    }
}

fn is_metric_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn push_counter(out: &mut String, name: &str, value: u64) {
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push_str(" counter\n");
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

/// 浮點 gauge。Prometheus 的 text format 接受一般十進位表示法。
fn push_gauge_f64(out: &mut String, name: &str, value: f64) {
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push_str(" gauge\n");
    out.push_str(name);
    out.push(' ');
    out.push_str(&format!("{value}"));
    out.push('\n');
}

fn push_gauge(out: &mut String, name: &str, value: u64) {
    out.push_str("# TYPE ");
    out.push_str(name);
    out.push_str(" gauge\n");
    out.push_str(name);
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_and_rate() {
        let m = MetricsRegistry::new();
        m.inc_collected(10);
        m.inc_documents_examined(10);
        m.inc_duplicate(2);
        m.add_raw_bytes(100);
        m.inc_failed_jobs(1);
        m.set_queue_depth(4);
        m.observe_processing_latency_ms(12);
        assert!((m.duplicate_rate() - 0.2).abs() < f64::EPSILON);
        let text = m.render_prometheus();
        assert!(text.contains("osint_collected_total 10"), "{text}");
        assert!(text.contains("osint_queue_depth 4"), "{text}");
        assert!(text.contains("osint_processing_latency_count 1"), "{text}");
        assert!(text.contains("osint_duplicate_rate 0.2"), "{text}");
    }

    #[test]
    fn duplicate_rate_denominator_is_examined_not_collected() {
        // 這是實際踩過的形狀：deduplicator 的 registry 裡 collected_total 永遠是 0
        // （那是 collector 行程的計數），用它當分母會讓重複率恆為 0。
        let m = MetricsRegistry::new();
        m.inc_duplicate(3);
        assert!(
            (m.duplicate_rate() - 0.0).abs() < f64::EPSILON,
            "還沒檢查過任何 Document 時應該是 0"
        );
        m.inc_documents_examined(6);
        assert!((m.duplicate_rate() - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn rejects_bad_extra_name() {
        let m = MetricsRegistry::new();
        m.inc("Not Valid", 1);
        assert!(!m.render_prometheus().contains("Not Valid"));
    }
}
