//! 慢批偵測：推論「每 token 耗時」明顯高於平常時，回報要暫停讓出 CPU。
//!
//! # 為什麼用每 token 耗時，不用每批耗時
//!
//! 2026-11-24 回填實測：語料多是短文件，一批常只有 1～2 段（~17 ms），基準被拉到 18 ms；
//! 碰到一份 8 段長文（123～246 ms）就超過基準 × 3，觸發 2 秒暫停——主機 load 0.5、完全沒壓力，
//! 吞吐卻被誤暫停壓到約 1/4（30 秒內 13 次）。批次耗時主要由 token 數決定，
//! 所以基準要以 token 正規化，長短批才能比。
//!
//! # 規則
//!
//! - token 數 < [`MIN_TOKENS`] 的批次不判定、不進基準：固定成本（HTTP 來回、tokenize）占比太高，
//!   每 token 耗時會被放大好幾倍。
//! - 前 [`WARMUP`] 個樣本只建基準、不判定。
//! - 基準是指數移動平均（α＝0.1）。慢批不併入基準——否則持續變慢時基準跟著升、永遠不觸發。

use std::sync::Mutex;

pub(crate) const MIN_TOKENS: u64 = 64;
pub(crate) const WARMUP: u32 = 5;
const ALPHA: f64 = 0.1;

pub(crate) struct SlowDetector {
    factor: f64,
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    /// 每 token 微秒的移動平均。
    baseline: f64,
    samples: u32,
}

impl SlowDetector {
    pub(crate) fn new(factor: f64) -> Self {
        Self {
            factor,
            state: Mutex::new(State::default()),
        }
    }

    /// 記一批（耗時微秒、token 數）。回 true 代表這批明顯變慢、呼叫端應暫停。
    pub(crate) fn observe(&self, elapsed_us: u64, tokens: u64) -> bool {
        if tokens < MIN_TOKENS {
            return false;
        }
        let per_token = elapsed_us as f64 / tokens as f64;
        let mut s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if s.samples < WARMUP {
            s.baseline = if s.samples == 0 {
                per_token
            } else {
                s.baseline * (1.0 - ALPHA) + per_token * ALPHA
            };
            s.samples += 1;
            return false;
        }
        if per_token > s.baseline * self.factor {
            return true;
        }
        s.baseline = s.baseline * (1.0 - ALPHA) + per_token * ALPHA;
        s.samples = s.samples.saturating_add(1);
        false
    }

    /// 目前基準（每 token 微秒，四捨五入）。給 log 用。
    pub(crate) fn baseline(&self) -> u64 {
        let s = self.state.lock().unwrap_or_else(|e| e.into_inner());
        s.baseline.round() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn warmed(factor: f64, us_per_token: u64) -> SlowDetector {
        let d = SlowDetector::new(factor);
        for _ in 0..WARMUP {
            assert!(!d.observe(us_per_token * 200, 200));
        }
        d
    }

    /// 回填實測的誤觸發情境：短批建基準後，來一個 8 倍 token 的長批，每 token 耗時相同 → 不暫停。
    #[test]
    fn long_batch_at_same_rate_does_not_pause() {
        let d = warmed(3.0, 100);
        assert!(!d.observe(100 * 1600, 1600));
    }

    /// 真的被搶 CPU：每 token 耗時變 4 倍 → 暫停；慢批不進基準，連續慢仍持續觸發。
    #[test]
    fn real_slowdown_pauses_and_does_not_raise_baseline() {
        let d = warmed(3.0, 100);
        assert!(d.observe(400 * 300, 300));
        assert!(d.observe(400 * 300, 300));
        assert_eq!(d.baseline(), 100);
    }

    /// 極短批次（固定成本主導）不判定、不進基準。
    #[test]
    fn tiny_batches_are_ignored() {
        let d = warmed(3.0, 100);
        assert!(!d.observe(50_000, 10));
        assert_eq!(d.baseline(), 100);
    }

    /// 暖機期間不判定，即使樣本差很多。
    #[test]
    fn warmup_never_pauses() {
        let d = SlowDetector::new(3.0);
        assert!(!d.observe(100 * 200, 200));
        assert!(!d.observe(10_000 * 200, 200));
    }
}
