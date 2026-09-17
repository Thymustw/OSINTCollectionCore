//! Token 用量 → 估計成本（SPEC_V0.3 §3 cost/token accounting）。
//!
//! `TokenUsage` 由 provider 解析出來（見 `openai.rs`），但目前沒有任何呼叫端
//! 使用它——這個模組把它轉成一個數字，Step B 會把這個數字寫進
//! `core_model::AiRun::estimated_cost`。

use crate::TokenUsage;

/// 每千 token 的估計價格。單位不強制是美元——本地推論通常沒有真實計費，
/// 這裡的用途是讓「這次呼叫花了多少資源」有一個可比較的相對數字，不是
/// 真實帳單金額。兩個欄位分開（prompt／completion）是因為多數 LLM 供應商
/// 的定價本來就不同價，`TokenUsage` 也已經是分開的兩個欄位。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pricing {
    pub prompt_price_per_1k: f64,
    pub completion_price_per_1k: f64,
}

impl Pricing {
    /// 兩者皆零：本地推論沒有真實計費時的預設值，`estimate_cost` 一律回 `0.0`。
    #[must_use]
    pub const fn free() -> Self {
        Self {
            prompt_price_per_1k: 0.0,
            completion_price_per_1k: 0.0,
        }
    }

    #[must_use]
    pub fn estimate_cost(&self, usage: &TokenUsage) -> f64 {
        let prompt_cost = f64::from(usage.prompt_tokens) / 1000.0 * self.prompt_price_per_1k;
        let completion_cost =
            f64::from(usage.completion_tokens) / 1000.0 * self.completion_price_per_1k;
        prompt_cost + completion_cost
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn free_pricing_is_always_zero() {
        let pricing = Pricing::free();
        let usage = TokenUsage {
            prompt_tokens: 1000,
            completion_tokens: 500,
        };
        assert_eq!(pricing.estimate_cost(&usage), 0.0);
    }

    #[test]
    fn estimate_cost_sums_prompt_and_completion_separately() {
        let pricing = Pricing {
            prompt_price_per_1k: 0.01,
            completion_price_per_1k: 0.03,
        };
        let usage = TokenUsage {
            prompt_tokens: 2000,
            completion_tokens: 1000,
        };
        // 2000/1000*0.01=0.02 + 1000/1000*0.03=0.03 = 0.05
        assert!((pricing.estimate_cost(&usage) - 0.05).abs() < 1e-9);
    }

    #[test]
    fn zero_tokens_is_zero_cost() {
        let pricing = Pricing {
            prompt_price_per_1k: 0.01,
            completion_price_per_1k: 0.03,
        };
        let usage = TokenUsage {
            prompt_tokens: 0,
            completion_tokens: 0,
        };
        assert_eq!(pricing.estimate_cost(&usage), 0.0);
    }
}
