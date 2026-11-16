//! 簡體／繁體正規化（應用端 OpenCC `t2s`，不是 OpenSearch plugin）。
//!
//! 關鍵字搜尋把 `title`／`summary`／`body` 轉成簡體再寫進 `*_s`，查詢字串也走同一條
//! 轉換。兩邊的表不一致時，繁體查詢會**靜默**漏掉簡體文件——所以轉換器載入失敗必須
//! 讓行程起不來，不能退回「照原文索引」。
//!
//! 用 `t2s` 不用 `tw2sp`：後者會連詞彙一起改（例如台灣用語對到大陸用語），
//! 搜尋要的是字形對齊，不是改寫句子。

use std::sync::LazyLock;

use ferrous_opencc::{OpenCC, config::BuiltinConfig};

/// 寫進 `text_norm_version` 的識別字串。
///
/// 轉換表或 crate 版本換了就要改這裡，否則重建前後的文件看起來一樣、
/// 缺轉換的舊文件也無從用這個欄位算出來。
pub const TEXT_NORM_VERSION: &str = "opencc-t2s/ferrous-opencc-0.4.0";

static CONVERTER: LazyLock<Result<OpenCC, String>> = LazyLock::new(|| {
    OpenCC::from_config(BuiltinConfig::T2s).map_err(|err| {
        format!(
            "簡繁轉換表載入失敗（{TEXT_NORM_VERSION}）：{err}。\
             關鍵字搜尋不能在沒有轉換的情況下啟動，否則繁體查詢會靜默漏掉簡體文件。\
             請確認 indexer 有帶 ferrous-opencc 的 t2s-conversion feature 後重新建置"
        )
    })
});

/// 強制建好轉換器。失敗回 `Err`，呼叫端應中止啟動。
///
/// 不在這裡把錯誤吞掉：`to_simplified` 若在轉換器壞掉時改回傳原文，
/// 索引與查詢會各自「看起來成功」但對不到。
pub fn init() -> Result<(), String> {
    match CONVERTER.as_ref() {
        Ok(_) => Ok(()),
        Err(err) => Err(err.clone()),
    }
}

/// 轉成簡體。已是簡體、ASCII、空字串都原樣回來（轉換表沒有對應時 OpenCC 保留原字）。
///
/// 轉換器沒建成功就 panic，不回傳原文。正常啟動路徑會先呼叫 [`init`]，
/// 走到這裡還是 `Err` 代表有人繞過啟動檢查。
#[must_use]
pub fn to_simplified(text: &str) -> String {
    match CONVERTER.as_ref() {
        Ok(converter) => converter.convert(text),
        Err(err) => {
            panic!("簡繁轉換尚未就緒就被呼叫：{err}。啟動流程必須先呼叫 script::init() 並處理錯誤")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn init_loads_the_converter() {
        assert!(
            init().is_ok(),
            "轉換表載入失敗時行程不該繼續，否則會靜默用未轉換的文字索引"
        );
    }

    #[test]
    fn traditional_names_become_simplified() {
        assert_eq!(to_simplified("秦剛"), "秦刚");
        assert_eq!(to_simplified("外長"), "外长");
        assert_eq!(to_simplified("華春瑩"), "华春莹");
    }

    #[test]
    fn ascii_and_cve_identifiers_are_unchanged() {
        assert_eq!(to_simplified("ransomware"), "ransomware");
        assert_eq!(to_simplified("CVE-2026-0001"), "CVE-2026-0001");
    }

    #[test]
    fn already_simplified_text_is_unchanged() {
        assert_eq!(to_simplified("秦刚外长"), "秦刚外长");
    }

    #[test]
    fn empty_string_stays_empty() {
        assert_eq!(to_simplified(""), "");
    }

    #[test]
    fn conversion_is_idempotent() {
        let once = to_simplified("秦剛是外長，華春瑩說明細節");
        assert_eq!(to_simplified(&once), once);
    }

    #[test]
    fn japanese_sample_is_pinned() {
        // 假名不在簡繁表裡。「東」在 t2s 會變成「东」（與中文「東」同一條），
        // 「渋」不轉。整句釘住，換字典版本時這個測試會先紅，而不是讓日文文件
        // 的 `*_s` 悄悄變成另一套字形。
        assert_eq!(to_simplified("東京都の渋谷"), "东京都の渋谷");
    }
}
