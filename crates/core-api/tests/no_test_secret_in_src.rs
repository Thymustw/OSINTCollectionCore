//! 防止硬編碼測試密鑰再次被放回 `src/`。
//!
//! # 為什麼需要這支測試
//!
//! Phase 6a 之前 `src/lib.rs` 有 `JwtService::new(&[b't'; 32], …)`，
//! 而且**沒有** `#[cfg(test)]` gate——那把密鑰會被編進 `osint-api` 的 release binary。
//! 修掉一次很容易；問題是下一個人為了寫測試方便，很可能再放一個回去，
//! 而那不會讓任何測試變紅。
//!
//! 這支測試掃 `crates/core-api/src/`，找「重複同一個位元組的密鑰字面量」。
//! 它擋不住所有形式的硬編碼密鑰（那要 gitleaks／`make secret-scan` 去做），
//! 但它精準擋住實際發生過的那一種。
//!
//! 真正的最終確認是對 release binary 做字串檢查，那一步在 CI／交付時做，
//! 不放進單元測試——`cargo build --release` 在共用工作站上太貴（CLAUDE.md §15）。

use std::path::Path;

/// 找 `[b'x'; N]` 這種形狀的位元組陣列字面量。
fn find_repeated_byte_literals(source: &str) -> Vec<String> {
    let mut hits = Vec::new();
    let bytes: Vec<char> = source.chars().collect();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == '[' {
            // 往後看 40 個字元夠涵蓋 `[b'x'; 32]` 這種寫法。
            let end = (i + 40).min(bytes.len());
            let window: String = bytes[i..end].iter().collect();
            if window.starts_with("[b'")
                && window.contains(';')
                && let Some(close) = window.find(']')
            {
                hits.push(window[..=close].to_string());
            }
        }
        i += 1;
    }
    hits
}

#[test]
fn src_contains_no_hardcoded_byte_array_secret() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    assert!(src.is_dir(), "找不到 {}", src.display());

    let mut offenders: Vec<String> = Vec::new();
    for entry in std::fs::read_dir(&src).expect("讀 src/") {
        let path = entry.expect("dir entry").path();
        if path.extension().and_then(|e| e.to_str()) != Some("rs") {
            continue;
        }
        let text = std::fs::read_to_string(&path).expect("讀檔");
        for hit in find_repeated_byte_literals(&text) {
            offenders.push(format!("{}: {hit}", path.display()));
        }
    }

    assert!(
        offenders.is_empty(),
        "crates/core-api/src/ 裡出現了位元組陣列字面量，看起來像硬編碼密鑰：\n{}\n\
         測試用的密鑰請放 tests/common/mod.rs——那裡不會被編進 release binary。\n\
         如果這是誤判（真的不是密鑰），請在這支測試加白名單並寫明理由。",
        offenders.join("\n")
    );
}

/// 這個 helper 本身要驗一次，否則「掃不到東西」與「掃描壞掉」長得一模一樣。
#[test]
fn detector_actually_detects() {
    let sample = r#"let jwt = JwtService::new(&[b't'; 32], "osint-core", ttl);"#;
    assert_eq!(find_repeated_byte_literals(sample), vec!["[b't'; 32]"]);
    assert!(find_repeated_byte_literals("let xs = [1, 2, 3];").is_empty());
}
