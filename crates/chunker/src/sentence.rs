//! 中英斷句（ADR-016 D7 第 2 點）。輸出是**連續、不重疊、覆蓋全文**的字元區間——
//! 段落 offset 直接由這些區間組成，所以空白與換行一律歸給前一句，不丟任何字元。
//!
//! 句界：
//! - 全形 `。！？；`、半形 `!?;`：遇到就斷（連續標點如 `？！`、`……` 視為一個句尾）。
//! - 半形 `.`：後面接空白或文末才斷（`3.14`、`example.com`、`v1.2.3` 不斷）。
//! - 換行：一律斷（標題、清單、段落）。
//! - 句尾後緊接的收尾引號／括號（`」』”’）)】》`）併入前一句。
//! - 在 `「『（(【《“` 等開括號內不斷句；括號深度遇到換行歸零，避免一個沒關的括號吃掉整篇。

/// 一句：`[start, end)` 字元偏移。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

impl Span {
    #[must_use]
    pub fn len(self) -> usize {
        self.end - self.start
    }

    #[must_use]
    pub fn is_empty(self) -> bool {
        self.end == self.start
    }
}

fn is_hard_terminator(c: char) -> bool {
    matches!(c, '。' | '！' | '？' | '；' | '!' | '?' | ';' | '…')
}

fn is_closer(c: char) -> bool {
    matches!(
        c,
        '」' | '』' | '”' | '’' | '）' | ')' | '】' | '》' | '〉' | ']' | '"' | '\''
    )
}

fn is_opener(c: char) -> bool {
    matches!(c, '「' | '『' | '“' | '（' | '(' | '【' | '《' | '〈' | '[')
}

/// `chars` 是整份串接文字的字元。回傳的區間依序覆蓋 `0..chars.len()`。
#[must_use]
pub fn split_sentences(chars: &[char]) -> Vec<Span> {
    let n = chars.len();
    let mut spans = Vec::new();
    let mut start = 0;
    let mut depth: u32 = 0;
    let mut i = 0;
    while i < n {
        let c = chars[i];
        if c == '\n' {
            depth = 0;
            let end = i + 1;
            spans.push(Span { start, end });
            start = end;
            i = end;
            continue;
        }
        if is_opener(c) {
            depth = depth.saturating_add(1);
            i += 1;
            continue;
        }
        let mut closes_quoted = false;
        if is_closer(c) && depth > 0 && !matches!(c, '"' | '\'') {
            depth -= 1;
            // 「已修補。」：句尾標點在引號內、緊接收尾引號 → 句子在引號之後結束。
            closes_quoted =
                depth == 0 && i > 0 && (is_hard_terminator(chars[i - 1]) || chars[i - 1] == '.');
            if !closes_quoted {
                i += 1;
                continue;
            }
        }
        let ends_sentence = closes_quoted
            || depth == 0
                && (is_hard_terminator(c)
                    || (c == '.' && (i + 1 == n || chars[i + 1].is_whitespace())));
        if !ends_sentence {
            i += 1;
            continue;
        }
        // 吃掉連續句尾標點、收尾引號括號、以及同一行的後續空白（不含換行：換行自己是句界）。
        let mut end = i + 1;
        while end < n && (is_hard_terminator(chars[end]) || chars[end] == '.') {
            end += 1;
        }
        while end < n && is_closer(chars[end]) {
            end += 1;
        }
        while end < n && chars[end].is_whitespace() && chars[end] != '\n' {
            end += 1;
        }
        spans.push(Span { start, end });
        start = end;
        i = end;
    }
    if start < n {
        spans.push(Span { start, end: n });
    }
    spans
}

#[cfg(test)]
mod tests {
    use super::*;

    fn split(text: &str) -> Vec<String> {
        let chars: Vec<char> = text.chars().collect();
        split_sentences(&chars)
            .into_iter()
            .map(|s| chars[s.start..s.end].iter().collect())
            .collect()
    }

    fn assert_covers(text: &str) {
        let chars: Vec<char> = text.chars().collect();
        let spans = split_sentences(&chars);
        let mut pos = 0;
        for s in &spans {
            assert_eq!(s.start, pos, "區間不連續：{spans:?}");
            assert!(s.end > s.start, "空區間：{spans:?}");
            pos = s.end;
        }
        assert_eq!(pos, chars.len(), "沒有覆蓋到文末");
    }

    #[test]
    fn chinese_terminators_split() {
        assert_eq!(
            split("第一句。第二句！第三句？第四；尾"),
            vec!["第一句。", "第二句！", "第三句？", "第四；", "尾"]
        );
    }

    #[test]
    fn closing_quote_stays_with_sentence() {
        assert_eq!(
            split("他說：「已修補。」接著離開。"),
            vec!["他說：「已修補。」", "接著離開。"]
        );
    }

    #[test]
    fn period_inside_brackets_does_not_split() {
        assert_eq!(
            split("影響版本（7.2.0。含以下）均需更新。下一句"),
            vec!["影響版本（7.2.0。含以下）均需更新。", "下一句"]
        );
    }

    #[test]
    fn english_period_needs_whitespace() {
        assert_eq!(
            split("See example.com v1.2.3 now. Pi is 3.14. End"),
            vec!["See example.com v1.2.3 now. ", "Pi is 3.14. ", "End"]
        );
    }

    #[test]
    fn newline_always_splits_and_resets_depth() {
        assert_eq!(split("標題（未關\n正文。"), vec!["標題（未關\n", "正文。"]);
    }

    #[test]
    fn repeated_punctuation_is_one_terminator() {
        assert_eq!(split("真的？！好……然後"), vec!["真的？！", "好……", "然後"]);
    }

    #[test]
    fn spans_cover_everything() {
        for text in [
            "",
            "\n\n",
            "  前導空白。  中間  \n\n尾巴",
            "沒有標點的一長串文字",
            "Mixed 中英. 混合！ end?",
            "🔥 emoji。é 組合字元。",
        ] {
            assert_covers(text);
        }
    }
}
