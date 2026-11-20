//! 段落切分核心（ADR-016 D6／D7，階段 5 M1-2）。
//!
//! 輸入一份 [`Document`]，輸出 [`storage_core::DocumentChunk`]。純計算：唯一的 I/O 是
//! 啟動時讀 `tokenizer.json`（[`E5Tokenizer::load`]）。寫庫、發事件是 chunk-worker 的事。
//!
//! # 策略（[`select_strategy`]）
//!
//! | 文件 | 策略 | 說明 |
//! |---|---|---|
//! | 匯入（`attributes.import_kind` 有值：JSON／CSV） | `record` | 一筆一段 |
//! | RSS／Atom 條目（`object_type = article` 且非匯入） | `whole` | 不切，整篇一段 |
//! | 其他（網頁 `web_page`、公告、報告…） | `fixed` | 300 token／重疊 100 |
//!
//! 語意切段（D6 的網頁／長篇報告預設）是 M3，這裡先走 `fixed`。
//!
//! # 512 token 保護（D6）
//!
//! 「模型實際吃到的 token 數」＝`passage: ` 前綴＋[`embedding_text`]（header＋段落文字）＋特殊 token。
//! - `whole`／`record` 超過上限時，**在這份文件內部**改用 300／100 的打包切開
//!   （段落仍記原策略名稱：這個退路是策略定義的一部分，`whole@1` 永遠這樣切）。
//! - 單句超過 300 token（實驗 5-2g：IoC 表格被當成一句，840 token）→ 在 token 邊界硬切成
//!   ≤300 token 的片段再打包。
//! - 打包完仍超過上限（header 太長或斷詞邊界效應）→ 該段再硬切；還超過回 [`ChunkerError::Invariant`]。
//!
//! # 偏移不變式
//!
//! `start_char`／`end_char` 是 [`storage_core::chunk_source_text`]`(doc, None)` 的**字元**偏移，
//! `text` 就是那段字元原樣（不 trim）。`tests/` 對每個策略逐段驗證。
//!
//! # 版本
//!
//! 下列任何一項改了，**所有策略的版本都要升**（段落 id 由版本推導，舊段落會被整批取代）：
//! tokenizer 檔（[`E5_TOKENIZER_SHA256`]）、斷句規則（[`sentence`]）、header 規則、
//! 打包參數預設值、策略選擇規則。

pub mod error;
pub mod sentence;
pub mod tokenizer;

use std::sync::Arc;

use chrono::{DateTime, Utc};
use core_model::{Document, DocumentType};
use storage_core::{DocumentChunk, chunk_id, chunk_source_text, truncate_on_char_boundary};

pub use error::ChunkerError;
pub use sentence::{Span, split_sentences};
pub use tokenizer::{E5_TOKENIZER_SHA256, E5Tokenizer, PASSAGE_PREFIX};

/// 切段策略。名稱與版本寫進每個段落（`strategy`／`strategy_version`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Strategy {
    Whole,
    Record,
    Fixed,
}

impl Strategy {
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Whole => "whole",
            Self::Record => "record",
            Self::Fixed => "fixed",
        }
    }

    #[must_use]
    pub fn version(self) -> u32 {
        match self {
            Self::Whole | Self::Record | Self::Fixed => 1,
        }
    }
}

/// D6 的策略對映。見 crate 文件的表。
#[must_use]
pub fn select_strategy(document: &Document) -> Strategy {
    let imported = document
        .attributes
        .get("import_kind")
        .is_some_and(|v| !v.is_null());
    if imported {
        return Strategy::Record;
    }
    match document.object_type {
        DocumentType::Article => Strategy::Whole,
        _ => Strategy::Fixed,
    }
}

#[derive(Debug, Clone)]
pub struct ChunkerConfig {
    /// 打包目標（D6 參考值 300）。
    pub target_tokens: u32,
    /// 相鄰段落重疊（D6 參考值 100）。
    pub overlap_tokens: u32,
    /// 模型上限（e5 `max_position_embeddings` 512），含前綴與特殊 token。
    pub max_model_tokens: u32,
    /// header（文件標題）最多佔多少 token；超過在 token 邊界截。
    pub max_header_tokens: u32,
    /// 串接文字的 byte 上限（外部輸入不可信，CLAUDE.md §5）。超過就截在字元邊界，
    /// 只切前面這段，[`ChunkOutput::truncated`] 為真。開發 DB 最長 body 64,395 字元。
    pub max_source_bytes: usize,
}

impl Default for ChunkerConfig {
    fn default() -> Self {
        Self {
            target_tokens: 300,
            overlap_tokens: 100,
            max_model_tokens: 512,
            max_header_tokens: 64,
            max_source_bytes: 2 * 1024 * 1024,
        }
    }
}

impl ChunkerConfig {
    pub fn validate(&self) -> Result<(), ChunkerError> {
        if self.target_tokens == 0 {
            return Err(ChunkerError::Config("target_tokens 必須 > 0".into()));
        }
        if self.overlap_tokens >= self.target_tokens {
            return Err(ChunkerError::Config(format!(
                "overlap_tokens（{}）必須小於 target_tokens（{}），否則段落不會前進",
                self.overlap_tokens, self.target_tokens
            )));
        }
        // 前綴約 3 token、特殊 token 2 個；留 16 的餘裕給斷詞邊界效應。
        let worst = self.target_tokens + self.max_header_tokens + 16;
        if worst > self.max_model_tokens {
            return Err(ChunkerError::Config(format!(
                "target_tokens + max_header_tokens + 16 = {worst} 超過 max_model_tokens {}，\
                 幾乎每段都會被二次硬切",
                self.max_model_tokens
            )));
        }
        if self.max_source_bytes == 0 {
            return Err(ChunkerError::Config("max_source_bytes 必須 > 0".into()));
        }
        Ok(())
    }
}

/// 一份文件的切段結果。
#[derive(Debug, Clone)]
pub struct ChunkOutput {
    pub strategy: Strategy,
    pub chunks: Vec<DocumentChunk>,
    /// 串接文字超過 `max_source_bytes` 被截。
    pub truncated: bool,
    /// `whole`／`record` 超過上限，改用 300／100 打包。
    pub fell_back_to_fixed: bool,
    /// 在 token 邊界硬切的次數（單句過長或打包後仍超限）。
    pub hard_cuts: u32,
}

/// 向量化時真正送進模型的文字（不含 `passage: ` 前綴）。M2 必須用這支組字，
/// 否則 `token_count` 與 512 保護對不上實際輸入。
#[must_use]
pub fn embedding_text(header: Option<&str>, text: &str) -> String {
    match header {
        Some(h) => format!("{h}\n{text}"),
        None => text.to_string(),
    }
}

pub struct Chunker {
    tokenizer: Arc<E5Tokenizer>,
    config: ChunkerConfig,
}

impl Chunker {
    pub fn new(tokenizer: Arc<E5Tokenizer>, config: ChunkerConfig) -> Result<Self, ChunkerError> {
        config.validate()?;
        Ok(Self { tokenizer, config })
    }

    #[must_use]
    pub fn config(&self) -> &ChunkerConfig {
        &self.config
    }

    /// 用 [`select_strategy`] 選策略再切。
    pub fn chunk_document(
        &self,
        document: &Document,
        created_at: DateTime<Utc>,
    ) -> Result<ChunkOutput, ChunkerError> {
        self.chunk_with_strategy(document, select_strategy(document), created_at)
    }

    pub fn chunk_with_strategy(
        &self,
        document: &Document,
        strategy: Strategy,
        created_at: DateTime<Utc>,
    ) -> Result<ChunkOutput, ChunkerError> {
        let full = chunk_source_text(document, None);
        let source = truncate_on_char_boundary(&full, self.config.max_source_bytes);
        let truncated = source.len() < full.len();
        let chars: Vec<char> = source.chars().collect();
        let mut stats = Stats::default();

        let title = document.title.as_deref().filter(|t| !t.is_empty());
        let header_text = match title {
            Some(t) => Some(self.cap_header(t)?).filter(|h| !h.is_empty()),
            None => None,
        };
        // 標題在串接文字最前面（chunk_source_text 的格式）。完整包含標題的段落不再加 header。
        let title_end = title.map_or(0, |t| t.chars().count());

        let whole_span = Span {
            start: 0,
            end: chars.len(),
        };
        let mut spans = match strategy {
            Strategy::Whole | Strategy::Record => {
                let header = header_for(whole_span, title_end, header_text.as_deref());
                let text: String = chars.iter().collect();
                if self.fits(header, &text)? {
                    vec![whole_span]
                } else {
                    stats.fell_back = true;
                    self.pack(&chars, &mut stats)?
                }
            }
            Strategy::Fixed => self.pack(&chars, &mut stats)?,
        };
        spans.retain(|s| chars[s.start..s.end].iter().any(|c| !c.is_whitespace()));

        // 512 保護的最後一道：打包結果逐段實算，超過就在段內硬切。
        let mut guarded: Vec<Span> = Vec::with_capacity(spans.len());
        for span in spans {
            let header = header_for(span, title_end, header_text.as_deref());
            let text: String = chars[span.start..span.end].iter().collect();
            if self.fits(header, &text)? {
                guarded.push(span);
                continue;
            }
            let budget = self.text_budget(header)?;
            for piece in self.hard_cut(&chars, span, budget, &mut stats)? {
                guarded.push(piece);
            }
        }

        let mut chunks = Vec::with_capacity(guarded.len());
        for (seq, span) in guarded.into_iter().enumerate() {
            let seq =
                u32::try_from(seq).map_err(|_| ChunkerError::Invariant("段落數超過 u32".into()))?;
            let header = header_for(span, title_end, header_text.as_deref());
            let text: String = chars[span.start..span.end].iter().collect();
            let input = embedding_text(header, &text);
            let model_tokens = self.tokenizer.count_model_input(&input)?;
            if model_tokens > self.config.max_model_tokens {
                return Err(ChunkerError::Invariant(format!(
                    "文件 {} 第 {seq} 段硬切後仍有 {model_tokens} token（上限 {}）",
                    document.id, self.config.max_model_tokens
                )));
            }
            chunks.push(DocumentChunk {
                id: chunk_id(document.id, strategy.name(), strategy.version(), seq),
                document_id: document.id,
                seq,
                start_char: to_u32(span.start)?,
                end_char: to_u32(span.end)?,
                text,
                header: header.map(str::to_string),
                token_count: self.tokenizer.count(&input)?,
                strategy: strategy.name().to_string(),
                strategy_version: strategy.version(),
                simhash: None,
                group_id: None,
                created_at,
            });
        }
        Ok(ChunkOutput {
            strategy,
            chunks,
            truncated,
            fell_back_to_fixed: stats.fell_back,
            hard_cuts: stats.hard_cuts,
        })
    }

    fn fits(&self, header: Option<&str>, text: &str) -> Result<bool, ChunkerError> {
        let n = self
            .tokenizer
            .count_model_input(&embedding_text(header, text))?;
        Ok(n <= self.config.max_model_tokens)
    }

    /// 段落文字本身可用的 token 數：上限扣掉前綴、特殊 token、header 與分隔換行。
    fn text_budget(&self, header: Option<&str>) -> Result<u32, ChunkerError> {
        let overhead = self
            .tokenizer
            .count_model_input(&embedding_text(header, ""))?;
        // 留 4 token 給段落首字與 header 換行之間的斷詞邊界效應。
        let budget = self
            .config
            .max_model_tokens
            .saturating_sub(overhead)
            .saturating_sub(4);
        if budget == 0 {
            return Err(ChunkerError::Invariant(format!(
                "header 已佔滿 {} token，段落沒有空間",
                self.config.max_model_tokens
            )));
        }
        Ok(budget)
    }

    fn cap_header(&self, title: &str) -> Result<String, ChunkerError> {
        let max = self.config.max_header_tokens;
        if self.tokenizer.count(title)? <= max {
            return Ok(title.to_string());
        }
        // 不能直接取第 max 個 token 起點：Metaspace 的前綴 `▁` 與第一個字共用 offset，
        // 起點清單比 token 數少一個，截出來的前綴重算會多 1（實測「很長的標題」×100）。
        // 從估計點往回退到實算 ≤ max 為止。
        let starts = self.tokenizer.token_char_starts(title)?;
        let mut idx = (max as usize).min(starts.len().saturating_sub(1));
        while idx > 0 {
            let candidate: String = title.chars().take(starts[idx]).collect();
            if self.tokenizer.count(&candidate)? <= max {
                return Ok(candidate);
            }
            idx -= 1;
        }
        Ok(String::new())
    }

    /// 斷句 → 過長單句硬切成 ≤ target 的片段 → 依 target／overlap 貪婪打包。
    fn pack(&self, chars: &[char], stats: &mut Stats) -> Result<Vec<Span>, ChunkerError> {
        let target = self.config.target_tokens;
        let overlap = self.config.overlap_tokens;

        let mut units: Vec<(Span, u32)> = Vec::new();
        for sentence in split_sentences(chars) {
            let text: String = chars[sentence.start..sentence.end].iter().collect();
            let tokens = self.tokenizer.count(&text)?;
            if tokens <= target {
                units.push((sentence, tokens));
                continue;
            }
            for piece in self.hard_cut(chars, sentence, target, stats)? {
                let text: String = chars[piece.start..piece.end].iter().collect();
                units.push((piece, self.tokenizer.count(&text)?));
            }
        }
        if units.is_empty() {
            return Ok(Vec::new());
        }

        let mut spans = Vec::new();
        let mut first = 0;
        loop {
            let mut last = first;
            let mut sum = units[first].1;
            while last + 1 < units.len() && sum + units[last + 1].1 <= target {
                last += 1;
                sum += units[last].1;
            }
            spans.push(Span {
                start: units[first].0.start,
                end: units[last].0.end,
            });
            if last + 1 >= units.len() {
                break;
            }
            // 下一段往回帶最多 overlap token 的完整單位，但一定要前進。
            let mut next = last + 1;
            let mut carried = 0;
            while next > first + 1 && carried + units[next - 1].1 <= overlap {
                next -= 1;
                carried += units[next].1;
            }
            first = next;
        }
        Ok(spans)
    }

    /// 在 token 起點上把 `span` 切成每片 ≤ `budget` token。
    fn hard_cut(
        &self,
        chars: &[char],
        span: Span,
        budget: u32,
        stats: &mut Stats,
    ) -> Result<Vec<Span>, ChunkerError> {
        let text: String = chars[span.start..span.end].iter().collect();
        let starts = self.tokenizer.token_char_starts(&text)?;
        // 每片重新斷詞時開頭會多一個 Metaspace `▁`（見 cap_header），所以步長少 1。
        let step = (budget as usize).saturating_sub(1).max(1);
        let mut cuts: Vec<usize> = starts.iter().copied().step_by(step).skip(1).collect();
        cuts.retain(|&c| c > 0 && c < span.len());
        stats.hard_cuts += 1;
        let mut pieces = Vec::with_capacity(cuts.len() + 1);
        let mut start = span.start;
        for cut in cuts {
            let end = span.start + cut;
            if end > start {
                pieces.push(Span { start, end });
                start = end;
            }
        }
        pieces.push(Span {
            start,
            end: span.end,
        });
        Ok(pieces)
    }
}

#[derive(Default)]
struct Stats {
    fell_back: bool,
    hard_cuts: u32,
}

fn header_for(span: Span, title_end: usize, header: Option<&str>) -> Option<&str> {
    if span.start == 0 && span.end >= title_end {
        None
    } else {
        header
    }
}

fn to_u32(value: usize) -> Result<u32, ChunkerError> {
    u32::try_from(value).map_err(|_| ChunkerError::Invariant(format!("字元偏移 {value} 超過 u32")))
}
