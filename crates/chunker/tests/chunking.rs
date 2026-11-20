//! 用真的 e5 `tokenizer.json` 跑切段。檔案不進 repo（17 MB，上游公開檔）：
//! 預設找 `<workspace>/models/e5-small/tokenizer.json`，可用 `E5_TOKENIZER_PATH` 覆寫。
//! 取得方式見 `docs/developer/chunker.md`。找不到檔案時測試**失敗**而不是跳過——
//! 跳過的測試在 CI 上看起來是綠的，等於沒驗。

use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use chrono::Utc;
use chunker::{
    Chunker, ChunkerConfig, ChunkerError, E5_TOKENIZER_SHA256, E5Tokenizer, Strategy,
    embedding_text, select_strategy,
};
use core_model::{Document, DocumentType};
use serde_json::json;
use storage_core::{DocumentChunk, chunk_source_text, validate_chunks};
use uuid::Uuid;

fn tokenizer_path() -> PathBuf {
    if let Ok(p) = std::env::var("E5_TOKENIZER_PATH") {
        return PathBuf::from(p);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../models/e5-small/tokenizer.json")
}

fn tokenizer() -> Arc<E5Tokenizer> {
    static TOK: OnceLock<Arc<E5Tokenizer>> = OnceLock::new();
    TOK.get_or_init(|| {
        let path = tokenizer_path();
        Arc::new(
            E5Tokenizer::load(&path, E5_TOKENIZER_SHA256).unwrap_or_else(|err| {
                panic!(
                    "載入 {} 失敗：{err}。請照 docs/developer/chunker.md 下載 tokenizer.json",
                    path.display()
                )
            }),
        )
    })
    .clone()
}

fn chunker() -> Chunker {
    Chunker::new(tokenizer(), ChunkerConfig::default()).expect("預設設定合法")
}

fn doc(object_type: DocumentType, title: Option<&str>, body: &str, imported: bool) -> Document {
    let now = Utc::now();
    Document {
        id: Uuid::now_v7(),
        object_type,
        schema_version: "1".into(),
        title: title.map(Into::into),
        body: Some(body.into()),
        summary: None,
        language: None,
        author: None,
        published_at: None,
        modified_at: None,
        observed_at: now,
        collected_at: now,
        source_url: None,
        canonical_url: None,
        normalized_content_hash: None,
        confidence: 1.0,
        labels: vec![],
        attributes: if imported {
            json!({"import_kind": "json", "record_index": 0})
        } else {
            json!({})
        },
        external_key: None,
        simhash: None,
        duplicate_of: None,
    }
}

/// 所有段落共同的不變式：偏移逐字對得上串接文字、seq 連續、在 512 內、
/// 每個 token_count 與實算一致、段落合起來覆蓋所有非空白字元。
fn assert_invariants(document: &Document, chunks: &[DocumentChunk]) {
    let tok = tokenizer();
    validate_chunks(document.id, chunks).expect("seq／document_id／offset 基本檢查");
    let source: Vec<char> = chunk_source_text(document, None).chars().collect();
    let mut covered = vec![false; source.len()];
    for c in chunks {
        let (s, e) = (c.start_char as usize, c.end_char as usize);
        let slice: String = source[s..e].iter().collect();
        assert_eq!(slice, c.text, "第 {} 段偏移與文字對不上", c.seq);
        let input = embedding_text(c.header.as_deref(), &c.text);
        assert_eq!(
            tok.count(&input).unwrap(),
            c.token_count,
            "第 {} 段 token_count",
            c.seq
        );
        let model = tok.count_model_input(&input).unwrap();
        assert!(model <= 512, "第 {} 段模型輸入 {model} token > 512", c.seq);
        covered[s..e].iter_mut().for_each(|x| *x = true);
    }
    for (i, ch) in source.iter().enumerate() {
        assert!(
            covered[i] || ch.is_whitespace(),
            "字元 {i}（{ch:?}）不在任何段落裡——內容被丟了"
        );
    }
}

#[test]
fn hash_mismatch_is_rejected() {
    let bytes = std::fs::read(tokenizer_path()).expect("讀 tokenizer");
    let err = E5Tokenizer::from_bytes(&bytes, &"0".repeat(64)).unwrap_err();
    assert!(
        matches!(err, ChunkerError::TokenizerHashMismatch { .. }),
        "{err}"
    );
}

/// 實驗 5-2c 的黃金句之一，id 序列由 Python tokenizers 0.23.3 產生；這裡只釘長度，
/// 防止有人換 tokenizer 檔或改計數方式（是否含特殊 token）卻沒發現。
#[test]
fn token_counts_are_pinned() {
    let tok = tokenizer();
    assert_eq!(tok.count("").unwrap(), 0);
    let zh = "勒索軟體 LockBit 3.0 於 2026 年第三季活動下降 30%。";
    let n = tok.count(zh).unwrap();
    assert_eq!(
        tok.count_model_input(zh).unwrap(),
        n + 2 + tok.count("passage:").unwrap()
    );
    assert!(n > 10 && n < 40, "中文句 token 數 {n} 不合理");
}

#[test]
fn strategy_selection_follows_adr_table() {
    assert_eq!(
        select_strategy(&doc(DocumentType::Article, None, "x", true)),
        Strategy::Record
    );
    assert_eq!(
        select_strategy(&doc(DocumentType::Advisory, None, "x", true)),
        Strategy::Record
    );
    assert_eq!(
        select_strategy(&doc(DocumentType::Article, None, "x", false)),
        Strategy::Whole
    );
    assert_eq!(
        select_strategy(&doc(DocumentType::WebPage, None, "x", false)),
        Strategy::Fixed
    );
    assert_eq!(
        select_strategy(&doc(DocumentType::Report, None, "x", false)),
        Strategy::Fixed
    );
}

#[test]
fn short_record_is_one_chunk_without_header() {
    let d = doc(
        DocumentType::Advisory,
        Some("CVE-2026-0001"),
        "某產品存在遠端程式碼執行漏洞，請更新至 1.2.3 版。",
        true,
    );
    let out = chunker().chunk_document(&d, Utc::now()).unwrap();
    assert_eq!(out.strategy, Strategy::Record);
    assert_eq!(out.chunks.len(), 1);
    assert!(!out.fell_back_to_fixed);
    // 段落本身就從標題開始，不需要再加 header。
    assert_eq!(out.chunks[0].header, None);
    assert_eq!(out.chunks[0].start_char, 0);
    assert_invariants(&d, &out.chunks);
}

fn long_mixed_body(sentences: usize) -> String {
    let zh = "研究人員指出，攻擊者利用 CVE-2026-1234 入侵邊界設備，並在內網橫向移動。";
    let en = "The actor deployed a loader that fetched the second stage from 203.0.113.45. ";
    (0..sentences)
        .map(|i| {
            if i % 2 == 0 {
                zh.to_string()
            } else {
                en.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("")
}

#[test]
fn fixed_strategy_overlaps_and_stays_under_limit() {
    let d = doc(
        DocumentType::WebPage,
        Some("長篇報告"),
        &long_mixed_body(120),
        false,
    );
    let out = chunker().chunk_document(&d, Utc::now()).unwrap();
    assert_eq!(out.strategy, Strategy::Fixed);
    assert!(out.chunks.len() >= 5, "段數 {}", out.chunks.len());
    assert_eq!(out.hard_cuts, 0, "一般句子不該觸發硬切");
    for pair in out.chunks.windows(2) {
        assert!(pair[1].start_char < pair[0].end_char, "相鄰段應重疊");
        assert!(pair[1].start_char > pair[0].start_char, "段落必須前進");
    }
    // 第一段含標題不加 header，之後的段落都帶標題 header。
    assert_eq!(out.chunks[0].header, None);
    assert!(
        out.chunks[1..]
            .iter()
            .all(|c| c.header.as_deref() == Some("長篇報告"))
    );
    // 段落大小落在目標附近（扣掉 header 後 ≤ 300＋單句餘量）。
    let tok = tokenizer();
    for c in &out.chunks {
        assert!(
            tok.count(&c.text).unwrap() <= 300,
            "第 {} 段超過 target",
            c.seq
        );
    }
    assert_invariants(&d, &out.chunks);
}

#[test]
fn long_whole_document_falls_back_inside_itself() {
    let d = doc(
        DocumentType::Article,
        Some("RSS 長文"),
        &long_mixed_body(80),
        false,
    );
    let out = chunker().chunk_document(&d, Utc::now()).unwrap();
    assert_eq!(out.strategy, Strategy::Whole);
    assert!(out.fell_back_to_fixed);
    assert!(out.chunks.len() > 1);
    assert!(out.chunks.iter().all(|c| c.strategy == "whole"));
    assert_invariants(&d, &out.chunks);
}

/// 實驗 5-2g：IoC 表格被斷句規則當成一句，840 token。必須硬切，不能交給模型截斷。
#[test]
fn single_oversized_sentence_is_hard_cut() {
    let hashes: String = (0..60)
        .map(|i| format!("{:064x}", u128::from(i as u32) * 0x9e37_79b9_7f4a_7c15))
        .collect::<Vec<_>>()
        .join(" ");
    let d = doc(DocumentType::WebPage, Some("IoC"), &hashes, false);
    let tok = tokenizer();
    assert!(tok.count(&hashes).unwrap() > 512, "測試資料要真的超過上限");
    let out = chunker().chunk_document(&d, Utc::now()).unwrap();
    assert!(out.hard_cuts >= 1);
    assert!(out.chunks.len() >= 2);
    assert_invariants(&d, &out.chunks);
}

#[test]
fn rechunk_is_deterministic() {
    let d = doc(
        DocumentType::WebPage,
        Some("t"),
        &long_mixed_body(60),
        false,
    );
    let a = chunker().chunk_document(&d, Utc::now()).unwrap().chunks;
    let b = chunker().chunk_document(&d, Utc::now()).unwrap().chunks;
    let strip = |v: &[DocumentChunk]| {
        v.iter()
            .map(|c| {
                (
                    c.id,
                    c.start_char,
                    c.end_char,
                    c.text.clone(),
                    c.token_count,
                )
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(strip(&a), strip(&b));
}

#[test]
fn empty_and_whitespace_documents_have_no_chunks() {
    let mut d = doc(DocumentType::WebPage, None, "", false);
    d.body = None;
    assert!(
        chunker()
            .chunk_document(&d, Utc::now())
            .unwrap()
            .chunks
            .is_empty()
    );
    let d = doc(DocumentType::WebPage, None, "  \n\n\t ", false);
    assert!(
        chunker()
            .chunk_document(&d, Utc::now())
            .unwrap()
            .chunks
            .is_empty()
    );
}

#[test]
fn source_over_byte_limit_is_truncated_and_flagged() {
    let config = ChunkerConfig {
        max_source_bytes: 1000,
        ..ChunkerConfig::default()
    };
    let chunker = Chunker::new(tokenizer(), config).unwrap();
    let d = doc(DocumentType::WebPage, None, &long_mixed_body(100), false);
    let out = chunker.chunk_document(&d, Utc::now()).unwrap();
    assert!(out.truncated);
    let last = out.chunks.last().unwrap();
    let source_chars = chunk_source_text(&d, Some(1000)).chars().count();
    assert!(last.end_char as usize <= source_chars);
    // 截斷後的文字仍是串接文字的前綴，偏移照樣對得上完整版。
    assert_invariants_prefix(&d, &out.chunks);
}

fn assert_invariants_prefix(document: &Document, chunks: &[DocumentChunk]) {
    let source: Vec<char> = chunk_source_text(document, None).chars().collect();
    for c in chunks {
        let slice: String = source[c.start_char as usize..c.end_char as usize]
            .iter()
            .collect();
        assert_eq!(slice, c.text);
    }
}

#[test]
fn invalid_config_is_rejected() {
    for config in [
        ChunkerConfig {
            overlap_tokens: 300,
            ..ChunkerConfig::default()
        },
        ChunkerConfig {
            target_tokens: 480,
            ..ChunkerConfig::default()
        },
        ChunkerConfig {
            target_tokens: 0,
            ..ChunkerConfig::default()
        },
    ] {
        assert!(matches!(
            Chunker::new(tokenizer(), config),
            Err(ChunkerError::Config(_))
        ));
    }
}

#[test]
fn long_title_header_is_capped() {
    let title = "很長的標題".repeat(100);
    let d = doc(
        DocumentType::WebPage,
        Some(&title),
        &long_mixed_body(40),
        false,
    );
    let out = chunker().chunk_document(&d, Utc::now()).unwrap();
    let tok = tokenizer();
    for c in out.chunks.iter().filter(|c| c.header.is_some()) {
        assert!(tok.count(c.header.as_deref().unwrap()).unwrap() <= 64);
    }
    assert_invariants(&d, &out.chunks);
}

/// 語料驗收（手動）：`CHUNKER_CORPUS=<jsonl>` 每行一份文件
/// `{"id","object_type","title","summary","body","attributes"}`，對全部文件跑不變式。
/// 語料不進 repo；產生方式見 `docs/developer/chunker.md`。
#[test]
#[ignore = "需要 CHUNKER_CORPUS 指向本機語料"]
fn corpus_invariants() {
    use std::io::BufRead;
    let path = std::env::var("CHUNKER_CORPUS").expect("CHUNKER_CORPUS");
    let file = std::fs::File::open(&path).expect("開語料");
    let chunker = chunker();
    let started = std::time::Instant::now();
    let (mut docs, mut chunks, mut fell_back, mut hard_cuts, mut truncated) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    let mut per_strategy = std::collections::BTreeMap::<&str, (u64, u64)>::new();
    let mut tokens: Vec<u32> = Vec::new();
    let mut max_chunks = 0usize;
    for line in std::io::BufReader::new(file).lines() {
        let v: serde_json::Value = serde_json::from_str(&line.expect("讀行")).expect("JSON");
        let s = |k: &str| v[k].as_str().map(str::to_string);
        let mut d = doc(
            serde_json::from_value(v["object_type"].clone()).expect("object_type"),
            None,
            "",
            false,
        );
        d.id = v["id"].as_str().unwrap().parse().unwrap();
        d.title = s("title");
        d.summary = s("summary");
        d.body = s("body");
        d.attributes = v["attributes"].clone();
        let out = chunker.chunk_document(&d, Utc::now()).expect("切段");
        let has_text = chunk_source_text(&d, None)
            .chars()
            .any(|c| !c.is_whitespace());
        assert!(
            !has_text || !out.chunks.is_empty(),
            "文件 {} 有文字卻 0 段",
            d.id
        );
        assert_invariants(&d, &out.chunks);
        docs += 1;
        chunks += out.chunks.len() as u64;
        fell_back += u64::from(out.fell_back_to_fixed);
        hard_cuts += u64::from(out.hard_cuts);
        truncated += u64::from(out.truncated);
        max_chunks = max_chunks.max(out.chunks.len());
        let e = per_strategy.entry(out.strategy.name()).or_default();
        e.0 += 1;
        e.1 += out.chunks.len() as u64;
        tokens.extend(out.chunks.iter().map(|c| c.token_count));
    }
    tokens.sort_unstable();
    let pct = |p: usize| tokens[(tokens.len() - 1) * p / 100];
    eprintln!(
        "語料 {docs} 份 → {chunks} 段，耗時 {:.1}s；策略 {per_strategy:?}；退回 fixed {fell_back}；硬切 {hard_cuts}；截斷 {truncated}；單份最多 {max_chunks} 段",
        started.elapsed().as_secs_f64()
    );
    eprintln!(
        "token_count p50={} p90={} p99={} max={}",
        pct(50),
        pct(90),
        pct(99),
        tokens.last().copied().unwrap_or(0)
    );
}
