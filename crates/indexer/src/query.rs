//! 使用者搜尋語法 → [`QueryExpr`]（SPEC §18 的 keyword／phrase／boolean）。
//!
//! # 為什麼自己 parse，而不是把字串交給 OpenSearch
//!
//! `query_string` 讓使用者可以寫 `欄位名:值`、`*`、`?`、`~`、`/regex/`、`_exists_:`。
//! 那不只是「語法比較豐富」——它是一個**查詢注入面**：
//!
//! | 輸入 | `query_string` 的行為 | 這裡的行為 |
//! |---|---|---|
//! | `title:*` | 對 title 做萬用比對 | 一個要比對的詞：字面上的 `title:*` |
//! | `*` | 命中全部文件 | 一個要比對的詞：字面上的 `*` |
//! | `body:/.*(a\|b)*.*/` | 正規表示式，可做 CPU DoS | 一個要比對的詞 |
//! | `_id:abc` | 直接指定內部欄位 | 一個要比對的詞 |
//!
//! `simple_query_string` 擋掉了欄位名與 regex，但仍保留 `*` 前綴萬用與 `~` 模糊比對，
//! 而且它會**靜默吞掉**語法錯誤（`AND AND` 不會報錯，只是不照你想的做）。
//! 選擇自己 parse 的代價是語法比較少，換到的是：任何使用者字串都只可能落在
//! [`QueryExpr::Term`] 或 [`QueryExpr::Phrase`] 的 `query` 值裡，
//! 不可能變成查詢語言的結構。
//!
//! # 語法
//!
//! ```text
//! ransomware              一個詞
//! "ransomware gang"       片語（詞序必須相符）
//! a b                     兩個詞都要有（相鄰預設是 AND）
//! a AND b                 同上，寫明
//! a OR b                  至少一個
//! a NOT b                 有 a、沒有 b
//! (a OR b) AND c          括號分組
//! ```
//!
//! `AND`／`OR`／`NOT` 必須**全大寫**才算運算子。小寫的 `and` 是一個普通的詞——
//! 否則使用者查 `crowdstrike and falcon` 會得到一個他沒打算下的布林運算。

use storage_core::QueryExpr;

/// 查詢字串長度上限（以 char 計）。
pub const MAX_QUERY_CHARS: usize = 1024;
/// token 數上限。每個 token 至少一個 OpenSearch 子查詢，不設限等於讓使用者
/// 用一行字串產生無界大小的查詢樹。
pub const MAX_TOKENS: usize = 64;
/// 括號巢狀深度上限。與 storage-opensearch 的 `MAX_EXPR_DEPTH` 對齊。
pub const MAX_DEPTH: usize = 8;

/// 解析失敗。每一則訊息都要讓使用者知道怎麼改，不是只說「語法錯誤」。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum QueryParseError {
    #[error(
        "查詢字串太長（{actual} 個字元，上限 {MAX_QUERY_CHARS}）。\
         請縮短關鍵字，或改用 source／entity／日期等過濾條件縮小範圍"
    )]
    TooLong { actual: usize },
    #[error(
        "查詢條件太多（{actual} 個詞，上限 {MAX_TOKENS}）。\
         請拆成多次查詢，或改用過濾條件取代大量 OR"
    )]
    TooManyTokens { actual: usize },
    #[error("括號巢狀太深（上限 {MAX_DEPTH} 層）。請把條件拆開成多次查詢")]
    TooDeep,
    #[error("引號沒有成對。請補上結尾的 `\"`，例如 `\"ransomware gang\"`")]
    UnterminatedQuote,
    #[error("多了一個 `)`。請檢查括號是否成對")]
    UnexpectedCloseParen,
    #[error("少了一個 `)`。請補上與 `(` 對應的結尾括號")]
    MissingCloseParen,
    #[error(
        "`{operator}` 後面沒有接查詢條件。\
         請在它後面補上要查的詞，例如 `ransomware {operator} lockbit`"
    )]
    DanglingOperator { operator: &'static str },
    #[error("`{operator}` 前面沒有查詢條件。布林運算子要放在兩個條件之間")]
    LeadingOperator { operator: &'static str },
    #[error("空的括號 `()` 沒有意義。請在括號裡放查詢條件，或整個拿掉")]
    EmptyGroup,
    #[error("空的引號 `\"\"` 沒有意義。請在引號裡放要精確比對的片語，或整個拿掉")]
    EmptyPhrase,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Word(String),
    Phrase(String),
    And,
    Or,
    Not,
    Open,
    Close,
}

impl Token {
    fn operator_name(&self) -> Option<&'static str> {
        match self {
            Self::And => Some("AND"),
            Self::Or => Some("OR"),
            Self::Not => Some("NOT"),
            _ => None,
        }
    }
}

/// 把使用者輸入解析成查詢語法樹。
///
/// 空字串（或只有空白）回 `Ok(None)`：那不是錯誤，而是「沒有全文條件」——
/// 例如只用 `source_id` 過濾列出某個來源的全部文件。
pub fn parse(input: &str) -> Result<Option<QueryExpr>, QueryParseError> {
    let count = input.chars().count();
    if count > MAX_QUERY_CHARS {
        return Err(QueryParseError::TooLong { actual: count });
    }
    let tokens = tokenize(input)?;
    if tokens.is_empty() {
        return Ok(None);
    }
    if tokens.len() > MAX_TOKENS {
        return Err(QueryParseError::TooManyTokens {
            actual: tokens.len(),
        });
    }
    let mut parser = Parser { tokens, pos: 0 };
    let expr = parser.parse_or(0)?;
    if parser.peek().is_some() {
        // 走到這裡只可能是多餘的 `)`：其他 token 都會被 parse_and 吃掉。
        return Err(QueryParseError::UnexpectedCloseParen);
    }
    Ok(Some(expr))
}

fn tokenize(input: &str) -> Result<Vec<Token>, QueryParseError> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(&ch) = chars.peek() {
        match ch {
            c if c.is_whitespace() => {
                chars.next();
            }
            '(' => {
                chars.next();
                tokens.push(Token::Open);
            }
            ')' => {
                chars.next();
                tokens.push(Token::Close);
            }
            '"' => {
                chars.next();
                let mut phrase = String::new();
                let mut closed = false;
                for c in chars.by_ref() {
                    if c == '"' {
                        closed = true;
                        break;
                    }
                    phrase.push(c);
                }
                if !closed {
                    return Err(QueryParseError::UnterminatedQuote);
                }
                if phrase.trim().is_empty() {
                    return Err(QueryParseError::EmptyPhrase);
                }
                tokens.push(Token::Phrase(phrase.trim().to_string()));
            }
            _ => {
                let mut word = String::new();
                while let Some(&c) = chars.peek() {
                    if c.is_whitespace() || c == '(' || c == ')' || c == '"' {
                        break;
                    }
                    word.push(c);
                    chars.next();
                }
                // 只有全大寫才是運算子。小寫的 and/or/not 是普通的詞。
                tokens.push(match word.as_str() {
                    "AND" => Token::And,
                    "OR" => Token::Or,
                    "NOT" => Token::Not,
                    _ => Token::Word(word),
                });
            }
        }
        if tokens.len() > MAX_TOKENS {
            return Err(QueryParseError::TooManyTokens {
                actual: tokens.len(),
            });
        }
    }
    Ok(tokens)
}

struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.pos)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.pos).cloned();
        if token.is_some() {
            self.pos += 1;
        }
        token
    }

    /// `or := and (OR and)*`
    fn parse_or(&mut self, depth: usize) -> Result<QueryExpr, QueryParseError> {
        if depth > MAX_DEPTH {
            return Err(QueryParseError::TooDeep);
        }
        if let Some(name) = self.peek().and_then(Token::operator_name) {
            if name != "NOT" {
                return Err(QueryParseError::LeadingOperator { operator: name });
            }
        }
        let mut parts = vec![self.parse_and(depth)?];
        while matches!(self.peek(), Some(Token::Or)) {
            self.next();
            if self.peek().is_none() || matches!(self.peek(), Some(Token::Close)) {
                return Err(QueryParseError::DanglingOperator { operator: "OR" });
            }
            parts.push(self.parse_and(depth)?);
        }
        Ok(if parts.len() == 1 {
            parts.remove(0)
        } else {
            QueryExpr::Or(parts)
        })
    }

    /// `and := unary (AND? unary)*`——相鄰的兩個條件預設就是 AND。
    fn parse_and(&mut self, depth: usize) -> Result<QueryExpr, QueryParseError> {
        let mut parts = vec![self.parse_unary(depth)?];
        loop {
            match self.peek() {
                Some(Token::And) => {
                    self.next();
                    if self.peek().is_none() || matches!(self.peek(), Some(Token::Close)) {
                        return Err(QueryParseError::DanglingOperator { operator: "AND" });
                    }
                    parts.push(self.parse_unary(depth)?);
                }
                // 隱含 AND：`a b`、`a "b c"`、`a (b OR c)`、`a NOT b`
                Some(Token::Word(_) | Token::Phrase(_) | Token::Open | Token::Not) => {
                    parts.push(self.parse_unary(depth)?);
                }
                _ => break,
            }
        }
        Ok(if parts.len() == 1 {
            parts.remove(0)
        } else {
            QueryExpr::And(parts)
        })
    }

    /// `unary := NOT unary | primary`
    fn parse_unary(&mut self, depth: usize) -> Result<QueryExpr, QueryParseError> {
        if matches!(self.peek(), Some(Token::Not)) {
            self.next();
            if self.peek().is_none() || matches!(self.peek(), Some(Token::Close)) {
                return Err(QueryParseError::DanglingOperator { operator: "NOT" });
            }
            return Ok(QueryExpr::Not(Box::new(self.parse_unary(depth + 1)?)));
        }
        self.parse_primary(depth)
    }

    /// `primary := '(' or ')' | PHRASE | WORD`
    fn parse_primary(&mut self, depth: usize) -> Result<QueryExpr, QueryParseError> {
        match self.next() {
            Some(Token::Word(word)) => Ok(QueryExpr::Term(word)),
            Some(Token::Phrase(phrase)) => Ok(QueryExpr::Phrase(phrase)),
            Some(Token::Open) => {
                if matches!(self.peek(), Some(Token::Close)) {
                    return Err(QueryParseError::EmptyGroup);
                }
                let inner = self.parse_or(depth + 1)?;
                match self.next() {
                    Some(Token::Close) => Ok(inner),
                    _ => Err(QueryParseError::MissingCloseParen),
                }
            }
            Some(Token::Close) => Err(QueryParseError::UnexpectedCloseParen),
            Some(token) => Err(QueryParseError::LeadingOperator {
                operator: token.operator_name().unwrap_or("AND"),
            }),
            None => Err(QueryParseError::DanglingOperator { operator: "AND" }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn term(text: &str) -> QueryExpr {
        QueryExpr::Term(text.into())
    }

    #[test]
    fn empty_query_is_not_an_error() {
        assert_eq!(parse("").unwrap(), None);
        assert_eq!(parse("   \n ").unwrap(), None);
    }

    #[test]
    fn single_keyword() {
        assert_eq!(parse("ransomware").unwrap(), Some(term("ransomware")));
    }

    #[test]
    fn quoted_text_is_a_phrase() {
        assert_eq!(
            parse("\"ransomware gang\"").unwrap(),
            Some(QueryExpr::Phrase("ransomware gang".into()))
        );
    }

    #[test]
    fn adjacent_terms_default_to_and() {
        assert_eq!(
            parse("lockbit ransomware").unwrap(),
            Some(QueryExpr::And(vec![term("lockbit"), term("ransomware")])),
            "相鄰預設 OR 會讓多打一個字反而拿到更多結果，違反直覺"
        );
    }

    #[test]
    fn explicit_boolean_operators() {
        assert_eq!(
            parse("a AND b").unwrap(),
            Some(QueryExpr::And(vec![term("a"), term("b")]))
        );
        assert_eq!(
            parse("a OR b").unwrap(),
            Some(QueryExpr::Or(vec![term("a"), term("b")]))
        );
        assert_eq!(
            parse("a NOT b").unwrap(),
            Some(QueryExpr::And(vec![
                term("a"),
                QueryExpr::Not(Box::new(term("b")))
            ]))
        );
    }

    #[test]
    fn or_binds_looser_than_and() {
        // `a AND b OR c` 必須是 `(a AND b) OR c`，不是 `a AND (b OR c)`。
        assert_eq!(
            parse("a AND b OR c").unwrap(),
            Some(QueryExpr::Or(vec![
                QueryExpr::And(vec![term("a"), term("b")]),
                term("c"),
            ]))
        );
    }

    #[test]
    fn parentheses_override_precedence() {
        assert_eq!(
            parse("(a OR b) AND c").unwrap(),
            Some(QueryExpr::And(vec![
                QueryExpr::Or(vec![term("a"), term("b")]),
                term("c"),
            ]))
        );
    }

    #[test]
    fn lowercase_operators_are_ordinary_words() {
        // 使用者查 `crowdstrike and falcon` 想要的是三個詞，不是布林運算。
        assert_eq!(
            parse("a and b").unwrap(),
            Some(QueryExpr::And(vec![term("a"), term("and"), term("b")]))
        );
    }

    #[test]
    fn opensearch_syntax_stays_literal() {
        // 這是 injection 防護的核心斷言：任何 OpenSearch 語法字元都只會變成 Term。
        for raw in ["title:*", "*", "_id:abc", "body:/.*/", "a~2", "?e?t"] {
            assert_eq!(
                parse(raw).unwrap(),
                Some(term(raw)),
                "`{raw}` 應該原樣變成一個要比對的詞"
            );
        }
    }

    #[test]
    fn cjk_query_is_a_single_term() {
        assert_eq!(parse("勒索軟體").unwrap(), Some(term("勒索軟體")));
    }

    #[test]
    fn too_long_is_rejected_with_actionable_message() {
        let err = parse(&"x".repeat(MAX_QUERY_CHARS + 1)).unwrap_err();
        assert!(matches!(err, QueryParseError::TooLong { .. }));
        assert!(err.to_string().contains("縮短"), "{err}");
    }

    #[test]
    fn too_many_tokens_is_rejected() {
        let input = (0..MAX_TOKENS + 5)
            .map(|i| format!("w{i}"))
            .collect::<Vec<_>>()
            .join(" ");
        assert!(matches!(
            parse(&input).unwrap_err(),
            QueryParseError::TooManyTokens { .. }
        ));
    }

    #[test]
    fn deep_nesting_is_rejected() {
        let input = format!("{}a{}", "(".repeat(20), ")".repeat(20));
        assert_eq!(parse(&input).unwrap_err(), QueryParseError::TooDeep);
    }

    #[test]
    fn unterminated_quote_says_how_to_fix() {
        let err = parse("\"ransomware").unwrap_err();
        assert_eq!(err, QueryParseError::UnterminatedQuote);
        assert!(err.to_string().contains("補上"), "{err}");
    }

    #[test]
    fn unbalanced_parens_are_rejected() {
        assert_eq!(parse("(a").unwrap_err(), QueryParseError::MissingCloseParen);
        assert_eq!(
            parse("a)").unwrap_err(),
            QueryParseError::UnexpectedCloseParen
        );
    }

    #[test]
    fn dangling_and_leading_operators_are_rejected() {
        assert_eq!(
            parse("a AND").unwrap_err(),
            QueryParseError::DanglingOperator { operator: "AND" }
        );
        assert_eq!(
            parse("OR b").unwrap_err(),
            QueryParseError::LeadingOperator { operator: "OR" }
        );
        assert_eq!(
            parse("NOT").unwrap_err(),
            QueryParseError::DanglingOperator { operator: "NOT" }
        );
    }

    #[test]
    fn empty_group_and_phrase_are_rejected() {
        assert_eq!(parse("()").unwrap_err(), QueryParseError::EmptyGroup);
        assert_eq!(parse("\"\"").unwrap_err(), QueryParseError::EmptyPhrase);
    }

    #[test]
    fn leading_not_is_allowed() {
        // `NOT a` 是合法的：全部文件裡排除 a。
        assert_eq!(
            parse("NOT a").unwrap(),
            Some(QueryExpr::Not(Box::new(term("a"))))
        );
    }
}
