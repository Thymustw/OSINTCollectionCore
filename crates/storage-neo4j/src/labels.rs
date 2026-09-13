//! Entity type／relationship type 字串 → Cypher identifier。
//!
//! `GraphNode.entity_type` 與 `GraphEdge.relationship_type` 都是 `String`，
//! 不是封閉 enum。寫入時必須 sanitize，否則動態 `$()` 語法會變成
//! Cypher injection 入口。呼叫端傳進來的值經過這裡才准許進 query。

use core_model::RelationshipType;
use storage_core::StorageError;

/// Neo4j 5.26 Community 對 relationship uniqueness constraint 必須綁
/// **特定關聯型別**。只對 `core_model::RelationshipType` 的 13 個變體建
/// constraint；執行期才出現的未知型別靠 MERGE pattern 保證冪等。
pub const KNOWN_RELATIONSHIP_TYPES: &[&str] = &[
    "MENTIONS",
    "REFERENCES",
    "PUBLISHED_BY",
    "AUTHORED_BY",
    "LINKS_TO",
    "AFFECTS",
    "BELONGS_TO",
    "MEMBER_OF",
    "OWNS",
    "USES",
    "LOCATED_AT",
    "ASSOCIATED_WITH",
    "DERIVED_FROM",
];

/// 讓 `RelationshipType` 新增變體時這裡編譯失敗，才不會漏建 constraint。
#[must_use]
pub(crate) fn relationship_type_cypher_name(t: RelationshipType) -> &'static str {
    match t {
        RelationshipType::Mentions => "MENTIONS",
        RelationshipType::References => "REFERENCES",
        RelationshipType::PublishedBy => "PUBLISHED_BY",
        RelationshipType::AuthoredBy => "AUTHORED_BY",
        RelationshipType::LinksTo => "LINKS_TO",
        RelationshipType::Affects => "AFFECTS",
        RelationshipType::BelongsTo => "BELONGS_TO",
        RelationshipType::MemberOf => "MEMBER_OF",
        RelationshipType::Owns => "OWNS",
        RelationshipType::Uses => "USES",
        RelationshipType::LocatedAt => "LOCATED_AT",
        RelationshipType::AssociatedWith => "ASSOCIATED_WITH",
        RelationshipType::DerivedFrom => "DERIVED_FROM",
    }
}

/// `"person"` → `"Person"`；`"ip"` → `"Ip"`。
///
/// 不認識的字串也盡量轉，不 panic、不拒絕寫入——`GraphNode.entity_type`
/// 之後可能帶 STIX 進來的非既有型別。非法字元在 [`sanitize_label`] 擋。
#[must_use]
pub fn snake_to_pascal(snake: &str) -> String {
    let mut out = String::with_capacity(snake.len());
    let mut cap = true;
    for ch in snake.chars() {
        if ch == '_' || ch == '-' {
            cap = true;
            continue;
        }
        if cap {
            for up in ch.to_uppercase() {
                out.push(up);
            }
            cap = false;
        } else {
            out.push(ch);
        }
    }
    out
}

/// `"associated_with"` → `"ASSOCIATED_WITH"`。純 ASCII 大寫，連字號也換成底線。
#[must_use]
pub fn to_rel_type(raw: &str) -> String {
    raw.chars()
        .map(|ch| {
            if ch == '-' {
                '_'
            } else {
                ch.to_ascii_uppercase()
            }
        })
        .collect()
}

/// Label 只能是 ASCII 字母開頭、後接字母／數字／底線。空字串或非法字元
/// 回 [`StorageError::ConstraintViolation`]，並告訴呼叫端下一步。
pub fn sanitize_label(label: &str) -> Result<&str, StorageError> {
    if label.is_empty() {
        return Err(StorageError::ConstraintViolation {
            message: "entity_type 轉成的 Neo4j label 是空的。請傳非空的 entity_type（例如 person）"
                .into(),
        });
    }
    let mut chars = label.chars();
    let Some(first) = chars.next() else {
        return Err(StorageError::ConstraintViolation {
            message: "entity_type 轉成的 Neo4j label 是空的。請傳非空的 entity_type".into(),
        });
    };
    if !first.is_ascii_alphabetic() {
        return Err(StorageError::ConstraintViolation {
            message: format!(
                "entity_type 轉成的 Neo4j label `{label}` 不是合法 identifier（必須以 ASCII 字母開頭）。\
                 請改成 snake_case 英數字，例如 person／organization"
            ),
        });
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return Err(StorageError::ConstraintViolation {
            message: format!(
                "entity_type 轉成的 Neo4j label `{label}` 含非法字元。\
                 只允許 ASCII 字母、數字、底線，避免 Cypher injection"
            ),
        });
    }
    Ok(label)
}

/// 關聯型別同 label：必須是合法 Cypher identifier。
pub fn sanitize_rel_type(rel: &str) -> Result<&str, StorageError> {
    if rel.is_empty() {
        return Err(StorageError::ConstraintViolation {
            message:
                "relationship_type 轉成的 Cypher 型別是空的。請傳非空字串（例如 associated_with）"
                    .into(),
        });
    }
    let mut chars = rel.chars();
    let Some(first) = chars.next() else {
        return Err(StorageError::ConstraintViolation {
            message: "relationship_type 轉成的 Cypher 型別是空的".into(),
        });
    };
    if !first.is_ascii_alphabetic() {
        return Err(StorageError::ConstraintViolation {
            message: format!(
                "relationship_type 轉成的 Cypher 型別 `{rel}` 不是合法 identifier（必須以 ASCII 字母開頭）"
            ),
        });
    }
    if !chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_') {
        return Err(StorageError::ConstraintViolation {
            message: format!(
                "relationship_type 轉成的 Cypher 型別 `{rel}` 含非法字元。只允許 ASCII 字母、數字、底線"
            ),
        });
    }
    Ok(rel)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snake_to_pascal_known_and_unknown() {
        assert_eq!(snake_to_pascal("person"), "Person");
        assert_eq!(snake_to_pascal("ip"), "Ip");
        assert_eq!(snake_to_pascal("threat_actor"), "ThreatActor");
        assert_eq!(snake_to_pascal("alreadyPascal"), "AlreadyPascal");
        assert_eq!(snake_to_pascal(""), "");
    }

    #[test]
    fn rel_type_is_upper_snake() {
        assert_eq!(to_rel_type("associated_with"), "ASSOCIATED_WITH");
        assert_eq!(to_rel_type("mentions"), "MENTIONS");
        assert_eq!(to_rel_type("published-by"), "PUBLISHED_BY");
    }

    #[test]
    fn sanitize_rejects_injection() {
        assert!(sanitize_label("Person").is_ok());
        assert!(sanitize_label("Foo_Bar1").is_ok());
        assert!(sanitize_label("").is_err());
        assert!(sanitize_label("1Person").is_err());
        assert!(sanitize_label("Person} DELETE").is_err());
        assert!(sanitize_rel_type("MENTIONS").is_ok());
        assert!(sanitize_rel_type("A`B").is_err());
    }

    #[test]
    fn known_rel_types_match_enum() {
        use RelationshipType::*;
        let from_enum: Vec<&str> = [
            Mentions,
            References,
            PublishedBy,
            AuthoredBy,
            LinksTo,
            Affects,
            BelongsTo,
            MemberOf,
            Owns,
            Uses,
            LocatedAt,
            AssociatedWith,
            DerivedFrom,
        ]
        .into_iter()
        .map(relationship_type_cypher_name)
        .collect();
        assert_eq!(from_enum, KNOWN_RELATIONSHIP_TYPES);
    }
}
