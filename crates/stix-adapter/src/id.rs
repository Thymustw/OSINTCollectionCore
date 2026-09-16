//! STIX 2.1 identifier：`{object-type}--{UUID}`。

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use uuid::Uuid;

use crate::error::StixError;

/// STIX 2.1 id。內部保存原始字串，建構時驗證過。
///
/// 格式：`type_prefix--uuid`，例如 `identity--2152fbe0-4471-4d43-8b64-0b907d186c23`。
/// `type_prefix` 必須是小寫字母開頭的 `[a-z][a-z0-9-]*`（含 `x-` 自訂型別）。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StixId(String);

impl StixId {
    /// 解析並驗證 STIX id。失敗時錯誤訊息會指出是前綴還是 UUID 出問題。
    pub fn parse(raw: impl AsRef<str>) -> Result<Self, StixError> {
        let raw = raw.as_ref();
        let Some((prefix, uuid_part)) = raw.split_once("--") else {
            return Err(StixError::InvalidId {
                id: raw.to_string(),
                message: "必須是 `type--uuid`，中間要有兩個連字號 `--`".into(),
            });
        };
        if prefix.is_empty() {
            return Err(StixError::InvalidId {
                id: raw.to_string(),
                message: "`--` 前面的 type 前綴不可為空".into(),
            });
        }
        if !is_valid_type_prefix(prefix) {
            return Err(StixError::InvalidId {
                id: raw.to_string(),
                message: format!(
                    "type 前綴 `{prefix}` 不合法：必須是小寫字母開頭，其餘只能是 \
                     小寫字母、數字或連字號（例如 `identity`、`ipv4-addr`、`x-custom-object`）"
                ),
            });
        }
        if uuid_part.is_empty() {
            return Err(StixError::InvalidId {
                id: raw.to_string(),
                message: "`--` 後面的 UUID 不可為空".into(),
            });
        }
        if Uuid::parse_str(uuid_part).is_err() {
            return Err(StixError::InvalidId {
                id: raw.to_string(),
                message: format!(
                    "UUID 部分 `{uuid_part}` 不是合法 RFC 4122 UUID，\
                     請用帶連字號的 8-4-4-4-12 形式"
                ),
            });
        }
        Ok(Self(raw.to_string()))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// `--` 前面的 STIX type 名稱。
    #[must_use]
    pub fn type_prefix(&self) -> &str {
        self.0
            .split_once("--")
            .map(|(prefix, _)| prefix)
            .unwrap_or("")
    }

    /// `--` 後面的 UUID 字串。
    #[must_use]
    pub fn uuid_part(&self) -> &str {
        self.0
            .split_once("--")
            .map(|(_, uuid_part)| uuid_part)
            .unwrap_or("")
    }
}

impl fmt::Display for StixId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl AsRef<str> for StixId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl FromStr for StixId {
    type Err = StixError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::parse(s)
    }
}

impl Serialize for StixId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for StixId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        Self::parse(raw).map_err(serde::de::Error::custom)
    }
}

fn is_valid_type_prefix(prefix: &str) -> bool {
    let mut chars = prefix.chars();
    match chars.next() {
        Some(c) if c.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
}

#[cfg(test)]
mod tests {
    use super::*;

    const UUID: &str = "2152fbe0-4471-4d43-8b64-0b907d186c23";

    #[test]
    fn parse_accepts_standard_sdo_id() {
        let raw = format!("identity--{UUID}");
        let id = StixId::parse(&raw).expect("合法 id");
        assert_eq!(id.as_str(), raw);
        assert_eq!(id.type_prefix(), "identity");
        assert_eq!(id.uuid_part(), UUID);
        assert_eq!(id.to_string(), raw);
    }

    #[test]
    fn parse_accepts_sco_and_custom_prefixes() {
        StixId::parse(format!("ipv4-addr--{UUID}")).expect("sco");
        StixId::parse(format!("domain-name--{UUID}")).expect("sco");
        StixId::parse(format!("x-osint-core-object--{UUID}")).expect("custom");
        StixId::parse(format!("bundle--{UUID}")).expect("bundle");
    }

    #[test]
    fn parse_rejects_missing_delimiter() {
        let err = StixId::parse(format!("identity-{UUID}")).expect_err("缺 --");
        match err {
            StixError::InvalidId { id, message } => {
                assert!(id.contains("identity-"));
                assert!(message.contains("兩個連字號"));
            }
            other => panic!("預期 InvalidId，得到 {other:?}"),
        }
    }

    #[test]
    fn parse_rejects_empty_prefix_or_uuid() {
        assert!(matches!(
            StixId::parse(format!("--{UUID}")),
            Err(StixError::InvalidId { .. })
        ));
        assert!(matches!(
            StixId::parse("identity--"),
            Err(StixError::InvalidId { .. })
        ));
    }

    #[test]
    fn parse_rejects_uppercase_prefix() {
        let err = StixId::parse(format!("Identity--{UUID}")).expect_err("大寫");
        let StixError::InvalidId { message, .. } = err else {
            panic!("預期 InvalidId");
        };
        assert!(message.contains("小寫"), "{message}");
    }

    #[test]
    fn parse_rejects_non_uuid_suffix() {
        let err = StixId::parse("identity--not-a-uuid").expect_err("假 UUID");
        let StixError::InvalidId { message, .. } = err else {
            panic!("預期 InvalidId");
        };
        assert!(message.contains("RFC 4122"), "{message}");
    }

    #[test]
    fn serde_round_trip() {
        let id = StixId::parse(format!("malware--{UUID}")).unwrap();
        let json = serde_json::to_string(&id).unwrap();
        assert_eq!(json, format!("\"malware--{UUID}\""));
        let back: StixId = serde_json::from_str(&json).unwrap();
        assert_eq!(back, id);
    }
}
