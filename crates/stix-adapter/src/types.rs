//! STIX 2.1 最小型別集合（SPEC_V0.2 §19 七種映射所需，不覆蓋完整規格）。

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::id::StixId;

/// SDO／SRO／SCO 共用欄位。STIX 2.1 允許 SCO 省略這些，所以全部是 `Option`。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CommonProperties {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spec_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub modified: Option<DateTime<Utc>>,
}

/// STIX 2.1 `identity_class` open vocabulary。
///
/// 規格至少要用到 `individual`／`organization`（Person／Organization → Identity）。
/// 其餘官方值一併收下；不認識的字串進 [`IdentityClass::Other`]，不要拒絕。
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum IdentityClass {
    Individual,
    Organization,
    Group,
    Class,
    System,
    Unknown,
    #[serde(untagged)]
    Other(String),
}

/// `type: "bundle"`。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StixBundle {
    #[serde(rename = "type")]
    pub type_: String,
    pub id: StixId,
    pub objects: Vec<StixObject>,
}

/// STIX 物件 tagged union。未知 type 不丟資料：`x-` 進 [`StixObject::Custom`]，
/// 其餘進 [`StixObject::Unknown`]。
#[derive(Debug, Clone, PartialEq)]
pub enum StixObject {
    Identity(Identity),
    ThreatActor(ThreatActor),
    Malware(Malware),
    Vulnerability(Vulnerability),
    Indicator(Indicator),
    Relationship(Relationship),
    DomainName(DomainName),
    Ipv4Addr(Ipv4Addr),
    Url(Url),
    EmailAddr(EmailAddr),
    Custom(CustomObject),
    Unknown(Value),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Identity {
    pub id: StixId,
    pub identity_class: IdentityClass,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(flatten)]
    pub common: CommonProperties,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThreatActor {
    pub id: StixId,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(flatten)]
    pub common: CommonProperties,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Malware {
    pub id: StixId,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is_family: Option<bool>,
    #[serde(flatten)]
    pub common: CommonProperties,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Vulnerability {
    pub id: StixId,
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(flatten)]
    pub common: CommonProperties,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Indicator {
    pub id: StixId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pattern: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(flatten)]
    pub common: CommonProperties,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Relationship {
    pub id: StixId,
    pub relationship_type: String,
    pub source_ref: StixId,
    pub target_ref: StixId,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(flatten)]
    pub common: CommonProperties,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DomainName {
    pub id: StixId,
    pub value: String,
    #[serde(flatten)]
    pub common: CommonProperties,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Ipv4Addr {
    pub id: StixId,
    pub value: String,
    #[serde(flatten)]
    pub common: CommonProperties,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Url {
    pub id: StixId,
    pub value: String,
    #[serde(flatten)]
    pub common: CommonProperties,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EmailAddr {
    pub id: StixId,
    pub value: String,
    #[serde(flatten)]
    pub common: CommonProperties,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

/// `type` 以 `x-` 開頭的自訂物件。所有自訂欄位（含之後 Step 1 的
/// `x_osint_core_type`／`x_osint_core_id`／`x_osint_core_provenance`）都進
/// [`CustomObject::extra`]，這一步不解釋語意。
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CustomObject {
    #[serde(rename = "type")]
    pub type_: String,
    pub id: StixId,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl StixObject {
    /// STIX `type` 字串。`Unknown` 從原始 JSON 讀；讀不到就回空字串。
    #[must_use]
    pub fn type_name(&self) -> &str {
        match self {
            Self::Identity(_) => "identity",
            Self::ThreatActor(_) => "threat-actor",
            Self::Malware(_) => "malware",
            Self::Vulnerability(_) => "vulnerability",
            Self::Indicator(_) => "indicator",
            Self::Relationship(_) => "relationship",
            Self::DomainName(_) => "domain-name",
            Self::Ipv4Addr(_) => "ipv4-addr",
            Self::Url(_) => "url",
            Self::EmailAddr(_) => "email-addr",
            Self::Custom(obj) => obj.type_.as_str(),
            Self::Unknown(value) => value.get("type").and_then(Value::as_str).unwrap_or(""),
        }
    }

    /// 這個物件的 STIX id。`Unknown` 從原始 JSON 讀不到就回 `None`——
    /// 這種物件本來就不會進 `entity_to_stix_object`／`stix_object_to_entity` 的對映，
    /// 呼叫端不該假設它一定有合法 id。
    #[must_use]
    pub fn id(&self) -> Option<&StixId> {
        match self {
            Self::Identity(v) => Some(&v.id),
            Self::ThreatActor(v) => Some(&v.id),
            Self::Malware(v) => Some(&v.id),
            Self::Vulnerability(v) => Some(&v.id),
            Self::Indicator(v) => Some(&v.id),
            Self::Relationship(v) => Some(&v.id),
            Self::DomainName(v) => Some(&v.id),
            Self::Ipv4Addr(v) => Some(&v.id),
            Self::Url(v) => Some(&v.id),
            Self::EmailAddr(v) => Some(&v.id),
            Self::Custom(v) => Some(&v.id),
            Self::Unknown(_) => None,
        }
    }
}

impl Serialize for StixObject {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Tagged<T: Serialize> {
            #[serde(rename = "type")]
            type_: &'static str,
            #[serde(flatten)]
            inner: T,
        }

        match self {
            Self::Identity(inner) => Tagged {
                type_: "identity",
                inner,
            }
            .serialize(serializer),
            Self::ThreatActor(inner) => Tagged {
                type_: "threat-actor",
                inner,
            }
            .serialize(serializer),
            Self::Malware(inner) => Tagged {
                type_: "malware",
                inner,
            }
            .serialize(serializer),
            Self::Vulnerability(inner) => Tagged {
                type_: "vulnerability",
                inner,
            }
            .serialize(serializer),
            Self::Indicator(inner) => Tagged {
                type_: "indicator",
                inner,
            }
            .serialize(serializer),
            Self::Relationship(inner) => Tagged {
                type_: "relationship",
                inner,
            }
            .serialize(serializer),
            Self::DomainName(inner) => Tagged {
                type_: "domain-name",
                inner,
            }
            .serialize(serializer),
            Self::Ipv4Addr(inner) => Tagged {
                type_: "ipv4-addr",
                inner,
            }
            .serialize(serializer),
            Self::Url(inner) => Tagged {
                type_: "url",
                inner,
            }
            .serialize(serializer),
            Self::EmailAddr(inner) => Tagged {
                type_: "email-addr",
                inner,
            }
            .serialize(serializer),
            Self::Custom(inner) => inner.serialize(serializer),
            Self::Unknown(value) => value.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for StixObject {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = Value::deserialize(deserializer)?;
        let type_name = value
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        dispatch_object(&type_name, value).map_err(serde::de::Error::custom)
    }
}

fn dispatch_object(type_name: &str, value: Value) -> Result<StixObject, String> {
    match type_name {
        "identity" => from_known("identity", value, StixObject::Identity),
        "threat-actor" => from_known("threat-actor", value, StixObject::ThreatActor),
        "malware" => from_known("malware", value, StixObject::Malware),
        "vulnerability" => from_known("vulnerability", value, StixObject::Vulnerability),
        "indicator" => from_known("indicator", value, StixObject::Indicator),
        "relationship" => from_known("relationship", value, StixObject::Relationship),
        "domain-name" => from_known("domain-name", value, StixObject::DomainName),
        "ipv4-addr" => from_known("ipv4-addr", value, StixObject::Ipv4Addr),
        "url" => from_known("url", value, StixObject::Url),
        "email-addr" => from_known("email-addr", value, StixObject::EmailAddr),
        other if other.starts_with("x-") => serde_json::from_value(value)
            .map(StixObject::Custom)
            .map_err(|err| format!("自訂物件 `{other}` 解析失敗：{err}")),
        _ => Ok(StixObject::Unknown(value)),
    }
}

fn from_known<T, F>(type_name: &str, mut value: Value, wrap: F) -> Result<StixObject, String>
where
    T: for<'de> Deserialize<'de>,
    F: FnOnce(T) -> StixObject,
{
    // `type` 由 tagged union 自己寫回去；留在 JSON 裡會被 `extra` flatten 吃進去，
    // round-trip 變成重複鍵或把 `type` 誤當成 custom property。
    if let Value::Object(map) = &mut value {
        map.remove("type");
    }
    serde_json::from_value(value)
        .map(wrap)
        .map_err(|err| format!("STIX `{type_name}` 物件解析失敗：{err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const UUID: &str = "2152fbe0-4471-4d43-8b64-0b907d186c23";

    fn round_trip(obj: &StixObject) -> StixObject {
        let json = serde_json::to_value(obj).expect("serialize");
        serde_json::from_value(json).expect("deserialize")
    }

    #[test]
    fn identity_round_trip_keeps_custom_property() {
        let raw = json!({
            "type": "identity",
            "id": format!("identity--{UUID}"),
            "spec_version": "2.1",
            "created": "2026-09-17T00:00:00.000Z",
            "modified": "2026-09-17T00:00:00.000Z",
            "name": "Acme",
            "identity_class": "organization",
            "description": "測試組織",
            "x_osint_note": "keep-me"
        });
        let obj: StixObject = serde_json::from_value(raw.clone()).unwrap();
        match &obj {
            StixObject::Identity(identity) => {
                assert_eq!(identity.name, "Acme");
                assert_eq!(identity.identity_class, IdentityClass::Organization);
                assert_eq!(identity.extra.get("x_osint_note"), Some(&json!("keep-me")));
            }
            other => panic!("預期 Identity，得到 {other:?}"),
        }
        let back = serde_json::to_value(&obj).unwrap();
        assert_eq!(back["x_osint_note"], "keep-me");
        assert_eq!(back["type"], "identity");
        assert_eq!(round_trip(&obj), obj);
    }

    #[test]
    fn identity_class_other_keeps_unknown_vocabulary() {
        let raw = json!({
            "type": "identity",
            "id": format!("identity--{UUID}"),
            "name": "X",
            "identity_class": "sector"
        });
        let obj: StixObject = serde_json::from_value(raw).unwrap();
        match obj {
            StixObject::Identity(identity) => {
                assert_eq!(
                    identity.identity_class,
                    IdentityClass::Other("sector".into())
                );
            }
            other => panic!("預期 Identity，得到 {other:?}"),
        }
    }

    #[test]
    fn threat_actor_malware_vulnerability_indicator_round_trip() {
        let cases = [
            json!({
                "type": "threat-actor",
                "id": format!("threat-actor--{UUID}"),
                "name": "APT-X",
                "description": "d"
            }),
            json!({
                "type": "malware",
                "id": format!("malware--{UUID}"),
                "name": "Family",
                "is_family": true
            }),
            json!({
                "type": "vulnerability",
                "id": format!("vulnerability--{UUID}"),
                "name": "CVE-2024-0001"
            }),
            json!({
                "type": "indicator",
                "id": format!("indicator--{UUID}"),
                "name": "bad domain",
                "pattern": "[domain-name:value = 'evil.example']"
            }),
        ];
        for raw in cases {
            let obj: StixObject = serde_json::from_value(raw).unwrap();
            assert_eq!(round_trip(&obj), obj);
        }
    }

    #[test]
    fn relationship_round_trip() {
        let raw = json!({
            "type": "relationship",
            "id": format!("relationship--{UUID}"),
            "relationship_type": "indicates",
            "source_ref": format!("indicator--{UUID}"),
            "target_ref": format!("malware--{UUID}"),
            "description": "連到惡意軟體"
        });
        let obj: StixObject = serde_json::from_value(raw).unwrap();
        match &obj {
            StixObject::Relationship(rel) => {
                assert_eq!(rel.relationship_type, "indicates");
                assert_eq!(rel.source_ref.type_prefix(), "indicator");
            }
            other => panic!("預期 Relationship，得到 {other:?}"),
        }
        assert_eq!(round_trip(&obj), obj);
    }

    #[test]
    fn sco_round_trips() {
        let cases = [
            json!({"type": "domain-name", "id": format!("domain-name--{UUID}"), "value": "example.com"}),
            json!({"type": "ipv4-addr", "id": format!("ipv4-addr--{UUID}"), "value": "192.0.2.1"}),
            json!({"type": "url", "id": format!("url--{UUID}"), "value": "https://example.com/a"}),
            json!({"type": "email-addr", "id": format!("email-addr--{UUID}"), "value": "a@example.com"}),
        ];
        for raw in cases {
            let obj: StixObject = serde_json::from_value(raw).unwrap();
            assert_eq!(round_trip(&obj), obj);
        }
    }

    #[test]
    fn custom_object_round_trip_keeps_arbitrary_keys() {
        let raw = json!({
            "type": "x-osint-core-object",
            "id": format!("x-osint-core-object--{UUID}"),
            "x_osint_core_type": "account",
            "x_osint_core_id": "01993c6a-7c3e-7a11-8000-7c3e7a110001",
            "x_osint_core_provenance": {"source": "manual"}
        });
        let obj: StixObject = serde_json::from_value(raw.clone()).unwrap();
        match &obj {
            StixObject::Custom(custom) => {
                assert_eq!(custom.type_, "x-osint-core-object");
                assert_eq!(
                    custom.extra.get("x_osint_core_type"),
                    Some(&json!("account"))
                );
            }
            other => panic!("預期 Custom，得到 {other:?}"),
        }
        let back = serde_json::to_value(&obj).unwrap();
        assert_eq!(back["x_osint_core_type"], "account");
        assert_eq!(back["x_osint_core_provenance"], json!({"source": "manual"}));
        assert_eq!(round_trip(&obj), obj);
    }

    #[test]
    fn known_entity_object_id_returns_stix_id() {
        // 對已知 Entity（Domain）組出 STIX 物件，`.id()` 應回帶合法 STIX id 的物件。
        let json = json!({
            "type": "domain-name",
            "id": format!("domain-name--{UUID}"),
            "value": "example.com"
        });
        let obj: StixObject = serde_json::from_value(json).unwrap();
        let id = obj.id().expect("已知型別必有 id");
        assert_eq!(id.type_prefix(), "domain-name");
        assert_eq!(id.uuid_part(), UUID);
    }

    #[test]
    fn unknown_object_id_is_none() {
        let obj = StixObject::Unknown(json!({"type": "campaign", "name": "Op-X"}));
        assert!(obj.id().is_none());
    }

    #[test]
    fn unknown_type_preserves_raw_json() {
        let raw = json!({
            "type": "campaign",
            "id": format!("campaign--{UUID}"),
            "name": "Op-X",
            "custom_field": 1
        });
        let obj: StixObject = serde_json::from_value(raw.clone()).unwrap();
        match &obj {
            StixObject::Unknown(value) => assert_eq!(value, &raw),
            other => panic!("預期 Unknown，得到 {other:?}"),
        }
        assert_eq!(serde_json::to_value(&obj).unwrap(), raw);
        assert_eq!(round_trip(&obj), obj);
    }

    #[test]
    fn bundle_parse_identity_and_relationship() {
        let raw = json!({
            "type": "bundle",
            "id": format!("bundle--{UUID}"),
            "objects": [
                {
                    "type": "identity",
                    "id": format!("identity--{UUID}"),
                    "name": "Alice",
                    "identity_class": "individual"
                },
                {
                    "type": "relationship",
                    "id": format!("relationship--{UUID}"),
                    "relationship_type": "attributed-to",
                    "source_ref": format!("threat-actor--{UUID}"),
                    "target_ref": format!("identity--{UUID}")
                }
            ]
        });
        let bundle: StixBundle = serde_json::from_value(raw).unwrap();
        assert_eq!(bundle.type_, "bundle");
        assert_eq!(bundle.id.type_prefix(), "bundle");
        assert_eq!(bundle.objects.len(), 2);
        assert!(matches!(bundle.objects[0], StixObject::Identity(_)));
        assert!(matches!(bundle.objects[1], StixObject::Relationship(_)));
        let back = serde_json::to_value(&bundle).unwrap();
        let again: StixBundle = serde_json::from_value(back).unwrap();
        assert_eq!(again, bundle);
    }
}
