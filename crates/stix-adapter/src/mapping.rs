//! STIX 2.1 ↔ Core Object 雙向映射（SPEC_V0.2 §19 七種）。
//!
//! 這一步是純函式庫：不寫 store、不解析 Relationship 的 Core id
//! （`source_ref`／`target_ref` 停在 [`StixId`]，Step 3 worker 才有
//! STIX id → Core id 映射表）。不認識的 STIX type 回 [`None`]，
//! 不是錯誤——bundle 常夾帶本版不處理的物件，呼叫端應跳過該筆。

use std::collections::HashMap;
use std::net::IpAddr;

use serde_json::{Map, Value, json};

use core_model::enums::{EntityType, RelationshipType};
use core_model::{Entity, Relationship as CoreRelationship};

use crate::id::StixId;
use crate::types::{
    CommonProperties, CustomObject, DomainName, EmailAddr, Identity, IdentityClass, Indicator,
    Ipv4Addr, Malware, Relationship as StixRelationship, StixObject, ThreatActor, Url,
    Vulnerability,
};

/// STIX SDO／SCO 轉成 Core Entity 的內容欄位。
///
/// `id` 策略（UUID v5／v7）由呼叫端依既有 upsert 慣例決定，這裡不填。
#[derive(Debug, Clone, PartialEq)]
pub struct MappedEntity {
    pub entity_type: EntityType,
    pub name: String,
    pub normalized_name: String,
    pub description: Option<String>,
    pub attributes: Value,
    pub stix_id: StixId,
}

impl MappedEntity {
    /// 組出用來反查原始 STIX id 的 identifier 欄位（`namespace="stix"`）。
    #[must_use]
    pub fn stix_identifier_fields(&self) -> StixIdentifierFields {
        stix_identifier_fields(&self.stix_id)
    }
}

/// `EntityIdentifier` 裡跟 STIX id 追蹤有關的三個欄位。
///
/// 主鍵／`entity_id`／confidence／時間由呼叫端（Step 3 worker）填。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StixIdentifierFields {
    pub namespace: String,
    pub value: String,
    pub normalized_value: String,
}

/// 組一筆 `namespace="stix"` 的 identifier 欄位。STIX id 本身已是規範格式，
/// `value` 與 `normalized_value` 相同。
#[must_use]
pub fn stix_identifier_fields(stix_id: &StixId) -> StixIdentifierFields {
    let value = stix_id.to_string();
    StixIdentifierFields {
        namespace: "stix".to_string(),
        value: value.clone(),
        normalized_value: value,
    }
}

/// STIX Relationship SRO 轉成 Core Relationship 的映射結果。
///
/// `source_ref`／`target_ref` 停在 [`StixId`]：解析成 `EntityId` 需要
/// 整批 bundle 掃過後的映射表，那是 Step 3 的事。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappedRelationship {
    pub source_ref: StixId,
    pub target_ref: StixId,
    pub relationship_type: RelationshipType,
    /// STIX 原始 `relationship_type` 字串。fallback 到 [`RelationshipType::AssociatedWith`]
    /// 時仍保留原文，語意不丟。
    pub stix_relationship_type: String,
}

/// STIX bundle 裡的一個 SDO／SCO 轉成 Core Entity。
///
/// 不認識的 type（`campaign` 等）、[`StixObject::Relationship`]、
/// [`StixObject::Custom`]、[`StixObject::Unknown`] 回 [`None`]。
/// Custom 的 STIX→Core 還原這一步不做——見模組測試與實作說明。
#[must_use]
pub fn stix_object_to_entity(object: &StixObject) -> Option<MappedEntity> {
    match object {
        StixObject::Identity(identity) => identity_to_entity(identity),
        StixObject::ThreatActor(actor) => Some(mapped(
            EntityType::ThreatActor,
            actor.name.clone(),
            actor.description.clone(),
            merge_attributes(&actor.extra, &[]),
            actor.id.clone(),
        )),
        StixObject::Malware(malware) => Some(malware_to_entity(malware)),
        StixObject::Vulnerability(vuln) => Some(mapped(
            EntityType::Vulnerability,
            vuln.name.clone(),
            vuln.description.clone(),
            merge_attributes(&vuln.extra, &[]),
            vuln.id.clone(),
        )),
        StixObject::Indicator(indicator) => Some(indicator_to_entity(indicator)),
        StixObject::DomainName(sco) => Some(sco_to_entity(
            EntityType::Domain,
            &sco.value,
            &sco.extra,
            sco.id.clone(),
        )),
        StixObject::Ipv4Addr(sco) => Some(sco_to_entity(
            EntityType::Ip,
            &sco.value,
            &sco.extra,
            sco.id.clone(),
        )),
        StixObject::Url(sco) => Some(sco_to_entity(
            EntityType::Url,
            &sco.value,
            &sco.extra,
            sco.id.clone(),
        )),
        StixObject::EmailAddr(sco) => Some(sco_to_entity(
            EntityType::Email,
            &sco.value,
            &sco.extra,
            sco.id.clone(),
        )),
        StixObject::Relationship(_) | StixObject::Custom(_) | StixObject::Unknown(_) => None,
    }
}

/// STIX Relationship SRO → Core 關係映射。未知 `relationship_type` fallback
/// [`RelationshipType::AssociatedWith`]，原始字串放進
/// [`MappedRelationship::stix_relationship_type`]。
#[must_use]
pub fn stix_relationship_to_core(rel: &StixRelationship) -> MappedRelationship {
    MappedRelationship {
        source_ref: rel.source_ref.clone(),
        target_ref: rel.target_ref.clone(),
        relationship_type: map_relationship_type(&rel.relationship_type),
        stix_relationship_type: rel.relationship_type.clone(),
    }
}

/// Core Entity → STIX 物件。
///
/// 對映不到已知 SDO／SCO 的 `EntityType`（Account／Hostname／Repository／
/// Hash／Software／Location）回 [`StixObject::Custom`]（`type` =
/// `x-osint-core-entity`），把 Core 型別與 id 塞進 `extra`，不丟資訊。
#[must_use]
pub fn entity_to_stix_object(entity: &Entity) -> StixObject {
    match entity.entity_type {
        EntityType::Person => {
            StixObject::Identity(identity_from_entity(entity, IdentityClass::Individual))
        }
        EntityType::Organization => {
            StixObject::Identity(identity_from_entity(entity, IdentityClass::Organization))
        }
        EntityType::ThreatActor => StixObject::ThreatActor(ThreatActor {
            id: compose_stix_id("threat-actor", entity.id),
            name: entity.name.clone(),
            description: entity.description.clone(),
            common: common_from_entity(entity),
            extra: extra_from_attributes(&entity.attributes, &[]),
        }),
        EntityType::Malware => StixObject::Malware(malware_from_entity(entity)),
        EntityType::Vulnerability => StixObject::Vulnerability(Vulnerability {
            id: compose_stix_id("vulnerability", entity.id),
            name: entity.name.clone(),
            description: entity.description.clone(),
            common: common_from_entity(entity),
            extra: extra_from_attributes(&entity.attributes, &[]),
        }),
        EntityType::Indicator => StixObject::Indicator(indicator_from_entity(entity)),
        EntityType::Domain => StixObject::DomainName(DomainName {
            id: compose_stix_id("domain-name", entity.id),
            value: entity.name.clone(),
            common: common_from_entity(entity),
            extra: extra_from_attributes(&entity.attributes, &[]),
        }),
        EntityType::Ip => StixObject::Ipv4Addr(Ipv4Addr {
            id: compose_stix_id("ipv4-addr", entity.id),
            value: entity.name.clone(),
            common: common_from_entity(entity),
            extra: extra_from_attributes(&entity.attributes, &[]),
        }),
        EntityType::Url => StixObject::Url(Url {
            id: compose_stix_id("url", entity.id),
            value: entity.name.clone(),
            common: common_from_entity(entity),
            extra: extra_from_attributes(&entity.attributes, &[]),
        }),
        EntityType::Email => StixObject::EmailAddr(EmailAddr {
            id: compose_stix_id("email-addr", entity.id),
            value: entity.name.clone(),
            common: common_from_entity(entity),
            extra: extra_from_attributes(&entity.attributes, &[]),
        }),
        EntityType::Account
        | EntityType::Hostname
        | EntityType::Repository
        | EntityType::Hash
        | EntityType::Software
        | EntityType::Location => StixObject::Custom(custom_from_entity(entity)),
    }
}

/// Core Relationship → STIX SRO。兩端 STIX id 由呼叫端提供
/// （export 時通常是新產生的，不必等於 import 時的原始 id）。
#[must_use]
pub fn relationship_to_stix_object(
    relationship: &CoreRelationship,
    source_stix_id: StixId,
    target_stix_id: StixId,
) -> StixRelationship {
    StixRelationship {
        id: compose_stix_id("relationship", relationship.id),
        relationship_type: relationship_type_to_stix(relationship.relationship_type),
        source_ref: source_stix_id,
        target_ref: target_stix_id,
        description: None,
        common: CommonProperties {
            spec_version: Some("2.1".into()),
            created: Some(relationship.created_at),
            modified: Some(relationship.updated_at),
        },
        extra: HashMap::new(),
    }
}

/// STIX `relationship_type` 字串 → Core [`RelationshipType`]。
/// 有直接對應的用對應值；其餘 fallback [`RelationshipType::AssociatedWith`]。
#[must_use]
fn map_relationship_type(stix_type: &str) -> RelationshipType {
    match stix_type.trim() {
        "indicates" => RelationshipType::Indicates,
        "attributed-to" => RelationshipType::AttributedTo,
        "targets" => RelationshipType::Targets,
        "mitigates" => RelationshipType::Mitigates,
        "uses" => RelationshipType::Uses,
        "located-at" => RelationshipType::LocatedAt,
        "derived-from" => RelationshipType::DerivedFrom,
        "belongs-to" => RelationshipType::BelongsTo,
        "member-of" => RelationshipType::MemberOf,
        _ => RelationshipType::AssociatedWith,
    }
}

fn relationship_type_to_stix(rel_type: RelationshipType) -> String {
    match rel_type {
        RelationshipType::Indicates => "indicates",
        RelationshipType::AttributedTo => "attributed-to",
        RelationshipType::Targets => "targets",
        RelationshipType::Mitigates => "mitigates",
        RelationshipType::Uses => "uses",
        RelationshipType::LocatedAt => "located-at",
        RelationshipType::DerivedFrom => "derived-from",
        RelationshipType::BelongsTo => "belongs-to",
        RelationshipType::MemberOf => "member-of",
        RelationshipType::Mentions => "mentions",
        RelationshipType::References => "references",
        RelationshipType::PublishedBy => "published-by",
        RelationshipType::AuthoredBy => "authored-by",
        RelationshipType::LinksTo => "links-to",
        RelationshipType::Affects => "affects",
        RelationshipType::Owns => "owns",
        // Core 泛型／STIX fallback 匯出成 STIX 慣用的 related-to。
        RelationshipType::AssociatedWith => "related-to",
    }
    .to_string()
}

fn identity_to_entity(identity: &Identity) -> Option<MappedEntity> {
    let entity_type = match identity.identity_class {
        IdentityClass::Individual => EntityType::Person,
        IdentityClass::Organization => EntityType::Organization,
        IdentityClass::Group
        | IdentityClass::Class
        | IdentityClass::System
        | IdentityClass::Unknown
        | IdentityClass::Other(_) => return None,
    };
    Some(mapped(
        entity_type,
        identity.name.clone(),
        identity.description.clone(),
        merge_attributes(&identity.extra, &[]),
        identity.id.clone(),
    ))
}

fn malware_to_entity(malware: &Malware) -> MappedEntity {
    let mut pairs = Vec::new();
    if let Some(is_family) = malware.is_family {
        pairs.push(("x_stix_is_family", json!(is_family)));
    }
    mapped(
        EntityType::Malware,
        malware.name.clone(),
        malware.description.clone(),
        merge_attributes(&malware.extra, &pairs),
        malware.id.clone(),
    )
}

fn indicator_to_entity(indicator: &Indicator) -> MappedEntity {
    let name = indicator
        .name
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| indicator.pattern.clone())
        .unwrap_or_else(|| indicator.id.to_string());
    let mut pairs = Vec::new();
    if let Some(pattern) = &indicator.pattern {
        pairs.push(("x_stix_pattern", json!(pattern)));
    }
    mapped(
        EntityType::Indicator,
        name,
        indicator.description.clone(),
        merge_attributes(&indicator.extra, &pairs),
        indicator.id.clone(),
    )
}

fn sco_to_entity(
    entity_type: EntityType,
    value: &str,
    extra: &HashMap<String, Value>,
    stix_id: StixId,
) -> MappedEntity {
    mapped(
        entity_type,
        value.to_string(),
        None,
        merge_attributes(extra, &[]),
        stix_id,
    )
}

fn mapped(
    entity_type: EntityType,
    name: String,
    description: Option<String>,
    attributes: Value,
    stix_id: StixId,
) -> MappedEntity {
    let normalized_name = normalize_name(entity_type, &name);
    MappedEntity {
        entity_type,
        name,
        normalized_name,
        description,
        attributes,
        stix_id,
    }
}

/// 正規化規則照抄 `entity-worker`／`core-model` 既有慣例，不發明新規則：
///
/// * Person／Organization（以及同為顯示名稱的 ThreatActor／Malware／Indicator）：
///   `entity-worker::extract::normalize_person_or_org`——空白壓成單一半形空格再
///   `to_lowercase`。
/// * Domain：`to_ascii_lowercase` + 去掉尾端 `.`（`extract.rs` `domain_item`）。
/// * Ip：`IpAddr::parse` 成功後用 `Display`（IPv6 會壓縮、hex 轉小寫）；
///   解析失敗則保留原文，不丟這筆。
/// * Url：`core_model::url_norm::canonicalize`（與 documents.canonical_url 同一套）。
///   解析不出來時保留原文——STIX import 不該因為非 http URL 就沒有 `normalized_name`。
/// * Email：`to_ascii_lowercase` + 去掉尾端 `.`（`extract_emails`）。
/// * Vulnerability：`to_ascii_uppercase`（`extract_cves`）。
fn normalize_name(entity_type: EntityType, name: &str) -> String {
    match entity_type {
        EntityType::Person
        | EntityType::Organization
        | EntityType::ThreatActor
        | EntityType::Malware
        | EntityType::Indicator
        | EntityType::Account
        | EntityType::Hostname
        | EntityType::Software
        | EntityType::Repository
        | EntityType::Location => normalize_person_or_org(name),
        EntityType::Domain => name.to_ascii_lowercase().trim_end_matches('.').to_string(),
        EntityType::Ip => name
            .parse::<IpAddr>()
            .map(|ip| ip.to_string())
            .unwrap_or_else(|_| name.to_string()),
        EntityType::Url => {
            core_model::url_norm::canonicalize(name).unwrap_or_else(|| name.to_string())
        }
        EntityType::Email => name.to_ascii_lowercase().trim_end_matches('.').to_string(),
        EntityType::Vulnerability => name.to_ascii_uppercase(),
        EntityType::Hash => name.to_ascii_lowercase(),
    }
}

/// 抄自 `crates/entity-worker/src/extract.rs` 的 `normalize_person_or_org`。
fn normalize_person_or_org(raw: &str) -> String {
    raw.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase()
}

fn identity_from_entity(entity: &Entity, identity_class: IdentityClass) -> Identity {
    Identity {
        id: compose_stix_id("identity", entity.id),
        identity_class,
        name: entity.name.clone(),
        description: entity.description.clone(),
        common: common_from_entity(entity),
        extra: extra_from_attributes(&entity.attributes, &[]),
    }
}

fn malware_from_entity(entity: &Entity) -> Malware {
    let is_family = entity
        .attributes
        .get("x_stix_is_family")
        .and_then(Value::as_bool);
    Malware {
        id: compose_stix_id("malware", entity.id),
        name: entity.name.clone(),
        description: entity.description.clone(),
        is_family,
        common: common_from_entity(entity),
        extra: extra_from_attributes(&entity.attributes, &["x_stix_is_family"]),
    }
}

fn indicator_from_entity(entity: &Entity) -> Indicator {
    let pattern = entity
        .attributes
        .get("x_stix_pattern")
        .and_then(Value::as_str)
        .map(str::to_string);
    Indicator {
        id: compose_stix_id("indicator", entity.id),
        name: Some(entity.name.clone()),
        pattern,
        description: entity.description.clone(),
        common: common_from_entity(entity),
        extra: extra_from_attributes(&entity.attributes, &["x_stix_pattern"]),
    }
}

fn custom_from_entity(entity: &Entity) -> CustomObject {
    let mut extra = extra_from_attributes(
        &entity.attributes,
        &[
            "x_osint_core_type",
            "x_osint_core_id",
            "name",
            "description",
        ],
    );
    extra.insert(
        "x_osint_core_type".into(),
        json!(entity_type_wire(entity.entity_type)),
    );
    extra.insert("x_osint_core_id".into(), json!(entity.id.to_string()));
    extra.insert("name".into(), json!(entity.name));
    if let Some(desc) = &entity.description {
        extra.insert("description".into(), json!(desc));
    }
    CustomObject {
        type_: "x-osint-core-entity".into(),
        id: compose_stix_id("x-osint-core-entity", entity.id),
        extra,
    }
}

fn common_from_entity(entity: &Entity) -> CommonProperties {
    CommonProperties {
        spec_version: Some("2.1".into()),
        created: Some(entity.first_seen),
        modified: Some(entity.last_seen),
    }
}

fn compose_stix_id(type_prefix: &str, uuid: uuid::Uuid) -> StixId {
    StixId::parse(format!("{type_prefix}--{uuid}"))
        .unwrap_or_else(|err| panic!("內部組出的 STIX id 不該失敗（prefix={type_prefix}）：{err}"))
}

fn entity_type_wire(entity_type: EntityType) -> &'static str {
    match entity_type {
        EntityType::Person => "person",
        EntityType::Organization => "organization",
        EntityType::Account => "account",
        EntityType::Domain => "domain",
        EntityType::Hostname => "hostname",
        EntityType::Ip => "ip",
        EntityType::Url => "url",
        EntityType::Email => "email",
        EntityType::Vulnerability => "vulnerability",
        EntityType::Software => "software",
        EntityType::Repository => "repository",
        EntityType::Hash => "hash",
        EntityType::Location => "location",
        EntityType::ThreatActor => "threat_actor",
        EntityType::Malware => "malware",
        EntityType::Indicator => "indicator",
    }
}

fn merge_attributes(extra: &HashMap<String, Value>, pairs: &[(&str, Value)]) -> Value {
    let mut map = Map::new();
    for (k, v) in extra {
        map.insert(k.clone(), v.clone());
    }
    for (k, v) in pairs {
        map.insert((*k).to_string(), v.clone());
    }
    Value::Object(map)
}

fn extra_from_attributes(attributes: &Value, skip: &[&str]) -> HashMap<String, Value> {
    let mut extra = HashMap::new();
    let Some(obj) = attributes.as_object() else {
        return extra;
    };
    for (k, v) in obj {
        if skip.contains(&k.as_str()) {
            continue;
        }
        extra.insert(k.clone(), v.clone());
    }
    extra
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};
    use core_model::ids::EntityId;
    use serde_json::json;
    use uuid::Uuid;

    const UUID: &str = "2152fbe0-4471-4d43-8b64-0b907d186c23";
    const ENTITY_UUID: &str = "01993c6a-7c3e-7a11-8000-7c3e7a110001";

    fn ts() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 9, 17, 12, 0, 0).unwrap()
    }

    fn entity_id() -> EntityId {
        Uuid::parse_str(ENTITY_UUID).unwrap()
    }

    fn stix_id(prefix: &str) -> StixId {
        StixId::parse(format!("{prefix}--{UUID}")).unwrap()
    }

    fn sample_entity(
        entity_type: EntityType,
        name: &str,
        description: Option<&str>,
        attributes: Value,
    ) -> Entity {
        Entity {
            id: entity_id(),
            entity_type,
            name: name.into(),
            normalized_name: normalize_name(entity_type, name),
            description: description.map(str::to_string),
            confidence: 1.0,
            first_seen: ts(),
            last_seen: ts(),
            merged_into: None,
            attributes,
        }
    }

    fn parse_object(raw: Value) -> StixObject {
        serde_json::from_value(raw).expect("合法 STIX JSON")
    }

    fn round_trip_core_fields(entity_type: EntityType, name: &str, description: Option<&str>) {
        let entity = sample_entity(entity_type, name, description, json!({}));
        let stix = entity_to_stix_object(&entity);
        let mapped = stix_object_to_entity(&stix)
            .unwrap_or_else(|| panic!("{entity_type:?} 應能 STIX→Core"));
        assert_eq!(mapped.entity_type, entity_type);
        assert_eq!(mapped.name, name);
        assert_eq!(mapped.description.as_deref(), description);
        assert_eq!(mapped.normalized_name, entity.normalized_name);
    }

    #[test]
    fn identity_individual_maps_to_person() {
        let obj = parse_object(json!({
            "type": "identity",
            "id": format!("identity--{UUID}"),
            "name": "  Alice  Bob ",
            "identity_class": "individual",
            "description": "分析師"
        }));
        let mapped = stix_object_to_entity(&obj).expect("Person");
        assert_eq!(mapped.entity_type, EntityType::Person);
        assert_eq!(mapped.name, "  Alice  Bob ");
        assert_eq!(mapped.normalized_name, "alice bob");
        assert_eq!(mapped.description.as_deref(), Some("分析師"));
        assert_eq!(mapped.stix_id, stix_id("identity"));
    }

    #[test]
    fn identity_organization_maps_to_organization() {
        let obj = parse_object(json!({
            "type": "identity",
            "id": format!("identity--{UUID}"),
            "name": "Acme Corp",
            "identity_class": "organization"
        }));
        let mapped = stix_object_to_entity(&obj).expect("Organization");
        assert_eq!(mapped.entity_type, EntityType::Organization);
        assert_eq!(mapped.normalized_name, "acme corp");
    }

    #[test]
    fn identity_group_is_skipped() {
        let obj = parse_object(json!({
            "type": "identity",
            "id": format!("identity--{UUID}"),
            "name": "Anon",
            "identity_class": "group"
        }));
        assert!(stix_object_to_entity(&obj).is_none());
    }

    #[test]
    fn sco_uses_value_not_name() {
        let cases = [
            (
                json!({"type": "domain-name", "id": format!("domain-name--{UUID}"), "value": "Example.COM."}),
                EntityType::Domain,
                "Example.COM.",
                "example.com",
            ),
            (
                json!({"type": "ipv4-addr", "id": format!("ipv4-addr--{UUID}"), "value": "192.0.2.1"}),
                EntityType::Ip,
                "192.0.2.1",
                "192.0.2.1",
            ),
            (
                json!({"type": "url", "id": format!("url--{UUID}"), "value": "https://Example.com/a?utm_source=x"}),
                EntityType::Url,
                "https://Example.com/a?utm_source=x",
                "https://example.com/a",
            ),
            (
                json!({"type": "email-addr", "id": format!("email-addr--{UUID}"), "value": "Bob@Example.com."}),
                EntityType::Email,
                "Bob@Example.com.",
                "bob@example.com",
            ),
        ];
        for (raw, entity_type, name, normalized) in cases {
            let obj = parse_object(raw);
            let mapped = stix_object_to_entity(&obj).expect("SCO");
            assert_eq!(mapped.entity_type, entity_type);
            assert_eq!(mapped.name, name, "{entity_type:?} 應取 SCO value");
            assert_eq!(mapped.normalized_name, normalized);
            assert!(mapped.description.is_none());
        }
    }

    #[test]
    fn malware_is_family_lands_in_attributes() {
        let obj = parse_object(json!({
            "type": "malware",
            "id": format!("malware--{UUID}"),
            "name": "Emotet",
            "is_family": true,
            "x_note": "keep"
        }));
        let mapped = stix_object_to_entity(&obj).expect("Malware");
        assert_eq!(mapped.entity_type, EntityType::Malware);
        assert_eq!(mapped.attributes["x_stix_is_family"], true);
        assert_eq!(mapped.attributes["x_note"], "keep");
    }

    #[test]
    fn indicator_pattern_lands_in_attributes_and_fills_empty_name() {
        let obj = parse_object(json!({
            "type": "indicator",
            "id": format!("indicator--{UUID}"),
            "pattern": "[domain-name:value = 'evil.example']"
        }));
        let mapped = stix_object_to_entity(&obj).expect("Indicator");
        assert_eq!(mapped.entity_type, EntityType::Indicator);
        assert_eq!(mapped.name, "[domain-name:value = 'evil.example']");
        assert_eq!(
            mapped.attributes["x_stix_pattern"],
            "[domain-name:value = 'evil.example']"
        );
    }

    #[test]
    fn vulnerability_normalizes_to_uppercase() {
        let obj = parse_object(json!({
            "type": "vulnerability",
            "id": format!("vulnerability--{UUID}"),
            "name": "cve-2024-0001",
            "description": "測試"
        }));
        let mapped = stix_object_to_entity(&obj).expect("Vulnerability");
        assert_eq!(mapped.normalized_name, "CVE-2024-0001");
    }

    #[test]
    fn unknown_and_relationship_and_custom_return_none() {
        let campaign = parse_object(json!({
            "type": "campaign",
            "id": format!("campaign--{UUID}"),
            "name": "Op-X"
        }));
        assert!(stix_object_to_entity(&campaign).is_none());

        let rel = parse_object(json!({
            "type": "relationship",
            "id": format!("relationship--{UUID}"),
            "relationship_type": "indicates",
            "source_ref": format!("indicator--{UUID}"),
            "target_ref": format!("malware--{UUID}")
        }));
        assert!(stix_object_to_entity(&rel).is_none());

        let custom = parse_object(json!({
            "type": "x-osint-core-entity",
            "id": format!("x-osint-core-entity--{UUID}"),
            "x_osint_core_type": "account"
        }));
        assert!(
            stix_object_to_entity(&custom).is_none(),
            "Custom 的 STIX→Core 這一步不做"
        );
    }

    #[test]
    fn map_relationship_type_known_and_fallback() {
        let known = [
            ("indicates", RelationshipType::Indicates),
            ("attributed-to", RelationshipType::AttributedTo),
            ("targets", RelationshipType::Targets),
            ("mitigates", RelationshipType::Mitigates),
            ("uses", RelationshipType::Uses),
            ("located-at", RelationshipType::LocatedAt),
            ("derived-from", RelationshipType::DerivedFrom),
            ("belongs-to", RelationshipType::BelongsTo),
            ("member-of", RelationshipType::MemberOf),
        ];
        for (stix, core) in known {
            let rel = StixRelationship {
                id: stix_id("relationship"),
                relationship_type: stix.into(),
                source_ref: stix_id("indicator"),
                target_ref: stix_id("malware"),
                description: None,
                common: CommonProperties::default(),
                extra: HashMap::new(),
            };
            let mapped = stix_relationship_to_core(&rel);
            assert_eq!(mapped.relationship_type, core, "{stix}");
            assert_eq!(mapped.stix_relationship_type, stix);
            assert_eq!(mapped.source_ref, rel.source_ref);
            assert_eq!(mapped.target_ref, rel.target_ref);
        }

        for unknown in ["based-on", "exploits", "variant-of"] {
            let rel = StixRelationship {
                id: stix_id("relationship"),
                relationship_type: unknown.into(),
                source_ref: stix_id("malware"),
                target_ref: stix_id("malware"),
                description: None,
                common: CommonProperties::default(),
                extra: HashMap::new(),
            };
            let mapped = stix_relationship_to_core(&rel);
            assert_eq!(
                mapped.relationship_type,
                RelationshipType::AssociatedWith,
                "{unknown} 應 fallback"
            );
            assert_eq!(mapped.stix_relationship_type, unknown);
        }
    }

    #[test]
    fn core_to_stix_known_types() {
        let cases = [
            (EntityType::Person, "identity", Some("Alice")),
            (EntityType::Organization, "identity", Some("Acme")),
            (EntityType::ThreatActor, "threat-actor", Some("APT-X")),
            (EntityType::Malware, "malware", Some("Family")),
            (
                EntityType::Vulnerability,
                "vulnerability",
                Some("CVE-2024-0001"),
            ),
            (EntityType::Indicator, "indicator", Some("bad domain")),
            (EntityType::Domain, "domain-name", Some("example.com")),
            (EntityType::Ip, "ipv4-addr", Some("192.0.2.1")),
            (EntityType::Url, "url", Some("https://example.com/a")),
            (EntityType::Email, "email-addr", Some("a@example.com")),
        ];
        for (entity_type, type_name, name) in cases {
            let entity = sample_entity(entity_type, name.unwrap(), Some("d"), json!({}));
            let stix = entity_to_stix_object(&entity);
            assert_eq!(stix.type_name(), type_name, "{entity_type:?}");
            match entity_type {
                EntityType::Person => match &stix {
                    StixObject::Identity(id) => {
                        assert_eq!(id.identity_class, IdentityClass::Individual);
                        assert_eq!(id.name, "Alice");
                    }
                    other => panic!("預期 Identity，得到 {other:?}"),
                },
                EntityType::Organization => match &stix {
                    StixObject::Identity(id) => {
                        assert_eq!(id.identity_class, IdentityClass::Organization);
                    }
                    other => panic!("預期 Identity，得到 {other:?}"),
                },
                EntityType::Domain => match &stix {
                    StixObject::DomainName(sco) => assert_eq!(sco.value, "example.com"),
                    other => panic!("預期 DomainName，得到 {other:?}"),
                },
                EntityType::Ip => match &stix {
                    StixObject::Ipv4Addr(sco) => assert_eq!(sco.value, "192.0.2.1"),
                    other => panic!("預期 Ipv4Addr，得到 {other:?}"),
                },
                EntityType::Url => match &stix {
                    StixObject::Url(sco) => assert_eq!(sco.value, "https://example.com/a"),
                    other => panic!("預期 Url，得到 {other:?}"),
                },
                EntityType::Email => match &stix {
                    StixObject::EmailAddr(sco) => assert_eq!(sco.value, "a@example.com"),
                    other => panic!("預期 EmailAddr，得到 {other:?}"),
                },
                _ => {}
            }
        }
    }

    #[test]
    fn round_trip_preserves_type_name_description() {
        round_trip_core_fields(EntityType::Person, "Alice", Some("分析師"));
        round_trip_core_fields(EntityType::Organization, "Acme", Some("廠商"));
        round_trip_core_fields(EntityType::ThreatActor, "APT-X", Some("d"));
        round_trip_core_fields(EntityType::Malware, "Emotet", None);
        round_trip_core_fields(EntityType::Vulnerability, "CVE-2024-0001", Some("cve"));
        round_trip_core_fields(EntityType::Indicator, "bad domain", Some("ioc"));
        round_trip_core_fields(EntityType::Domain, "example.com", None);
        round_trip_core_fields(EntityType::Ip, "192.0.2.1", None);
        round_trip_core_fields(EntityType::Url, "https://example.com/a", None);
        round_trip_core_fields(EntityType::Email, "a@example.com", None);
    }

    #[test]
    fn malware_is_family_round_trips_through_attributes() {
        let entity = sample_entity(
            EntityType::Malware,
            "Emotet",
            None,
            json!({"x_stix_is_family": true}),
        );
        let stix = entity_to_stix_object(&entity);
        match &stix {
            StixObject::Malware(m) => assert_eq!(m.is_family, Some(true)),
            other => panic!("預期 Malware，得到 {other:?}"),
        }
        let mapped = stix_object_to_entity(&stix).expect("Malware");
        assert_eq!(mapped.attributes["x_stix_is_family"], true);
    }

    #[test]
    fn unmapped_entity_type_becomes_custom_object() {
        let entity = sample_entity(
            EntityType::Account,
            "github:alice",
            Some("帳號"),
            json!({"platform": "github"}),
        );
        let stix = entity_to_stix_object(&entity);
        match &stix {
            StixObject::Custom(custom) => {
                assert_eq!(custom.type_, "x-osint-core-entity");
                assert_eq!(
                    custom.extra.get("x_osint_core_type"),
                    Some(&json!("account"))
                );
                assert_eq!(
                    custom.extra.get("x_osint_core_id"),
                    Some(&json!(ENTITY_UUID))
                );
                assert_eq!(custom.extra.get("name"), Some(&json!("github:alice")));
                assert_eq!(custom.extra.get("description"), Some(&json!("帳號")));
                assert_eq!(custom.extra.get("platform"), Some(&json!("github")));
                assert_eq!(custom.id.type_prefix(), "x-osint-core-entity");
            }
            other => panic!("預期 Custom，得到 {other:?}"),
        }
        assert!(
            stix_object_to_entity(&stix).is_none(),
            "Custom 反向這一步不做"
        );
    }

    #[test]
    fn relationship_core_round_trip_type() {
        let core = CoreRelationship {
            id: Uuid::parse_str(ENTITY_UUID).unwrap(),
            source_object_id: entity_id(),
            relationship_type: RelationshipType::Indicates,
            target_object_id: entity_id(),
            confidence: 0.9,
            first_seen: ts(),
            last_seen: ts(),
            evidence_count: 1,
            created_at: ts(),
            updated_at: ts(),
        };
        let stix = relationship_to_stix_object(&core, stix_id("indicator"), stix_id("malware"));
        assert_eq!(stix.relationship_type, "indicates");
        assert_eq!(stix.source_ref, stix_id("indicator"));
        assert_eq!(stix.target_ref, stix_id("malware"));
        let mapped = stix_relationship_to_core(&stix);
        assert_eq!(mapped.relationship_type, RelationshipType::Indicates);
        assert_eq!(mapped.stix_relationship_type, "indicates");
    }

    #[test]
    fn stix_identifier_fields_use_stix_namespace() {
        let id = stix_id("identity");
        let fields = stix_identifier_fields(&id);
        assert_eq!(fields.namespace, "stix");
        assert_eq!(fields.value, format!("identity--{UUID}"));
        assert_eq!(fields.normalized_value, fields.value);
    }
}
