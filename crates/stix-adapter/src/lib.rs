//! STIX 2.1 型別定義、bundle 結構驗證、與 Core Object 雙向映射（SPEC_V0.2 §18-19）。
//!
//! 手刻型別、不引入外部 STIX crate。映射是純函式庫：不寫 store、
//! 不解析 Relationship 的 Core id（那是 Step 3 worker 的事）。
//!
//! 範圍限定在 SPEC §19 七種映射需要的最小集合：Identity／Threat Actor／
//! Malware／Vulnerability／Indicator／Relationship／Cyber observable（SCO），
//! 加上 `x-` 自訂物件與未知 type 的逃生艙——未知資料保留原始 JSON，
//! 不得為追求 STIX 相容而丟失欄位。

mod error;
mod id;
mod mapping;
mod types;
mod validate;

pub use error::StixError;
pub use id::StixId;
pub use mapping::{
    MappedEntity, MappedRelationship, StixIdentifierFields, entity_to_stix_object,
    relationship_to_stix_object, stix_identifier_fields, stix_object_to_entity,
    stix_relationship_to_core,
};
pub use types::{
    CommonProperties, CustomObject, DomainName, EmailAddr, Identity, IdentityClass, Indicator,
    Ipv4Addr, Malware, Relationship, StixBundle, StixObject, ThreatActor, Url, Vulnerability,
};
pub use validate::validate_bundle;
