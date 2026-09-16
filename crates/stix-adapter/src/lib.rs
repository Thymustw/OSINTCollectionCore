//! STIX 2.1 型別定義與 bundle 結構驗證（SPEC_V0.2 §18-19）。
//!
//! **這一步只做型別與驗證，不做 STIX ↔ Core Object 映射**（映射是 Step 1，
//! 那時才會依賴 `core-model`）。手刻型別、不引入外部 STIX crate。
//!
//! 範圍限定在 SPEC §19 七種映射需要的最小集合：Identity／Threat Actor／
//! Malware／Vulnerability／Indicator／Relationship／Cyber observable（SCO），
//! 加上 `x-` 自訂物件與未知 type 的逃生艙——未知資料保留原始 JSON，
//! 不得為追求 STIX 相容而丟失欄位。

mod error;
mod id;
mod types;
mod validate;

pub use error::StixError;
pub use id::StixId;
pub use types::{
    CommonProperties, CustomObject, DomainName, EmailAddr, Identity, IdentityClass, Indicator,
    Ipv4Addr, Malware, Relationship, StixBundle, StixObject, ThreatActor, Url, Vulnerability,
};
pub use validate::validate_bundle;
