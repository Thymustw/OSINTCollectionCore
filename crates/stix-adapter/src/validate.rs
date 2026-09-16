//! Bundle 結構驗證。不做 STIX ↔ Core 語意映射（那是 Step 1）。

use serde_json::Value;

use crate::error::StixError;
use crate::id::StixId;

/// 結構驗證，不做語意映射。
///
/// 檢查：根是 object；`type == "bundle"`；`id` 是合法 [`StixId`] 且
/// `type_prefix() == "bundle"`；`objects` 是陣列且長度 `<= max_objects`；
/// 每個物件是 object、有非空 `type`、有合法格式的 `id`。
///
/// 錯誤訊息會指出是哪個欄位、第幾個物件出問題。
pub fn validate_bundle(raw: &Value, max_objects: usize) -> Result<(), StixError> {
    let obj = raw.as_object().ok_or_else(|| StixError::InvalidBundle {
        message: "根節點必須是 JSON object，不能是陣列或純量".into(),
    })?;

    match obj.get("type") {
        None => {
            return Err(StixError::InvalidBundle {
                message: "缺少 `type` 欄位，必須是 `\"bundle\"`".into(),
            });
        }
        Some(Value::String(t)) if t == "bundle" => {}
        Some(other) => {
            return Err(StixError::InvalidBundle {
                message: format!(
                    "`type` 必須是 `\"bundle\"`，實際是 {other}。\
                     這支函式只接受 STIX bundle，單一 SDO 請包進 bundle 再送"
                ),
            });
        }
    }

    match obj.get("id") {
        None => {
            return Err(StixError::InvalidBundle {
                message: "缺少 `id` 欄位，必須是 `bundle--<uuid>`".into(),
            });
        }
        Some(Value::String(id)) => {
            let parsed = StixId::parse(id)?;
            if parsed.type_prefix() != "bundle" {
                return Err(StixError::InvalidId {
                    id: id.clone(),
                    message: format!(
                        "bundle 的 id 前綴必須是 `bundle`，實際是 `{}`",
                        parsed.type_prefix()
                    ),
                });
            }
        }
        Some(other) => {
            return Err(StixError::InvalidBundle {
                message: format!("`id` 必須是字串，實際是 {other}"),
            });
        }
    }

    let objects = match obj.get("objects") {
        None => {
            return Err(StixError::InvalidBundle {
                message: "缺少 `objects` 欄位，必須是物件陣列（可為空）".into(),
            });
        }
        Some(Value::Array(items)) => items,
        Some(other) => {
            return Err(StixError::InvalidBundle {
                message: format!("`objects` 必須是陣列，實際是 {other}"),
            });
        }
    };

    if objects.len() > max_objects {
        return Err(StixError::TooManyObjects {
            actual: objects.len(),
            limit: max_objects,
        });
    }

    for (index, item) in objects.iter().enumerate() {
        validate_object(index, item)?;
    }
    Ok(())
}

fn validate_object(index: usize, item: &Value) -> Result<(), StixError> {
    let obj = item.as_object().ok_or_else(|| StixError::InvalidObject {
        index,
        message: "必須是 JSON object，不能是陣列或純量".into(),
    })?;

    match obj.get("type") {
        None => {
            return Err(StixError::InvalidObject {
                index,
                message: "缺少 `type` 欄位".into(),
            });
        }
        Some(Value::String(t)) if !t.is_empty() => {}
        Some(other) => {
            return Err(StixError::InvalidObject {
                index,
                message: format!("`type` 必須是非空字串，實際是 {other}"),
            });
        }
    }

    match obj.get("id") {
        None => {
            return Err(StixError::InvalidObject {
                index,
                message: "缺少 `id` 欄位，必須是 `type--<uuid>`".into(),
            });
        }
        Some(Value::String(id)) => {
            StixId::parse(id).map_err(|err| match err {
                StixError::InvalidId { id, message } => StixError::InvalidObject {
                    index,
                    message: format!("id `{id}` {message}"),
                },
                other => other,
            })?;
        }
        Some(other) => {
            return Err(StixError::InvalidObject {
                index,
                message: format!("`id` 必須是字串，實際是 {other}"),
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const UUID: &str = "2152fbe0-4471-4d43-8b64-0b907d186c23";

    fn ok_bundle() -> Value {
        json!({
            "type": "bundle",
            "id": format!("bundle--{UUID}"),
            "objects": [
                {
                    "type": "identity",
                    "id": format!("identity--{UUID}"),
                    "name": "Alice",
                    "identity_class": "individual"
                }
            ]
        })
    }

    #[test]
    fn valid_bundle_passes() {
        validate_bundle(&ok_bundle(), 10).expect("合法 bundle");
    }

    #[test]
    fn empty_objects_passes() {
        let raw = json!({
            "type": "bundle",
            "id": format!("bundle--{UUID}"),
            "objects": []
        });
        validate_bundle(&raw, 0).expect("空陣列合法");
    }

    #[test]
    fn rejects_non_object_root() {
        let err = validate_bundle(&json!([]), 1).expect_err("陣列");
        match err {
            StixError::InvalidBundle { message } => {
                assert!(message.contains("JSON object"), "{message}");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn rejects_wrong_type() {
        let mut raw = ok_bundle();
        raw["type"] = json!("identity");
        let err = validate_bundle(&raw, 10).expect_err("type");
        match err {
            StixError::InvalidBundle { message } => {
                assert!(message.contains("bundle"), "{message}");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn rejects_missing_id() {
        let mut raw = ok_bundle();
        raw.as_object_mut().unwrap().remove("id");
        let err = validate_bundle(&raw, 10).expect_err("缺 id");
        match err {
            StixError::InvalidBundle { message } => {
                assert!(message.contains("id"), "{message}");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn rejects_non_bundle_id_prefix() {
        let mut raw = ok_bundle();
        raw["id"] = json!(format!("identity--{UUID}"));
        let err = validate_bundle(&raw, 10).expect_err("前綴");
        match err {
            StixError::InvalidId { message, .. } => {
                assert!(message.contains("bundle"), "{message}");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn rejects_missing_objects() {
        let mut raw = ok_bundle();
        raw.as_object_mut().unwrap().remove("objects");
        let err = validate_bundle(&raw, 10).expect_err("缺 objects");
        match err {
            StixError::InvalidBundle { message } => {
                assert!(message.contains("objects"), "{message}");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn rejects_object_missing_type() {
        let raw = json!({
            "type": "bundle",
            "id": format!("bundle--{UUID}"),
            "objects": [{ "id": format!("identity--{UUID}") }]
        });
        let err = validate_bundle(&raw, 10).expect_err("缺 type");
        match err {
            StixError::InvalidObject { index, message } => {
                assert_eq!(index, 0);
                assert!(message.contains("type"), "{message}");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn rejects_object_missing_id() {
        let raw = json!({
            "type": "bundle",
            "id": format!("bundle--{UUID}"),
            "objects": [{ "type": "identity" }]
        });
        let err = validate_bundle(&raw, 10).expect_err("缺 id");
        match err {
            StixError::InvalidObject { index, message } => {
                assert_eq!(index, 0);
                assert!(message.contains("id"), "{message}");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn rejects_object_invalid_id() {
        let raw = json!({
            "type": "bundle",
            "id": format!("bundle--{UUID}"),
            "objects": [{ "type": "identity", "id": "not-an-id" }]
        });
        let err = validate_bundle(&raw, 10).expect_err("壞 id");
        match err {
            StixError::InvalidObject { index, message } => {
                assert_eq!(index, 0);
                assert!(message.contains("id"), "{message}");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn rejects_too_many_objects() {
        let err = validate_bundle(&ok_bundle(), 0).expect_err("超量");
        match err {
            StixError::TooManyObjects { actual, limit } => {
                assert_eq!(actual, 1);
                assert_eq!(limit, 0);
            }
            other => panic!("{other:?}"),
        }
    }
}
