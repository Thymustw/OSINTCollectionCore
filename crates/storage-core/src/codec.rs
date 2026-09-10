//! 列舉與 JSON 欄位的共用編解碼。SQL 本身仍留在各 adapter。

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::error::StorageError;

/// 把 serde 列舉編成資料庫使用的 snake_case 字串。
pub fn encode_enum<T: Serialize>(value: &T) -> Result<String, StorageError> {
    match serde_json::to_value(value) {
        Ok(Value::String(s)) => Ok(s),
        Ok(other) => Err(StorageError::CorruptionSuspected {
            message: format!("列舉編碼後不是字串：{other}"),
        }),
        Err(err) => Err(StorageError::Unknown {
            backend: "codec",
            message: err.to_string(),
        }),
    }
}

/// 從資料庫字串解回列舉。
pub fn decode_enum<T: DeserializeOwned>(raw: &str, field: &str) -> Result<T, StorageError> {
    serde_json::from_value(Value::String(raw.to_string())).map_err(|err| {
        StorageError::CorruptionSuspected {
            message: format!(
                "欄位 `{field}` 的值 `{raw}` 不是合法列舉（{err}）。請檢查寫入端或 migration"
            ),
        }
    })
}

/// 把 JSON 字串解成 `Value`。空字串當空物件。
pub fn decode_json(raw: &str, field: &str) -> Result<Value, StorageError> {
    if raw.is_empty() {
        return Ok(Value::Object(serde_json::Map::new()));
    }
    serde_json::from_str(raw).map_err(|err| StorageError::CorruptionSuspected {
        message: format!("欄位 `{field}` 不是合法 JSON：{err}"),
    })
}

/// 把 JSON 字串解成 `Vec<String>`。
pub fn decode_string_vec(raw: &str, field: &str) -> Result<Vec<String>, StorageError> {
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    serde_json::from_str(raw).map_err(|err| StorageError::CorruptionSuspected {
        message: format!("欄位 `{field}` 不是字串陣列 JSON：{err}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use core_model::SourceType;

    #[test]
    fn source_type_round_trip() {
        let encoded = encode_enum(&SourceType::Rss).unwrap();
        assert_eq!(encoded, "rss");
        let back: SourceType = decode_enum(&encoded, "source_type").unwrap();
        assert_eq!(back, SourceType::Rss);
    }
}
