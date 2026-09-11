//! 匯入解析錯誤。
//!
//! 訊息是給上傳者看的：只講「哪一筆／哪個欄位／超過哪個上限／下一步怎麼做」，
//! 不夾帶檔案路徑、bucket 名稱、SQL 或任何內部結構。

/// JSON／CSV 匯入解析失敗。
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ImportError {
    #[error("上傳內容是空的。請確認 file 欄位真的帶了檔案內容")]
    Empty,

    #[error(
        "JSON 巢狀深度超過上限 {limit} 層。請把資料攤平成「物件陣列」或 NDJSON 再上傳；\
         過深的結構會被視為攻擊輸入而拒絕"
    )]
    TooDeep { limit: usize },

    #[error(
        "第 {line} 行不是合法 JSON 物件。JSON 匯入只接受「物件陣列」（`[{{…}}, {{…}}]`）\
         或 NDJSON（每行一個物件）"
    )]
    NotAnObject { line: usize },

    #[error("第 {line} 行 JSON 解析失敗：{detail}。請用 JSON 驗證工具確認該行格式")]
    MalformedJson { line: usize, detail: String },

    #[error(
        "筆數 {count} 超過單次匯入上限 {limit}。請把檔案拆成多份上傳，\
         或請管理者調高 config [import].max_records"
    )]
    TooManyRecords { count: usize, limit: usize },

    #[error(
        "第 {index} 筆資料是 {size} bytes，超過單筆上限 {limit}。\
         請縮短該筆內容，或請管理者調高 config [import].max_record_bytes"
    )]
    RecordTooLarge {
        index: usize,
        size: usize,
        limit: usize,
    },

    #[error(
        "第 {index} 筆的 `{field}` 是 {size} bytes，超過單一欄位上限 {limit}。\
         請縮短該欄位，或請管理者調高 config [import].max_field_bytes"
    )]
    FieldTooLarge {
        index: usize,
        field: &'static str,
        size: usize,
        limit: usize,
    },

    #[error(
        "CSV 欄位數 {count} 超過上限 {limit}。請確認第一列是 header 而不是資料，\
         或請管理者調高 config [import].max_columns"
    )]
    TooManyColumns { count: usize, limit: usize },

    #[error("CSV 沒有 header 列。CSV 匯入以第一列為欄位名稱，請補上 header 再上傳")]
    MissingHeader,

    #[error(
        "CSV 第 {line} 行解析失敗：{detail}。常見原因是引號沒有成對，或該行欄位數與 header 不同"
    )]
    MalformedCsv { line: usize, detail: String },

    #[error(
        "對映設定指到不存在的欄位 `{column}`（可用的 header：{available}）。\
         請修正 mapping，或改用 header 原本的名稱"
    )]
    UnknownColumn { column: String, available: String },

    #[error(
        "整份檔案沒有任何可用資料（{total} 筆都缺 title／body／summary）。\
         請確認 mapping 對到正確的欄位名稱"
    )]
    NothingUsable { total: usize },
}
