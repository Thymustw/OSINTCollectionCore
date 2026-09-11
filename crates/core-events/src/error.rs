//! Broker／envelope 錯誤。

/// 事件系統錯誤。
#[derive(Debug, thiserror::Error)]
pub enum EventError {
    #[error("事件 JSON 編碼失敗：{0}")]
    Encode(#[from] serde_json::Error),
    #[error(
        "Redpanda producer 建立失敗：{message}。請確認 broker 位址（例如 127.0.0.1:9092）與容器 osint-core-redpanda-1 在跑"
    )]
    ProducerCreate { message: String },
    #[error("Redpanda consumer 建立失敗：{message}。請確認 broker 位址與 consumer group")]
    ConsumerCreate { message: String },
    #[error(
        "送出事件到 topic `{topic}` 失敗：{message}。請確認 Redpanda 在跑，且 topic 可自動建立"
    )]
    Produce { topic: String, message: String },
    #[error("從 topic `{topic}` 讀取逾時（{timeout_ms} ms）。請確認有人 produce，或把逾時調大")]
    ConsumeTimeout { topic: String, timeout_ms: u64 },
    #[error("消費到的訊息不是合法 EventEnvelope：{message}")]
    InvalidEnvelope { message: String },
    #[error("broker 設定不正確：{message}")]
    Configuration { message: String },
    #[error("提交 consumer offset 失敗：{message}。請確認 Redpanda 在跑，或稍後重試")]
    Commit { message: String },
}
