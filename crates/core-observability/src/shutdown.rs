//! 行程停止訊號。
//!
//! Docker／k8s 送的是 SIGTERM；本機 Ctrl-C 是 SIGINT。PID 1 若沒有 handler，
//! kernel 不會做預設終止，Docker 會等到 grace 後 SIGKILL（exit 137）。
//!
//! 這個 helper 只負責「等到訊號並記下是哪一個」。何時 cancel、drain、abort
//! 由各服務自己決定。目前只有 collector 接上；其他服務是另案。

/// 讓行程結束的訊號。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShutdownCause {
    /// SIGINT（Ctrl-C）。
    Interrupt,
    /// SIGTERM（`docker stop`／k8s）。
    Terminate,
}

/// 等到行程該停的訊號。
///
/// Unix：SIGINT 或 SIGTERM 任一。非 Unix：`ctrl_c()`。
/// 收到時打 info，內容含訊號名稱，方便對 log 核對停止原因。
///
/// **測試不要對 cargo test 行程送 SIGTERM／SIGINT**：同一行程的其他測試會一起被殺。
pub async fn shutdown_signal() -> ShutdownCause {
    #[cfg(unix)]
    {
        unix_shutdown_signal().await
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("收到 SIGINT");
        ShutdownCause::Interrupt
    }
}

#[cfg(unix)]
async fn unix_shutdown_signal() -> ShutdownCause {
    use tokio::signal::unix::{SignalKind, signal};

    let mut sigterm = signal(SignalKind::terminate()).unwrap_or_else(|err| {
        panic!("註冊 SIGTERM handler 失敗：{err}。沒有 handler 時 PID 1 收不到預設終止，docker stop 會變成 SIGKILL")
    });
    let mut sigint = signal(SignalKind::interrupt()).unwrap_or_else(|err| {
        panic!("註冊 SIGINT handler 失敗：{err}。本機 Ctrl-C 將無法結束行程")
    });
    tokio::select! {
        _ = sigterm.recv() => {
            tracing::info!("收到 SIGTERM");
            ShutdownCause::Terminate
        }
        _ = sigint.recv() => {
            tracing::info!("收到 SIGINT");
            ShutdownCause::Interrupt
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn shutdown_signal_does_not_complete_without_a_signal() {
        // 不可對測試行程送 SIGTERM：cargo test 同行程的其他測試會被誤殺。
        let completed = tokio::time::timeout(Duration::from_millis(80), shutdown_signal()).await;
        assert!(
            completed.is_err(),
            "沒有訊號時 shutdown_signal 必須繼續等，80ms 內不該完成"
        );
    }
}
