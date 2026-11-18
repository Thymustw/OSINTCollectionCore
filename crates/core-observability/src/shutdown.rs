//! 行程停止訊號。
//!
//! Docker／k8s 送的是 SIGTERM；本機 Ctrl-C 是 SIGINT。PID 1 若沒有 handler，
//! kernel 不會做預設終止，Docker 會等到 grace 後 SIGKILL（exit 137）。
//!
//! 這個 helper 只負責「等到訊號並記下是哪一個」。何時 cancel、drain、abort
//! 由各服務自己決定。collector 用 [`shutdown_signal`] + CancellationToken；
//! 其餘 9 個服務（2026-11-18 起）用 [`shutdown_watch`]。
//!
//! # 為什麼迴圈裡不能直接 `select! { _ = shutdown_signal() => … }`
//!
//! 每次呼叫 `shutdown_signal()` 都新建一個 `signal()` stream，只收得到**建立之後**的訊號。
//! 放在 `loop { select! { … } }` 裡時，處理一則事件（或 indexer 送一批）的期間沒有任何
//! stream 存在，這段時間到的 SIGTERM 下一圈新建的 stream 看不到，行程就一路等到 SIGKILL。
//! [`shutdown_watch`] 只註冊一次，結果存在 watch channel 裡，任何時候查都看得到。

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

/// 註冊 SIGTERM／SIGINT 一次，回傳「是否該停了」的 watch。
///
/// handler 在**這個函式回傳前**就註冊好（同步），所以之後任何時刻送來的訊號都不會漏。
/// 收到訊號後值變成 `true` 並維持；迴圈用 `changed()` 等、或用 `*borrow()` 查。
/// 必須在 Tokio runtime 內呼叫。
///
/// ```ignore
/// let mut stop = shutdown_watch();
/// loop {
///     tokio::select! {
///         _ = stop.changed() => break,
///         r = consumer.next_envelope(t) => { /* 處理一則 */ }
///     }
/// }
/// ```
#[must_use]
pub fn shutdown_watch() -> tokio::sync::watch::Receiver<bool> {
    let (tx, rx) = tokio::sync::watch::channel(false);
    #[cfg(unix)]
    {
        let (mut sigterm, mut sigint) = register_unix_signals();
        tokio::spawn(async move {
            let _cause = wait_unix(&mut sigterm, &mut sigint).await;
            let _ = tx.send(true);
        });
    }
    #[cfg(not(unix))]
    {
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            tracing::info!("收到 SIGINT");
            let _ = tx.send(true);
        });
    }
    rx
}

#[cfg(unix)]
fn register_unix_signals() -> (tokio::signal::unix::Signal, tokio::signal::unix::Signal) {
    use tokio::signal::unix::{SignalKind, signal};

    let sigterm = signal(SignalKind::terminate()).unwrap_or_else(|err| {
        panic!("註冊 SIGTERM handler 失敗：{err}。沒有 handler 時 PID 1 收不到預設終止，docker stop 會變成 SIGKILL")
    });
    let sigint = signal(SignalKind::interrupt()).unwrap_or_else(|err| {
        panic!("註冊 SIGINT handler 失敗：{err}。本機 Ctrl-C 將無法結束行程")
    });
    (sigterm, sigint)
}

#[cfg(unix)]
async fn unix_shutdown_signal() -> ShutdownCause {
    let (mut sigterm, mut sigint) = register_unix_signals();
    wait_unix(&mut sigterm, &mut sigint).await
}

#[cfg(unix)]
async fn wait_unix(
    sigterm: &mut tokio::signal::unix::Signal,
    sigint: &mut tokio::signal::unix::Signal,
) -> ShutdownCause {
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

    #[tokio::test]
    async fn shutdown_watch_stays_false_without_a_signal() {
        let mut stop = shutdown_watch();
        assert!(!*stop.borrow(), "沒有訊號時初始值必須是 false");
        let changed = tokio::time::timeout(Duration::from_millis(80), stop.changed()).await;
        assert!(changed.is_err(), "沒有訊號時 watch 不該變動");
    }
}
