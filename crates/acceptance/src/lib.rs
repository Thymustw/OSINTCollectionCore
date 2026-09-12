//! 跨服務驗收測試的容器 crate。**這裡沒有生產程式碼。**
//!
//! # 為什麼另開一個 crate，而不是塞進 deduplicator／entity-worker 的 tests/
//!
//! 這裡的測試同時用到 collector、normalizer、deduplicator、entity-worker、
//! indexer 與 core-api。放進任何一個服務的 `tests/` 都會造成兩個問題：
//!
//! 1. **相依方向被測試帶壞**：deduplicator 的 dev-dependencies 會多出 core-api，
//!    也就是一個 pipeline stage 在測試相依上指向 API 層。之後有人要拆 crate
//!    時看到的相依圖就不是真的了。
//! 2. **沒有明確的家**：「驗收測試」不屬於任何單一服務。放在誰的 tests/ 裡
//!    都只是因為那個服務剛好先寫到，下一個人要找 Acceptance F 在哪會找很久。
//!
//! 服務**自己的** e2e 仍然留在各自的 `tests/e2e.rs`（那裡驗的是單一服務的契約）。
//! 這個 crate 只放**需要多個服務一起才成立**的斷言。
//!
//! # 檔案
//!
//! * `tests/acceptance_f.rs` — SPEC §26 Acceptance F：consumer crash 後重送事件，
//!   不產生重複的 canonical object（normalizer／deduplicator／entity-worker 三段）。
