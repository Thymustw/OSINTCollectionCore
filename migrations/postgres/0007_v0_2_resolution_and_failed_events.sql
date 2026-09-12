-- V0.2 Phase 0c：Entity Resolution 的四張表 + ADR-008 的 failed_events。
--
-- 對應規格：
--   entity_aliases        SPEC_V0.2 §3
--   entity_identifiers    SPEC_V0.2 §4
--   resolution_candidates SPEC_V0.2 §5（欄位）／§6（method 名稱）
--   merge_history         SPEC_V0.2 §7 + Acceptance C（undo 不遺失歷史）
--   failed_events         ADR-008「V0.2 should implement the full subsystem」
--
-- ⚠️ 這個 migration **只建 schema**。V0.2 Phase 0 沒有任何服務會寫這五張表
--    （resolver / graph-worker / DLQ 重放都還沒做）。跑完看到五張空表是預期結果，
--    不代表 pipeline 壞掉——同 0006 對 SQLite 的處理方式。

-- ---------------------------------------------------------------------------
-- entity_aliases（SPEC §3）
-- ---------------------------------------------------------------------------
-- 「Microsoft」／「Microsoft Corporation」／「微軟」是同一個 Entity 的三個 alias。
-- 與 entity_identifiers 的分工：alias 是人看的名字，**允許多個 Entity 共用**
-- （「Apple」可以是公司也可以是水果）；identifier 才是唯一鍵。
--
-- 因此這裡刻意**沒有** UNIQUE：對 (entity_id, alias) 建唯一鍵看似無害，
-- 實際上會擋掉「同一個別名由兩個不同來源、不同信心度分別觀察到」的正常情形。
CREATE TABLE entity_aliases (
    id              UUID PRIMARY KEY,
    entity_id       UUID NOT NULL REFERENCES entities (id),
    alias           TEXT NOT NULL,
    -- SPEC §3 沒有列舉可用值，維持自由字串（同 jobs.type / events.event_type）。
    alias_type      TEXT NOT NULL,
    -- 可為 NULL：resolver 自己推導出來的 alias（正規化變體、merge 時搬過來的名字）
    -- 沒有單一 Source 可指。設成 NOT NULL 只會逼寫入端塞假值。
    source_id       UUID REFERENCES sources (id),
    confidence      DOUBLE PRECISION NOT NULL,
    first_seen      TIMESTAMPTZ NOT NULL,
    last_seen       TIMESTAMPTZ NOT NULL
);

-- 「這個 Entity 有哪些別名」。id 一起放進索引，讓 ORDER BY id 不必額外排序。
CREATE INDEX idx_entity_aliases_entity ON entity_aliases (entity_id, id);

-- 「這個名字對到哪些 Entity」——SPEC §6 的 `alias` 這條 resolution method。
-- 少了它，alias 比對只能全表掃描。
CREATE INDEX idx_entity_aliases_alias ON entity_aliases (alias);

-- ---------------------------------------------------------------------------
-- entity_identifiers（SPEC §4）
-- ---------------------------------------------------------------------------
-- namespace 的存在理由見 crates/core-model/src/entity_identifier.rs：
-- V0.1 報告 T10（40 位 hex 分不出 SHA-1 與 git commit）的根源是「值本身不帶型別」。
CREATE TABLE entity_identifiers (
    id                  UUID PRIMARY KEY,
    entity_id           UUID NOT NULL REFERENCES entities (id),
    namespace           TEXT NOT NULL,
    -- 原樣看到的字串。
    value               TEXT NOT NULL,
    -- 比對用的正規化形式。正規化由呼叫端負責，理由同 entities.normalized_name：
    -- 把 lower() 放進 SQL 會走不到索引，而且 PG 與 SQLite 的 collation 規則不同。
    normalized_value    TEXT NOT NULL,
    confidence          DOUBLE PRECISION NOT NULL,
    source_id           UUID REFERENCES sources (id),
    first_seen          TIMESTAMPTZ NOT NULL,
    last_seen           TIMESTAMPTZ NOT NULL
);

-- 同一個 namespace 底下，一個識別碼只屬於一個 Entity。這是 SPEC §6
-- 「exact identifier」能當作合併依據的前提。
--
-- ⚠️ **這個唯一鍵會讓「兩個 Entity 宣稱同一個識別碼」變成寫入衝突。**
-- 那不是錯誤處理的邊角，那正是 resolution 要偵測的訊號：寫入端收到
-- StorageError::Conflict 時應該去建一筆 resolution_candidate，而不是吞掉。
-- 吞掉的話識別碼會靜靜地少記一筆，沒有任何跡象。
CREATE UNIQUE INDEX idx_entity_identifiers_natural
    ON entity_identifiers (namespace, normalized_value);

CREATE INDEX idx_entity_identifiers_entity ON entity_identifiers (entity_id, id);

-- ---------------------------------------------------------------------------
-- resolution_candidates（SPEC §5）
-- ---------------------------------------------------------------------------
-- 一列 = 一個 (pair, method)。SPEC §5 寫的是 methods（複數），這裡落地成單數，
-- 理由見 crates/core-model/src/resolution.rs：score 與 status 都只有在
-- 「針對某一種方法」時才講得清楚。這是與 SPEC 字面不同的一處。
CREATE TABLE resolution_candidates (
    id              UUID PRIMARY KEY,
    entity_a_id     UUID NOT NULL REFERENCES entities (id),
    entity_b_id     UUID NOT NULL REFERENCES entities (id),
    score           DOUBLE PRECISION NOT NULL,
    -- SPEC §6 的方法名（10 種，見 core_model::RESOLUTION_METHODS）。
    -- §6 的原文是「至少」，所以不做 CHECK 白名單——新增方法不該需要 migration。
    method          TEXT NOT NULL,
    evidence        JSONB NOT NULL DEFAULT '{}'::jsonb,
    -- pending / confirmed / rejected / auto_confirmed（SPEC §5）。
    status          TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL,
    reviewed_at     TIMESTAMPTZ,

    -- 候選對是**無向**的：(A,B) 與 (B,A) 是同一件事。唯一索引分不出這兩者，
    -- 所以不強制順序的話同一對會存成兩列，而且不會有任何錯誤——只會在
    -- Resolution Review 畫面上看到重複項目，審核者確認一個、拒絕另一個。
    -- 強制 a < b 讓那個狀態在結構上不可能存在。
    -- 呼叫端用 core_model::ResolutionCandidate::ordered_pair 排序。
    CONSTRAINT resolution_candidates_pair_ordered CHECK (entity_a_id < entity_b_id)
);

-- 同一對用同一方法只有一筆 candidate。重跑 resolver 是 upsert 不是長出新列。
CREATE UNIQUE INDEX idx_resolution_candidates_pair_method
    ON resolution_candidates (entity_a_id, entity_b_id, method);

-- Resolution Review 的主要查詢：「還有哪些 pending」。status 是低基數欄位，
-- 帶上 id DESC 才能同時服務排序與 cursor 分頁。
CREATE INDEX idx_resolution_candidates_status
    ON resolution_candidates (status, id DESC);

-- ---------------------------------------------------------------------------
-- merge_history（SPEC §7 / Acceptance C）
-- ---------------------------------------------------------------------------
CREATE TABLE merge_history (
    id              UUID PRIMARY KEY,
    -- 留下來的 canonical Entity。
    survivor_id     UUID NOT NULL REFERENCES entities (id),
    -- 被併掉的 Entity（SPEC §7 的 source entity）。
    --
    -- ⚠️ 刻意**不加**外鍵。merge 實作若選擇刪掉被併掉的那一列，外鍵會讓這筆
    -- 歷史紀錄寫不進去（或被 CASCADE 一起刪掉）——正好把 Acceptance C 要保留的
    -- 東西弄丟。要支援 undo 就不該刪那一列，但那是 resolver 的決定，
    -- schema 這層不替它預設。
    merged_id       UUID NOT NULL,
    reason          TEXT NOT NULL,
    operator        TEXT NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    -- merge 前每個被 repoint 的參照：[{table,row_id,column,previous_value}]。
    -- **undo 的全部依據。** merge 完成後資料庫上已經沒有這個資訊了：事後掃描
    -- 只看得到「這些列現在指向 survivor」，分不出哪些是 merge 改過來的、
    -- 哪些本來就指向 survivor。把後者一起改回去會靜默破壞無關的資料。
    repointed_references JSONB NOT NULL DEFAULT '[]'::jsonb,
    -- 這次 merge 被撤銷的時間。NULL = 仍然生效。
    -- undo **不刪這一列**——刪掉等於「曾經合併過又被拆開」這件事整個消失，
    -- 直接違反 Acceptance C。
    undone_at       TIMESTAMPTZ
);

-- 兩個方向都要查得到：「這個 canonical 吃掉了誰」與「這個 id 被併去哪了」。
-- 後者是 API 收到舊 entity id 時能不能轉導到 survivor 的依據。
CREATE INDEX idx_merge_history_survivor ON merge_history (survivor_id, id DESC);
CREATE INDEX idx_merge_history_merged ON merge_history (merged_id, id DESC);

-- ---------------------------------------------------------------------------
-- failed_events（ADR-008）
-- ---------------------------------------------------------------------------
-- V0.1 對永久失敗的事件只寫 error log 並照常 commit offset（避免一則毒訊息卡死
-- 整個 partition）。ADR-008 把那個缺口記為 T14，並指定 V0.2 用 canonical 表補，
-- 不是開 Redpanda DLQ topic——topic 會過期，過期就是靜默資料遺失。
CREATE TABLE failed_events (
    id              UUID PRIMARY KEY,
    topic           TEXT NOT NULL,
    partition       INTEGER NOT NULL,
    -- `offset` 是 SQL 保留字，必須加引號。Kafka/Redpanda 的 offset 是 64-bit。
    "offset"        BIGINT NOT NULL,
    consumer_group  TEXT NOT NULL,
    -- 要能讓運維判斷「這值不值得重放」。只寫 "processing failed" 會讓
    -- ADR-008 提到的手動復原路徑走不通。
    failure_reason  TEXT NOT NULL,
    attempt_count   INTEGER NOT NULL DEFAULT 1,
    -- 原始 EventEnvelope。重放靠它，不是回頭去 broker 撈（撈不撈得到取決於
    -- retention，而 retention 正是 ADR-008 要避開的坑）。
    envelope        JSONB NOT NULL DEFAULT '{}'::jsonb,
    first_seen      TIMESTAMPTZ NOT NULL,
    last_seen       TIMESTAMPTZ NOT NULL,
    replayed_at     TIMESTAMPTZ
);

-- (topic, partition, offset) 是 broker 上一則訊息的完整座標。同一則事件重試三次
-- 失敗應該是一列 attempt_count=3，不是三列——否則「有幾則事件壞掉」與
-- 「壞掉的事件被試了幾次」會混在一起，DLQ 的筆數就沒有意義了。
CREATE UNIQUE INDEX idx_failed_events_coordinates
    ON failed_events (topic, partition, "offset");

-- 「現在還有哪些沒重放」——Operations Center 的 DLQ 視圖。
CREATE INDEX idx_failed_events_unreplayed
    ON failed_events (id DESC)
    WHERE replayed_at IS NULL;
