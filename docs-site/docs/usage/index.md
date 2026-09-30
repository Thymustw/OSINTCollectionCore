# 使用指南

每個功能的完整用法。還沒跑過系統的話，先看 [快速上手](../getting-started/quickstart.md)。

所有範例都假設你已經設好這兩個環境變數：

```bash
export TOKEN=$(python3 scripts/dev-token.py)
export API=http://127.0.0.1:18080/api/v1
```

| 頁面 | 內容 |
|---|---|
| [取得 API 憑證](authentication.md) | 兩種憑證、三種角色、發放與撤銷長期憑證 |
| [匯入資料](import.md) | 建立資料來源、上傳 JSON／CSV／檔案、欄位對映、自動抓取的來源 |
| [搜尋](search.md) | 關鍵字、片語、布林、過濾條件、中文、語意搜尋 |
| [在圖上看關聯](graph.md) | Neo4j 網頁介面、圖查詢 API、圖上看得到與看不到的東西 |
| [發現新關聯](discovery.md) | 從已知實體找出相關的新目標、審核候選 |
| [STIX 匯入匯出](stix.md) | 與其他威脅情報平台交換資料 |
| [命令列工具](cli.md) | `osint-cli` 本機查詢工具（開發者用） |
