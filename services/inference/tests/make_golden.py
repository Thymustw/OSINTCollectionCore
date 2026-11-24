"""產生 tests/golden.json：同一組句子打 OpenSearch ml-commons 已部署的 e5-small-int8，
存下向量，給 test_golden.py 比對本服務的輸出。

只在換模型、換 pooling 或懷疑兩邊不一致時重跑；結果 commit 進 repo（句子都是合成的）。
用法（repo 根目錄）：
  OPENSEARCH_URL=http://127.0.0.1:19200 python3 -I services/inference/tests/make_golden.py
"""

import json
import os
import sys
import urllib.request
from pathlib import Path

MODEL_NAME = "intfloat/multilingual-e5-small-int8"

# 合成句子：中英混雜、全形、IoC、URL、emoji、簡體、日文、長句。都加 passage:／query: 前綴，
# 因為呼叫端會加，前綴本身也要對得上。
SENTENCES = [
    "passage: Fortinet FortiOS 存在高風險漏洞 CVE-2026-1234，影響 7.2.0 至 7.2.8 版。",
    "passage: 研究人員指出，攻擊者透過 https://evil.example.com/a?b=1&c=2 散布惡意程式。",
    "passage: 勒索軟體 LockBit 3.0 於 2026 年第三季活動下降 30%。",
    "passage: 雜湊值 SHA-256: e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
    "passage: The threat actor used spear-phishing emails with malicious .docx attachments.",
    "passage: 全形字元測試：ＡＰＴ４１、ＣＶＥ－２０２６－０００１、１２３４５。",
    "passage: Emoji 測試 🔥🚨 攻擊警報 ⚠️ 請注意",
    "passage: 简体中文混合测试：漏洞利用与恶意软件。",
    "passage: 日本語の混在テスト：脆弱性情報データベース（JVN）。",
    "query: APT41 最近利用哪些漏洞攻擊台灣半導體業？",
    "query: Which ransomware groups targeted healthcare in 2026?",
    "passage: " + "外交部發言人表示，雙方將持續就區域安全議題保持溝通，並在多邊架構下推動合作。" * 6,
]


def post(base: str, path: str, body: dict) -> dict:
    req = urllib.request.Request(
        base + path, data=json.dumps(body).encode(), headers={"content-type": "application/json"}
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.load(r)


def main() -> None:
    base = os.environ["OPENSEARCH_URL"].rstrip("/")
    hits = post(
        base,
        "/_plugins/_ml/models/_search",
        {
            "query": {
                "bool": {
                    "must": [{"term": {"name.keyword": MODEL_NAME}}, {"term": {"model_state": "DEPLOYED"}}],
                    "must_not": {"exists": {"field": "chunk_number"}},
                }
            },
            "size": 1,
        },
    )["hits"]["hits"]
    if not hits:
        sys.exit(f"ml-commons 沒有已部署的 {MODEL_NAME}")
    model_id = hits[0]["_id"]
    out = []
    for text in SENTENCES:
        r = post(
            base,
            f"/_plugins/_ml/_predict/text_embedding/{model_id}",
            {"text_docs": [text], "return_number": True, "target_response": ["sentence_embedding"]},
        )
        vec = r["inference_results"][0]["output"][0]["data"]
        out.append({"text": text, "embedding": [round(x, 7) for x in vec]})
    path = Path(__file__).with_name("golden.json")
    path.write_text(json.dumps({"model": MODEL_NAME, "items": out}, ensure_ascii=False, indent=1) + "\n")
    print(f"寫入 {path}，{len(out)} 筆")


if __name__ == "__main__":
    main()
