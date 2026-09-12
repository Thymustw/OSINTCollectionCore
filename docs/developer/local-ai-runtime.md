# Local AI Runtime Developer Guide

Authoritative specification:

`../architecture/LOCAL_AI.md`

Current baseline:

```text
Windows + WSL2
Qwen3.8-27B
UD-Q4_K_XL
GGUF
Alias: qwen-primary
```

Preferred benchmark target:

```text
vLLM + vllm-gguf-plugin
```

Fallback/reference:

```text
llama.cpp
```

All Core requests pass through the Rust AI Gateway.

Do not call the inference runtime directly from domain services.

Production selection is benchmark/soak-test driven.
