# Local AI Runtime Developer Guide

Authoritative specification:

`../architecture/LOCAL_AI.md`

Deployment shape:

```text
Windows + WSL2
Local generative model (family/quantization/format: deployment-specific)
Alias: qwen-primary
```

The exact model artifact, quantization, and runtime are deployment
configuration, not part of this guide — see `../architecture/LOCAL_AI.md`
§0/§1/§2 for why, and consult your deployment's own operations record for
what is actually running.

Runtime candidates:

```text
vLLM (native, or with a format-specific plugin depending on the deployed
artifact)
llama.cpp
```

All Core requests pass through the Rust AI Gateway.

Do not call the inference runtime directly from domain services.

Production selection is benchmark/soak-test driven.
