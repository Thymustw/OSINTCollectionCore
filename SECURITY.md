# Security Policy

This project follows the DevSecOps lifecycle defined in
[`docs/governance/DEVSECOPS.md`](docs/governance/DEVSECOPS.md) and the threat
model in [`docs/security/SECURITY_THREAT_MODEL.md`](docs/security/SECURITY_THREAT_MODEL.md).
This file exists so external reporters (this is a public repository) have a
clear channel — it does not define a separate policy from those documents.

## Reporting a Vulnerability

Please report suspected security issues privately via **GitHub Security
Advisories** ("Report a vulnerability" under the Security tab of this
repository) rather than filing a public issue. Do not open a public issue or
pull request describing an unpatched vulnerability.

Include, where applicable:

- affected component (connector, storage adapter, API endpoint, etc.)
- reproduction steps or a minimal proof of concept
- impact you believe it has (e.g. SSRF, authorization bypass, data exposure)

## Response Targets

Once a report is triaged and confirmed, remediation follows the same targets
defined in `docs/governance/DEVSECOPS.md` §14:

| Severity | Target |
|---|---|
| Critical | immediate triage; remediate within 7 days |
| High | remediate within 30 days |
| Medium | remediate within 90 days |
| Low | risk-based maintenance |

An exception to these targets requires the documented finding/severity/component/
exploitability/compensating-controls/owner/expiry record described in
`DEVSECOPS.md` — it is never silently missed.

## Scope

This repository is in active pre-V1.0 development (see `docs/specs/`). Known
architectural risk areas are documented deliberately in
`docs/security/SECURITY_THREAT_MODEL.md` and `docs/security/CONNECTOR_SECURITY.md`
rather than hidden — a gap you find there that isn't yet mitigated in code is
still worth reporting if you found a concrete exploitable instance of it.
