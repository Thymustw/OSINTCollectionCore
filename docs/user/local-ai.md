# AI Gateway

Core includes an AI Gateway that talks to a configured language model over an
OpenAI-compatible API. The Gateway does not hardcode which model or inference
runtime is behind that endpoint — point it at whatever local or remote
OpenAI-compatible backend you run, and Core works against the same contract.

AI requests go through an admission controller rather than being sent to the
backend unbounded, so a slow or saturated model does not stall the rest of
the platform. Interactive resolve calls (P0) use a small reserved slot and
fail immediately if those slots are full — they do not wait in a queue.
Background work (currently STIX import auto-approval, P3) shares a separate
queue and can be refused when the gateway observes high pressure (recent
latency, HTTP 429 / timeout / 5xx rates, or a long admission queue).
Interactive P0 calls ignore that pressure signal on purpose, so a busy
background workload cannot take down the path a user is waiting on.

Pressure thresholds live in `[ai_gateway.pressure]` and are speculative —
they have not been capacity-tested. With too little traffic the state stays
Normal. This is HTTP-side observation, not a host CPU / RAM / GPU monitor.
Operational pause/resume/drain controls are not exposed.

Collection-scoped AI budgets (`daily_ai_budget`, `max_ai_calls_per_run`) are
enforced at the gateway for Collection accounts. Current production callers
(auto-approval) use a System account and do not consume collection quota.
`max_requests_per_run` is not wired: no Discovery method issues outbound HTTP
yet. That field is not unlimited; it is unused.

The Operations Center shows the currently active model/runtime and basic
health for the configured backend.

Which specific model and runtime this deployment currently points at is an
operator/deployment detail, not part of this document — see your deployment's
own configuration and internal runbooks.
