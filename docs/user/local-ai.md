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
queue and can be refused when resource pressure is high.

Resource-pressure signals are not yet wired to real GPU/VRAM monitors; the
controller currently always sees a Normal state. Operational pause/resume/
drain controls are not exposed.

The Operations Center shows the currently active model/runtime and basic
health for the configured backend.

Which specific model and runtime this deployment currently points at is an
operator/deployment detail, not part of this document — see your deployment's
own configuration and internal runbooks.
