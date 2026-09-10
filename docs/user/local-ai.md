# AI Gateway

Core includes an AI Gateway that talks to a configured language model over an
OpenAI-compatible API. The Gateway does not hardcode which model or inference
runtime is behind that endpoint — point it at whatever local or remote
OpenAI-compatible backend you run, and Core works against the same contract.

AI requests are priority-queued and admission-controlled rather than sent to
the backend unbounded, so a slow or saturated model does not stall the rest of
the platform.

The Operations Center shows the currently active model/runtime and basic
health for the configured backend.

Which specific model and runtime this deployment currently points at is an
operator/deployment detail, not part of this document — see your deployment's
own configuration and internal runbooks.
