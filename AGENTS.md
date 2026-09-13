# Working on this SDK

This is a thin, unofficial Elixir SDK for Betradar's Unified Odds Feed (UOF).
It owns feed consumption, decoding, producer health, and recovery orchestration.
Applications own business logic, message effects, and their persistence.
Keep new features within that scope unless the task explicitly expands it.

## Start here

- Read the [architecture guide](docs/architecture.md) before changing transport,
  supervision, message processing, or recovery behavior. It describes component
  responsibilities, process boundaries, and design invariants.
- Use the [README](README.md) for public configuration, handler contracts,
  persistence requirements, and integration-test setup.
- Update those documents when a change alters the behavior they describe.

## Implementation conventions

- Prefer idiomatic Elixir: pattern matching, explicit contracts, small functions,
  and OTP supervision. Let invalid internal states and programming errors fail
  visibly; handle expected transport failures through the appropriate backoff.
- Catch failures only where there is a concrete recovery or cleanup responsibility.
  Preserve the original cause. Cleanup must respect shared resource ownership.
- Keep state transitions in their owning component. Follow the architecture guide
  when deciding whether a change belongs in a pipeline, transport, or monitor.
- Reuse `uof_api` for HTTP API access and `uof_schemas` for XML decoding and feed
  structs. Preserve the ability to install only the chosen transport dependency.
- Inspect the installed dependency source before relying on adapter behavior.
  Document why private APIs or unusual lifecycle mechanisms are necessary, and
  include compatibility validation when changing those dependencies.
- Prefer a focused change over speculative abstractions or configuration options.

## Validate UOF behavior against upstream

Use UOF protocol/API documentation for feed requirements. Use the official SDK
implementations and tests to understand reference behavior and edge cases:

- [UOF documentation](https://docs.sportradar.com/uof)
- [Session behavior](https://docs.sportradar.com/uof/sdk/features/session)
- [Recovery behavior](https://docs.sportradar.com/uof/sdk/features/recovery)
- [Official Java SDK](https://github.com/sportradar/UnifiedOddsSdkJava)
- [Official .NET SDK](https://github.com/sportradar/UnifiedOddsSdkNetCore)

For changes to protocol interpretation, producer health, sessions, or recovery,
verify the relevant documentation and consult upstream code or tests where the
behavior is unclear. Cite the documentation or exact upstream revision in the PR.
Distinguish protocol requirements from upstream implementation choices. If sources
disagree, describe the discrepancy and the evidence supporting the chosen behavior.

Preserve this project's documented scope and intentional differences. Upstream
feature availability does not require feature parity, and Java/.NET process or
object structures should not dictate the Elixir design.

## Validation

Use the toolchain declared in `mise.toml`. The standard checks are:

```sh
mix deps.get
MIX_ENV=test mix compile --warnings-as-errors
mix format --check-formatted
mix test
```

For transport or lifecycle changes, also run `make test-integration`. It uses
Docker for RabbitMQ and Pulsar and builds the RabbitMQ source connector when
needed; see the README and Makefile for prerequisites and overrides.

Add regression coverage for changed behavior. Lifecycle tests should exercise
real process exits, ownership, and reconnection where relevant. Use monitors and
explicit synchronization rather than arbitrary sleeps. Set test retry intervals
so production backoff cannot exceed assertion deadlines.

Run checks appropriate to the change; documentation-only changes do not require
the runtime suites. Report what was tested and any checks that were not run.

## Code Review Rules

- Prioritize concrete correctness risks in supported configurations. Explain the
  trigger, affected behavior, and consequence; distinguish verified failures from
  hypotheses. Consult dependency source when a finding depends on its semantics.
- Check the architecture guide's invariants, particularly shared AMQP ownership,
  active-only Pulsar recovery, persisted recovery intent, and session generations.
- Check recovery completion against the active producer/request and treat stale
  observations explicitly. Feed synchronization, application processing, and
  transport acknowledgement are distinct boundaries.
- Do not request generic error catches, extra retries, or forced process termination
  without showing how they preserve the relevant failure and ownership contracts.
- Keep mechanical formatting checks in tooling. Avoid turning internal contracts
  into general extension APIs merely to accommodate hypothetical callers.
