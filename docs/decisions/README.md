# Architecture decisions

These records explain the choices that shaped the project, the alternatives considered, and the resulting trade-offs.

| ADR | Decision |
| --- | --- |
| [0001](0001-single-tool-calling-loop.md) | Use one tool-calling agent instead of multiple agents |
| [0002](0002-deterministic-evidence-grounding.md) | Enforce evidence grounding in code |
| [0003](0003-gateway-abstraction.md) | Place gateway protocols between tools and infrastructure |
| [0004](0004-human-approved-remediation.md) | Require human approval before remediation execution |
| [0005](0005-deterministic-eval-grading.md) | Start with deterministic evaluation grading |
| [0006](0006-diagnosis-system-derived-not-asserted.md) | Derive the diagnosed system from cited evidence |
| [0007](0007-root-cause-signal-id.md) | Require an explicit root-cause signal ID |
| [0008](0008-dbt-previous-run-via-state-dir.md) | Use dbt state artifacts for previous-run comparison |
| [0009](0009-no-data-is-unknown-not-ok.md) | Report missing data as `unknown`, not `ok` |
| [0010](0010-proposals-model-chosen-code-checked.md) | Let the model choose proposals and code check them |
| [0011](0011-record-approvals-defer-execution.md) | Record approval decisions, defer execution |
| [0012](0012-typed-signal-payloads.md) | Type signal payloads without changing their JSON |

The ADRs were extracted from working plans so the architectural reasoning remains visible in the repository.
