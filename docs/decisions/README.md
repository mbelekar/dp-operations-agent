# Architecture decisions

Short records of decisions that shaped this codebase: what was chosen, what was considered and rejected, and why. Extracted from the working plan docs in `docs/plans/` (kept locally, not committed), so the reasoning behind the architecture is visible without needing those files.

- [ADR-0001: A single tool-calling loop, not multi-agent orchestration](0001-single-tool-calling-loop.md)
- [ADR-0002: Evidence grounding is enforced in code, not requested in the prompt](0002-deterministic-evidence-grounding.md)
- [ADR-0003: A gateway Protocol between tools and infrastructure, not direct API calls](0003-gateway-abstraction.md)
- [ADR-0004: Diagnose and propose only, no autonomous execution in v1](0004-human-approved-remediation.md)
- [ADR-0005: Deterministic eval grading, not LLM-as-judge, for the first version](0005-deterministic-eval-grading.md)
- [ADR-0006: `Diagnosis.system` is derived from cited evidence, not asserted](0006-diagnosis-system-derived-not-asserted.md)
- [ADR-0007: `submit_diagnosis` requires an explicit `root_cause_signal_id`](0007-root-cause-signal-id.md)
- [ADR-0008: dbt's "previous run" is its `--state` directory, and column shapes come from `catalog.json`](0008-dbt-previous-run-via-state-dir.md)
