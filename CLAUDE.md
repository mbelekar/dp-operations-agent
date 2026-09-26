# Development Workflow

## Planning before implementation

For any non-trivial task (new feature, multi-file change, refactor):

1. Do not write or edit code immediately. First explore the relevant parts of
   the codebase to understand current behavior and constraints.
2. Ask clarifying questions if requirements are ambiguous - one at a time,
   not as a giant list - before proposing a design.
3. Produce a written plan that includes:
   - The problem being solved and the chosen approach (briefly note
     alternatives considered, if any exist)
   - A numbered list of concrete tasks, each small enough to be described
     completely on its own, with a clear definition of done
   - Files/modules each task touches
   - Any risks, edge cases, or assumptions
4. Save the plan to `docs/plans/<short-name>.md` and show it to me.
5. Stop after presenting the plan. Do not implement anything until I
   explicitly approve it (e.g. "approved", "go ahead", "implement this").

For trivial changes (single-line fixes, typo corrections, config tweaks),
skip planning and just make the change - but say so explicitly
("this is small enough to skip the plan step").

## Simplicity

Prefer the smallest implementation that completely solves the approved problem.

- Do not add features, abstractions, configuration, or extension points that
  are not required by the current task.
- Do not add defensive handling for scenarios that cannot occur under the
  system's documented invariants.
- If a simpler approach exists, explain it before proposing a more elaborate one.
- Push back when a request would introduce unnecessary complexity or conflict
  with an existing architectural decision.
- State assumptions explicitly. If several reasonable interpretations exist,
present them before choosing one. Do not silently resolve ambiguity.
- Before finishing, check whether the implementation can be made materially
  smaller without weakening correctness, readability, or testability.

## Keeping changes surgical

Every changed line should be directly connected to the approved task.

- Do not refactor, reformat, rename, or clean up adjacent code unless the
  approved plan requires it.
- Match the style and patterns already used in the affected module.
- If the change makes an import, variable, function, test, or file obsolete,
  remove that newly orphaned code.
- Do not remove pre-existing dead code or fix unrelated issues. Record them
  separately for possible follow-up.
- Keep unrelated changes out of the same commit.

## Executing an approved plan

- Work through the plan's tasks in order.
- After each task, briefly report what changed and run relevant tests or
  checks before moving to the next task.
- If you discover the plan was wrong or incomplete once you're implementing,
  stop and tell me rather than silently improvising a different approach.
- Don't expand scope beyond what the plan describes. If you notice something
  else worth fixing, note it at the end instead of doing it unprompted.

## Project-specific constraints

- Use `./auto/build` for a frozen dependency installation.
- Before completing a code change, run:
  - `./auto/lint`
  - `./auto/test --cov`
- Do not run live-model evaluations unless explicitly requested. They make
  billed API calls.
- Preserve the model-facing signal JSON. Changes require updating the typed
  signal models and intentional review of the JSON snapshot tests.
- Keep gateway I/O asynchronous. Do not introduce blocking network or file
  operations directly inside `async` methods.
- Diagnostic tools are read-only. Do not add state-changing tools without a
  new approved architecture decision and an independently enforced permission
  boundary.
- The model may choose evidence and proposals, but facts that code can derive
  or validate must remain enforced outside the model.
- Record significant architectural changes as ADRs under `docs/decisions/`.

## Debugging

When investigating a bug, follow this order and do not skip ahead:

1. **Reproduce it first.** Confirm you can trigger the actual failure before
   changing anything.
2. **Find the root cause**, not the first symptom. Trace the failure back
   through the code path to where it actually originates.
3. **Check for the same pattern elsewhere.** If this root cause could affect
   other similar code paths, check them before fixing just the one you found.
4. **State your hypothesis explicitly** before writing a fix - what you
   believe is wrong and why - and verify it (e.g. with a log statement,
   a minimal repro, or a targeted test) before changing code.
5. **Only then implement the fix**, and re-run the original repro to confirm
   it's actually resolved.

Do not guess-and-check by making speculative changes and re-running to see
if they help. If you're not sure of the root cause after step 2, say so and
explain what you'd need to check next, rather than trying a fix anyway.

## Before declaring anything done

Run the test suite (or the relevant subset) and/or the specific
reproduction steps for a bug fix. Do not say a task is complete based on
the code "looking correct" - show the verification you actually ran.