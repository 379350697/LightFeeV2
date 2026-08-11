# Root-Cause Repair Protocol

This protocol applies to every bug fix, regression fix, compatibility repair,
production incident remediation, and review follow-up in LightFeeV2. It exists
to prevent symptom-level patches, false-green tests, parallel implementations,
and repeated fix-review-fix loops.

The protocol is mandatory. A fix is not complete because the reported example
passes. It is complete only when the affected contract, its production path,
its counterexample family, and its state transitions have been closed together.

## 1. Non-Negotiable Principles

1. Fix the earliest shared broken contract, not the latest visible symptom.
2. Define the intended invariant before editing implementation code.
3. Trace the real production path from producer to terminal state or side effect.
4. Treat all equivalent branches as one bug family and audit them together.
5. Add a failing production-path regression before, or alongside, the fix.
6. Test complementary outcomes: block and release, create and clear, retry and
   terminal, success and evidence failure.
7. A green helper/unit test does not prove the real adapter/runtime path.
8. Review is an independent closure gate, not an incremental bug-discovery loop.
9. Unknown, malformed, stale, partial, or failed evidence must not be converted
   into a safe value unless the governing contract explicitly allows it.
10. Never claim "root fixed", "final review", or "all green" without recording
    the evidence required by this protocol.
11. Prefer the smallest implementation that fully enforces the contract. Broad
    analysis does not justify broad code.
12. Do not add abstraction, configurability, compatibility layers, helpers, or
    files for hypothetical future use.

## 2. Phase A - Freeze and Reproduce the Baseline

Before changing code:

- Record `HEAD`, branch, tracked modifications, and untracked paths.
- Preserve unrelated user changes in a dirty worktree.
- Capture the exact symptom, inputs, state, output, and timestamped evidence.
- For production incidents, gather evidence read-only; exchange truth outranks
  recovered local state.
- Reproduce the failure with the smallest real-path test or deterministic probe.
- If reproduction is impossible, state what is missing and do not describe an
  unverified hypothesis as a confirmed root cause.

Required output:

```text
Symptom:
Observed input/state:
Observed output/state transition:
Expected contract:
Reproduction evidence:
```

## 3. Phase B - Define the Contract and Bug Family

Write the root-cause statement before implementation:

```text
When <condition/input/state>, <shared component> violates <invariant> because
<mechanism>. This affects <all callers/processes/parallel branches>. Existing
tests missed it because <coverage or fixture gap>.
```

Then identify:

- Authoritative behavior: V1 semantics, exchange documentation, protocol/schema,
  or an explicitly approved V2 deviation.
- Contract owner: the single module or boundary that should normalize and decide.
- Producers: adapters, parsers, persistence, journal replay, diagnostics, APIs.
- Consumers: decision cores, ledgers, lifecycle transitions, order flows, health
  checks, and operators.
- Parallel implementations: duplicated predicates, copied constants, alternate
  APIs, legacy helpers, and test-only substitutes.
- Terminal outcomes: success, retry, block, clear, evidence gap, operator stop,
  and unrecoverable failure.

Do not begin with a one-line patch when the same business fact is interpreted in
multiple places. First choose one owner and plan how every consumer will use it.
Choosing one owner does not require a new framework: prefer an existing module,
type, constant, or transition function when it already owns the concept.

## 4. Phase C - Impact and Path Analysis

Before modifying a function, class, or method, follow the repository GitNexus
rules and report direct callers, affected processes, and risk.

Also perform working-tree analysis:

- A GitNexus index matching `HEAD` does not include unstaged code changes.
- Use local diff and repository search to inspect every changed or duplicated
  condition in the working tree.
- Trace the full runtime path, not only the target symbol:

```text
external/raw input
  -> parser/adapter
  -> normalization
  -> domain model
  -> decision/ownership logic
  -> state transition/side effect
  -> persistence/journal/diagnostics
```

- Search for all representations of the same rule: enum members, string
  literals, reason sets, boolean predicates, lifecycle checks, and copied lists.
- If a new method duplicates an existing production capability, wire or repair
  the existing production interface instead of creating a parallel path.

## 5. Phase D - Build the Counterexample Matrix Before the Fix

Create a matrix appropriate to the contract. At minimum consider:

| Dimension | Required counterexamples when applicable |
|---|---|
| Presence | missing, `None`, empty, populated |
| Numeric | zero, positive, negative, boundary epsilon, `NaN`, infinity, invalid string |
| Evidence | complete, partial, stale, unavailable, endpoint error, malformed response |
| Scope | item, symbol, venue, account, global |
| Ownership | owned, orphaned, conflicting owner, unknown owner |
| Lifecycle | clean start, blocked, stale blocked, retrying, terminal, operator fail-closed |
| Collection | one row, many rows, mixed success/error, duplicate, unknown row shape |
| Venue/schema | canonical field, every supported alias, missing identity, unsupported variant |
| Time | before boundary, at boundary, after boundary, expired/replayed evidence |

Every release/clear rule must have complementary tests:

- the evidence that must keep the latch;
- the evidence that is sufficient to release the latch;
- incomplete evidence that must not release it;
- a previously unscanned or alternate branch that must still be detected.

Classify existing tests before changing them:

- valid contract test;
- obsolete behavior test;
- test that accidentally preserves the bug;
- helper-only or mocked-path test that does not exercise production.

Changing an existing expected result requires authoritative contract evidence.
Never change a test merely to make the new implementation green.

Keep the matrix concise: parameterize equivalent cases and avoid duplicate tests
that prove the same branch without adding a distinct contract boundary.

## 6. Phase E - Implement at the Shared Boundary

Implementation rules:

- Repair the earliest boundary that has enough information to enforce the
  invariant.
- Normalize external shapes once into an explicit domain contract. Consumers
  should not reinterpret raw dictionaries independently.
- Reuse shared enums, sets, predicates, and state-transition functions. Do not
  copy a subset into a local literal or `if` chain.
- Remove or route around obsolete parallel logic when safe; do not leave two
  authoritative paths.
- Keep fail-closed behavior for trading state when evidence is unknown.
- Preserve stronger evidence when mixed with weaker/error evidence.
- Do not hide parser or adapter failures by synthesizing empty, zero, flat, or
  successful values.
- Keep the patch within the declared bug family. If the required scope expands,
  update the root-cause statement, impact analysis, and matrix before editing
  the newly discovered area.
- Minimize code and concepts: prefer changing one existing owner over adding a
  wrapper, service, strategy, registry, or generalized framework.
- Add a helper or type only when it removes duplicated authority, makes an
  invalid state unrepresentable, or serves multiple current production callers.
- Do not add extension points, flags, fallbacks, or compatibility behavior that
  no current requirement or caller needs.
- Remove newly obsolete branches and duplicated logic when safe instead of
  keeping both old and new paths.
- Comments should explain non-obvious contracts or safety reasons, not restate
  straightforward code.

## 7. Phase F - Layered Verification

Verification must proceed in layers:

1. RED proof: demonstrate the original failure before the fix, when practical.
2. GREEN proof: the exact reproduction passes after the fix.
3. Contract matrix: all applicable counterexamples pass.
4. Production-path integration: real domain objects and the actual called API
   path are exercised; do not monkeypatch the method whose wiring is under test.
5. Adjacent regression: callers and sibling branches identified by impact
   analysis pass.
6. Repository validation profile: use
   `docs/testing-validation-strategy.md` and the matching profile.
7. Static gates: compile/import checks, lint where applicable, and
   `git diff --check`.
8. Full suite: required before merge/release unless an explicit exception is
   recorded. An interrupted or timed-out suite is not a pass.
9. Production read-only proof: required when the original symptom involved live
   state, deployment behavior, or exchange responses.

Test counts are evidence only when the command, scope, exit status, and any
excluded or interrupted tests are stated.

## 8. Phase G - Independent Closure Review

The closure review must be read-only. It reviews the contract and working tree
from first principles, not the implementer's narrative.

The reviewer must:

- restate the invariant independently;
- inspect the complete diff and all changed tests;
- trace every producer, consumer, state transition, and clear/release path;
- search for copied literals, duplicated predicates, legacy bypasses, and
  alternate APIs;
- challenge the patch with new matrix counterexamples;
- verify tests use real production objects and call paths;
- check that mixed success/error evidence cannot erase stronger live evidence;
- compare compatibility-sensitive semantics with V1;
- distinguish pre-existing issues from regressions introduced by the patch;
- report P0/P1 findings before merge and avoid editing during the review.

A review that only reruns the implementer's tests is not an independent review.

## 9. Repair-Loop Circuit Breaker

A root-cause reset is mandatory when any of these occurs:

- a P0/P1 is found after the fix was declared complete;
- the same bug family fails two fix-review cycles;
- a review discovers another copied condition or release path;
- an existing test has been changed more than once to follow the implementation;
- the patch keeps growing into new modules without an updated contract map;
- targeted tests stay green while a production-path or broader suite is red.

When triggered:

1. Stop adding local patches.
2. Mark the fix as not closed.
3. Preserve the current working tree and summarize every failed attempt.
4. Rebuild the root-cause statement, contract owner, complete path map, and
   counterexample matrix from the current baseline.
5. Identify and remove the duplicated authority that allowed each miss.
6. Implement one cohesive remediation and rerun all verification layers.

After a circuit breaker, another isolated `if`, field alias, or test expectation
change is prohibited unless the renewed contract analysis proves it is the
shared-boundary fix.

## 10. Completion Gate

A fix may be called complete only when every applicable item is true:

- [ ] Original failure has deterministic reproduction evidence.
- [ ] Authoritative expected behavior is cited.
- [ ] Root cause explains the mechanism and why prior tests missed it.
- [ ] Contract owner and full production path are identified.
- [ ] Direct callers, affected processes, and risk are reported.
- [ ] Repository search found and reconciled duplicate rules/parallel paths.
- [ ] Counterexample matrix covers both blocking and release/terminal outcomes.
- [ ] A real production-path RED/GREEN regression exists.
- [ ] Adjacent and profile validations completed successfully.
- [ ] Full-suite status is stated accurately.
- [ ] Live read-only verification completed when applicable.
- [ ] Independent review has no unresolved P0/P1.
- [ ] GitNexus `detect_changes` matches the intended scope before commit.
- [ ] Remaining uncertainty, unrelated failures, and unverified assumptions are
      explicitly listed.
- [ ] The final implementation is the smallest coherent change, with every new
      abstraction/helper/file justified by a current contract or caller.

If any required box is unchecked, report "implemented but not closed" rather
than "fixed".

## 11. Required Handoff Format

Every substantive fix handoff must include:

```text
Status: fixed / implemented but not closed / blocked
Symptom and reproduction:
Authoritative contract:
Root cause:
Broken production path:
Invariant enforced:
Changed shared boundary:
Counterexample matrix covered:
Validation commands and results:
Independent review result:
Production verification:
Remaining uncertainty/non-goals:
```

## 12. Prohibited Closure Patterns

Do not:

- patch only the reported example and call it a root fix;
- add a new business-reason list when a canonical set already exists;
- validate a new helper while production calls another method;
- monkeypatch the target method and claim its real wiring works;
- treat missing rows, parse failures, or endpoint errors as empty/flat truth;
- let weaker evidence erase stronger confirmed evidence;
- change a regression expectation without contract proof;
- claim broad success from targeted test counts;
- claim a timed-out, interrupted, or still-running suite passed;
- use a GitNexus index of `HEAD` as proof that unstaged changes are indexed;
- perform implementation and independent closure review in the same review step;
- continue point-fixing after the repair-loop circuit breaker has triggered.
- turn a local bug fix into a speculative framework or unrelated refactor;
- keep dead, duplicate, or superseded paths merely to avoid simplifying the
  implementation;
- add repetitive tests when a parameterized contract matrix proves the same
  behavior more clearly.
