# V1 Record Layer Full Parity and Semantic Alignment Design

**Goal:** Keep LightFeeV2's record layer fully aligned with Rust V1, preserving every V1-visible field, event kind, and replay-visible parameter used to reconstruct order flow, candidate filtering, and recovery evidence, while cleanly separating full-parity and partial-parity responsibilities for the rest of the live path.

**Primary source of truth:** `/media/wl/新加卷/codex/LightFee`

**Target repository:** `/media/wl/新加卷/codex/LightFeeV2`

**Date:** 2026-05-11

---

## Decision

The record layer is business evidence, not an auxiliary log stream.

For this phase, LightFeeV2 must not drop any V1-visible record data for convenience. If Rust V1 emitted a field, reason code, list item, or replay input on the live path, V2 must keep it or derive it losslessly. V2 may reorganize code, but it may not shrink the record contract.

In this document, "replay" means rebuilding state and outcomes from recorded evidence. "Observability" means the surfaces that present that evidence to humans or downstream tools. Observability may reformat the record, but it must not erase information.

## Alignment Classes

| Class | Meaning | Examples |
| --- | --- | --- |
| Full parity | V1-visible record schema, payload fields, replay inputs/outputs, and record-driven state reconstruction must match in meaning and completeness | journal envelope, order lifecycle records, candidate/filter list records, recovery evidence, replay reconstruction |
| Partial parity | Presentation and packaging may differ if the underlying record evidence stays lossless | report formatting, offline summaries, helper decomposition, directory layout |
| No parity requirement | Internal structure that does not affect the record contract or replay evidence | file organization, naming style, helper boundaries |

## Scope

In scope:

- append-only journal shape and critical append semantics
- order records and state-transition records
- candidate/filter list records and their reasons
- replay input and replay output contracts
- persistence metrics that are part of the record contract
- offline analysis and reporting that consume the same raw evidence
- tests and fixtures that prove no V1-visible field is lost
- docs and parity tracking for record-related drift

Out of scope:

- trading formula changes
- venue API changes unless they are needed to preserve record evidence
- file-layout mimicry
- summary/report styling that does not alter the evidence itself

## Record Contract

The record layer must preserve:

- envelope fields such as sequence, run id, timestamp, kind, and payload
- order lifecycle evidence, including submits, acknowledgements, fills, rejections, cancels, replaces, and uncertain outcomes
- candidate/filter list evidence, including blocked reasons, acceptance reasons, and the full candidate list that produced a decision
- recovery evidence, including enough payload to rebuild live state after restart
- replay-visible inputs and outputs, including the state needed to reconstruct open positions, pending work, lifecycle, and risk mode
- metrics that are part of the persisted contract, not just in-memory counters

The record layer may add normalized fields, but it must not remove any V1-visible ones.

## Success Criteria

This design is satisfied when:

- journal records round-trip without losing V1-visible fields
- candidate/filter list records can be replayed without synthesizing missing data
- order success, rejection, and partial-fill evidence can be reconstructed from records alone
- replay output can rebuild the same live-state timeline that V1 uses for recovery and analysis
- the repo can clearly distinguish full parity from partial parity without mixing the two

## Non-Goals

- reducing record payload size by dropping fields
- replacing replay with aggregate-only summaries
- rewriting Rust file structure in Python
- treating observability as a substitute for record fidelity

