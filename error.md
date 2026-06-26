# ETSI NGSI-LD — OPEN Issues & ETSI Tool Bugs

> This file tracks failures, ETSI-tool bugs, and fixed items. Fixed items stay in this table with the `Fixed?` flag set to `yes`. Last updated: 2026-06-25.

| # | Date | Path | Test / Symptom | Root cause | Location | Fixed? | Notes |
|---|------|------|----------------|------------|----------|--------|-------|
| 1 | 2026-06-25 | ContextSource | `047_02/04/05/08/09` - Sub vs Reg type mismatch | Spec ambiguity / Fixture inconsistency | ETSI tool | **no (tool bug)** | Do not chase in Java. |
| 2 | 2026-06-25 | ContextSource | `047_12` / `047_14/15` - shouldSendOut filter excludes reg | Attribute/geo matchers bug on reg payload | Java: `checkRegForTypeAttrs` | **no** | Broker bug; needs rewrite. |
| 3 | 2026-06-25 | ContextSource | `047_03` - entities serialized as object not array | JSON-LD compaction collapse | Java: `Serializer` | **no** | Broker bug. |
| 4 | 2026-06-25 | ContextSource | `047_16_01/03` - vehicle update times out | Race condition in update path | Java: `RegistryService` | **no** | Broker bug. |
| 5 | 2026-06-25 | Consumption-Entity | `019_12_07` - Filter by attrs+datasetId returns `[]` | `QueryDAO` folds datasetId into entity match | Java: `QueryDAO` | **yes** | Fixed entity matching. |
| 6 | 2026-06-25 | Consumption-Temporal | `020_14_01/02` - 206 + Content-Range mismatch | Pagination headers missing/mismatched | Java: `HttpUtils` | **yes** | Applied in Iteration 1. |
| 7 | 2026-06-25 | Consumption-Temporal | `021_21_03` - pick on core-only members fails | PickTerm INNER JOIN drops entity | Java: `PickTerm.toTempSql`| **yes** | Fixed to `1=1` for core. |
| 8 | 2026-06-25 | Consumption-Temporal | `021_25` - local=true expects 1 entity, gets 2 | local=true doesn't drop local entities | ETSI tool | **no (tool bug)** | Broker behavior is correct. |
| 9 | 2026-06-25 | Provision-Temporal | `017_02_05` - empty attr delete returns 404 | `//` collapses to `/attrs/{instanceId}` | ETSI tool | **no (tool bug)** | JAX-RS framework boundary. |
| 10| 2026-06-25 | Provision-Attr | `010_07_01` - Append scope noOverwrite | Needs 207 body with EXPANDED attr names | Java: `EntityService` | **no** | Multi-status rabbit hole. |
| 11| 2026-06-25 | Provision-Attr | `011_05_02` - Update scope on no-scope entity | `updateLocalEntity` never reported `notUpdated` | Java: `EntityService` | **yes** | DB scope migration applied. |
| 12| 2026-06-25 | ContextSource | `042_02/03` - HTTPError raised on DELETE | Missing `expected_status=any` in DELETE | ETSI tool | **no (tool bug)** | Test harness bug. |
| 13| 2026-06-25 | ContextSource | `038_05_01` - No keyword found | Missing `Library DateTime` import | ETSI tool | **no (tool bug)** | Test harness bug. |
| 14| 2026-06-25 | ContextSource | `038_02_01` - 405 Method Not Allowed | Test doubles full Location path in URL | ETSI tool | **no (tool bug)** | Test harness bug. |
| 15| 2026-06-25 | ContextSource | `040_01/041_*` - nested regex ignore fails | Regex targets root-level keys instead of nested | ETSI tool | **no (tool bug)** | Test harness bug. |
| 16| 2026-06-25 | ContextSource | `037_07_01` vs `033_01_03` - self-contradiction | GeoProperty vs bare GeoJSON raw response | ETSI tool | **no (tool bug)** | Impossible to satisfy both. |
| 17| 2026-06-25 | ContextSource | `037_07_02` - 400 on malformed polygon | Unclosed polygon ring sent in payload | ETSI tool | **no (tool bug)** | Broker correctly rejects. |
| 18| 2026-06-25 | Subscription | `028_06 / 031_01_01` - representation cluster | `timesSent` seeded to 0 on fresh sub | Java: `SubService` | **no** | Requires Serializer update. |
| 19| 2026-06-25 | Subscription | `046_32 / 046_22_09` - previousValue is null | Old attribute value not propagated | Java: `EntityManager` | **no** | Cross-module update needed. |
| 20| 2026-06-25 | Subscription | `046_16_01` - regex type selection fails | Regex `|` matching gap | Java: `SubMatcher` | **yes** | Feature implemented. |
| 21| 2026-06-25 | Subscription | `046_19 / 046_22_07` - Link header implicit URL | Sub Original `@context` overridden | Java: `SubService` | **no** | Context wrapping design gap. |
| 22| 2026-06-25 | IOP (5-Broker)| `IOP_CNF_*` - representation mismatch | Retrieve without context vs Compacted expectation | ETSI tool | **no (tool bug)** | Harness test mismatch (0/24). |