# Scorpio Context Broker — VERIFIED Memory & Optimization Analysis (v2)

Date: 2026-07-25 · branch `development-quarkus` · analyst: ScorpioBroker agent
Method: first-pass audit (5 parallel area audits) → **adversarial verification pass** (5 independent
verifiers, each required to refute findings against the current working tree using `/usr/bin/grep`
and direct file reads — the sandbox grep wrapper silently skips files and was bypassed) → NGSI-LD
spec grounding via pdf-rag over `gs_CIM009v010901p.pdf` (ETSI GS CIM 009 V1.9.1, 2025-07).
Working tree diff at verification time touched only `.DS_Store`, `MAC_DEV_SETUP.md`, `README.md` —
none of the audited files. `/workspace/compose-files/ScorpioBroker/` is a stale duplicate tree,
excluded everywhere.

Classification legend:
- **BUG** — verified code defect (wrong behavior, leak, or race).
- **MISSING-IMPL** — spec-mandated behavior that is absent (forgotten implementation).
- **SPEC-VIOLATION** — behavior that actively contradicts ETSI GS CIM 009.
- **CONFIG-GAP** — missing/misconfigured runtime setting; no code defect.
- **DESIGN-DEBT** — works correctly but wastes memory/allocations; no spec dimension.
- **CORRECTED** — first-pass claim was wrong or already implemented; correction stated.

## Verdict summary

| ID | Finding | Verdict | Class |
|----|---------|---------|-------|
| R1 | No JVM memory flags in runtime image | CONFIRMED | CONFIG-GAP |
| R2 | No mem_limit on any of 16 compose containers | CONFIRMED | CONFIG-GAP |
| R3 | 5 unused Kafka containers (in-memory-baked image) | CONFIRMED | CONFIG-GAP |
| R4 | Caffeine caches have TTL but no maximum-size | CONFIRMED | CONFIG-GAP |
| R5 | quarkus.http.limits.max-body-size unset | CONFIRMED | CONFIG-GAP |
| R6 | messaging.maxSize=2 GB in default/in-memory profile | CONFIRMED | CONFIG-GAP |
| R7 | event-loops-pool-size=20 hardcoded | CONFIRMED | CONFIG-GAP |
| R8 | No direct-memory caps anywhere | CONFIRMED | CONFIG-GAP |
| R9 | No Kafka client buffer tuning; broadcast=true everywhere | CONFIRMED | CONFIG-GAP |
| R10 | `$[quarkus.uuid}` broken interpolation → literal group.id | CONFIRMED | BUG |
| R11 | Micrometer metrics fixed-name, no tags | CONFIRMED OK | non-issue |
| L1 | Agroal/Flyway datasource never closed per tenant | CONFIRMED | BUG (leak) |
| L2 | tenant2Client plain HashMap, racy, never evicted | CONFIRMED¹ | BUG (leak) |
| L3 | syncDeleteSubscription leaks subscriptionId2RequestGlobal | CONFIRMED | BUG (leak) |
| L4a | Subscription expiry: no eviction; interval subs fire when expired; no create-time reject; no derived status | CONFIRMED | MISSING-IMPL + SPEC-VIOLATION |
| L4b | Registration expiry "never checked" | **CORRECTED** — checked+lazily evicted in most paths; HistoryQueryManager unchecked; over-eviction bug found | PARTIAL + 2 new BUGs |
| L4c | 7 in-VM registration table copies | CONFIRMED | DESIGN-DEBT |
| L5 | MQTT clients never closed, racy creation, plain HashMap in RegSub | CONFIRMED | BUG (leak) |
| L6 | Orphaned remote-notify callbacks + 2 wrong-key Table lookups | CONFIRMED | BUG (leak + federation) |
| L7 | ContextCache side maps unbounded/unsynchronized | CONFIRMED | BUG (leak) |
| L8 | Tenant-key leaks, RegSub internal delete misses interval table, startup buffers | CONFIRMED | BUG (minor leaks) |
| U1 | 11 WebClients, all default options, unbounded wait queue, no timeouts | CONFIRMED | BUG (unbounded) |
| U2 | History write buffer unbounded, PRE_PROCESSING ack | CONFIRMED | BUG (unbounded) |
| U3 | Temporal array_agg unbounded without lastN; post-hoc slice with lastN | CONFIRMED | DESIGN-DEBT (spec-sanctioned fix exists) |
| U4 | Federated pages accumulate without cap | **PARTIAL** — retrieve-mode recursion already fixed; query-mode unbounded remains | DESIGN-DEBT |
| U5 | entitymap insert without LIMIT; local path OK | CONFIRMED | DESIGN-DEBT |
| U6 | 11 separate WebClient pools in one JVM | CONFIRMED | DESIGN-DEBT |
| J1a | Core-context clone per parse just for keySet | CONFIRMED | DESIGN-DEBT |
| J1b | No in-VM parsed-@context cache | CONFIRMED | DESIGN-DEBT |
| J1c | inverse map rebuilt per compaction | CONFIRMED | DESIGN-DEBT |
| J1d | Notification context re-parsed per delivery | **CORRECTED** — default path reuses SubscriptionRequest.context; only explicit jsonldContext re-parses | PARTIAL |
| J2 | payload.toString() twice per write | CONFIRMED | BUG (perf) |
| J3 | Pretty-print + zip response copies, no streaming | CONFIRMED | DESIGN-DEBT |
| J4 | deepCopyMap fan-out incl. dead-store copies | CONFIRMED (dead-store verified; `:971` copy is live — first pass partly wrong there) | BUG (dead code) + DESIGN-DEBT |
| J5 | Compaction mutates input expanded entity | CONFIRMED | BUG (root cause of J4) |
| J6 | Per-sub copy+compact+prettyString; throttle check after build | CONFIRMED | DESIGN-DEBT + BUG (ordering) |
| J7 | Kafka emit byte[]→String→byte[]; payload+prevPayload | CONFIRMED | DESIGN-DEBT |
| J8a-e | Expansion internals churn (5 sub-findings) | ALL CONFIRMED | DESIGN-DEBT |
| J9 | Batch: 3+ whole-batch representations, context parse per entity | CONFIRMED (History part corrected: single-entity endpoints, no batch amplification) | DESIGN-DEBT |
| J10 | Per-registration compacted copy + String pinned by closure | CONFIRMED | DESIGN-DEBT |
| J11a | getJsonObject(0) twice per row | CONFIRMED | BUG (perf, trivial) |
| J11b | Batch upsert returns old+new entity | CONFIRMED but **JUSTIFIED** — old feeds prevPayload for notifications | non-issue |
| J11c | Zero RowStream/Cursor usage repo-wide | CONFIRMED | DESIGN-DEBT |
| J11d | RegSub throttling: seconds-vs-millis, defer-not-drop | CONFIRMED | BUG + SPEC-VIOLATION |
| B1 | (new) getEntityMap uses `first` instead of loop row | CONFIRMED | BUG |
| B2 | (new) concise compaction removes INSTANCE_ID where unitCode intended | CONFIRMED | BUG (latent) |

¹ L2 softened: `setShared(true)` means raced PgPool wrappers share the underlying pool, so the race
leaks wrapper objects + one Agroal pool (L1), not a second connection set.

---

# Section-by-section analysis

## R1 — Runtime image has no JVM memory configuration — CONFIG-GAP, 100% verified
`AllInOneRunner/src/main/docker/Dockerfile.jvm` (7 lines), line 7:
`CMD java --enable-native-access=ALL-UNNAMED -jar quarkus-run.jar`.
`dev/build-image.sh:31` proves this exact file builds `scorpio-local:latest` (used by `run-iop.sh:29`).
No `-Xmx`, `-XX:MaxRAMPercentage`, GC choice, metaspace or direct-memory cap, no `JAVA_OPTS`
passthrough. JVM ergonomics therefore size max heap at 25% of *visible host* RAM per broker.
Not a spec topic. **Fix:** add `-XX:MaxRAMPercentage=60 -XX:+UseG1GC -XX:MaxMetaspaceSize=256m
-XX:MaxDirectMemorySize=256m -XX:+ExitOnOutOfMemoryError` and honor `JAVA_OPTS`.

## R2 — No memory limits in the 5-broker stack — CONFIG-GAP, 100% verified
`compose-files/docker-compose-iop.yml` (172 lines, 16 services incl. templates x-postgres-tmpl/
x-kafka-tmpl/x-mqtt-tmpl/x-scorpio-tmpl): zero occurrences of `mem_limit`/`deploy.resources`.
Combined with R1: 5 JVMs × 25% host + 5 Kafka (~1 GB each) + 5 Postgres. Explains the documented
"build OOM workaround (stop scorpio2-5)". **Fix:** `mem_limit: 1g`–`1.5g` per scorpio, `512m` per
kafka with `KAFKA_HEAP_OPTS=-Xmx256m`.

## R3 — Five Kafka containers run unused — CONFIG-GAP, 100% verified
`run-iop.sh:28-29` builds with the in-memory profile (baked at build time, `build-image.sh:9-11,22`).
Compose still starts `kafka`..`kafka5` (lines 56/81/104/127/150). Nuance verified: scorpio services
DO set `BUSHOST: kafka..kafka5` (lines 68/93/116/139/162), but `${bushost}` is only consumed by
`application-kafka.properties` (kafka profile), so the env vars are inert dead config — the brokers
provably never connect. ~5 GB RSS wasted. **Fix:** remove or profile-gate the kafka services.

## R4 — Caffeine caches: TTL but no size bound; client-controlled keys — CONFIG-GAP, 100% verified
`quarkus.cache.caffeine.expire-after-access=20m` in AllInOneRunner/AtContextServer/InfoManager
application.properties (lines 28/24/24). Repo-wide grep: **no `maximum-size` in any properties
file.** The `@CacheName("context")` cache (`ContextCache.java:42`) is keyed by client-supplied
@context URLs (`Context.java:252-257` routes every external URL from any request Link header through
`createcache/`), so an unauthenticated client can grow it without bound for 20-minute windows.
Spec note: JSON-LD context caching itself is implementation territory (the spec's
`/jsonldContexts` management endpoints are supported); the DoS window is purely a config gap.
**Fix:** `quarkus.cache.caffeine."context".maximum-size=1000` + a global default.

## R5–R9 — Remaining config gaps — CONFIG-GAP, 100% verified
- **R5**: `max-body-size` appears nowhere (only guard = Quarkus 10 MB default; batch limit 1000
  entities × 3-5× expansion multiplier). Set explicitly (e.g. `5M`).
- **R6**: `scorpio.messaging.maxSize=2147483646` (`application.properties:97`); kafka profile
  overrides to 1 MB (`application-kafka.properties:6`); **no `application-in-memory.properties`
  exists** — verified by find. In-memory mostly bypasses serialization, but mqtt/amqp/sqs builds
  inherit the 2 GB base. Set a 1–4 MB base.
- **R7**: `quarkus.vertx.event-loops-pool-size=20` (`application.properties:8`) regardless of CPUs.
  Remove (default = 2×cores).
- **R8**: no `MaxDirectMemorySize`/`io.netty.maxDirectMemory` anywhere; default direct ceiling =
  max heap, doubling worst-case per broker. Covered by R1 flags.
- **R9**: `application-kafka.properties` (57 lines) has no `buffer.memory`/`fetch.max.bytes`/
  `max.poll.records`/`max-queue-size`; all 5 incoming channels `broadcast=true` (lines
  12/24/36/47/56) so every record is held until the slowest in-JVM subscriber acks. Defaults:
  5×32 MB producer buffers + 5×50 MB consumer fetch ceilings in one AAIO JVM.

## R10 — Broken property interpolation — BUG, 100% verified
`application-kafka.properties:4-5`: `client-id-prefix=$[quarkus.uuid}` and
`group.id=$[quarkus.application.name}$[quarkus.uuid}` — `$[…}` instead of `${…}`. The literal
string becomes the consumer group id, so **all broker instances share one Kafka consumer group**:
in a multi-instance deployment, entity messages are load-balanced between instances instead of
broadcast, which breaks subscription sync (each instance misses a share of sync messages).
Not a spec topic; a real distributed-deployment bug. **Fix:** `${quarkus.uuid}` etc.

## R11 — Micrometer metrics — verified NON-ISSUE
All annotations use fixed literal names, zero dynamic tags (samples verified in EntityController,
QueryController, EntityBatchController; repo-wide grep for `extraTags`: none). Bounded cardinality.

## L1 — Per-tenant Agroal/Flyway datasource leak — BUG (true leak), 100% verified
`ConnectionManager.java:264-284`: `AgroalDataSource.from(configuration)` (line 276, `initialSize(1)`)
is passed only to `flywayValidateAndMigrate` (320-366 — which never closes it either); the method
returns a String; the variable appears nowhere else in the file (368 lines read in full). One open
JDBC pool + housekeeper thread leaks per tenant-pool construction, and per race duplicate (L2).
Not spec-related (multi-tenancy 5.5.10 says nothing about pooling). **Fix:** try-with-resources.

## L2 — tenant2Client: unsynchronized, unbounded, never evicted — BUG, 100% verified (softened)
`ConnectionManager.java:119` plain `HashMap`; puts at 114 and 253; **zero removes, zero closes**
repo-wide; check-then-act between `executeQuery:141` and `getTenant:253`. Softening (verified):
`options.setShared(true).setName(db)` at 242-243 → raced wrappers share the underlying pool, so the
overwrite leaks a wrapper object, not a connection set — but each race also fires L1's Agroal leak.
There is **no tenant-delete path in the codebase at all** (grep for deleteTenant/DROP DATABASE:
nothing), so "eviction on tenant delete" has no hook today. **Fix:** `ConcurrentHashMap.computeIfAbsent`
(single-flight), bounded Guava LoadingCache with close-on-evict if tenant lifecycle ever arrives.

## L3 — syncDeleteSubscription leaks the global request map — BUG (true leak), 100% verified
All `subscriptionId2RequestGlobal` sites enumerated: puts at 437/568/736/743/2278/2341; the ONLY
remove is line 770 inside local `deleteSubscription`. `syncDeleteSubscription` (2236-2242) and the
`syncUpdateSubscription` failure path (2245-2251) remove from both Tables but not the global map.
In multi-instance (kafka-sync) mode every remotely-deleted subscription leaks its
`SubscriptionRequest` + fully parsed Context forever. Side effect: `getSubscription` (866-867)
then serves **stale status** from the leaked entry. **Fix:** add the remove to both sync paths.

## L4a — Subscription expiry — MISSING-IMPL + SPEC-VIOLATION, 100% verified
Spec (ETSI GS CIM 009 V1.9.1):
- 5.8.6 Notification behaviour: "Notifications shall only be sent **if and only if** the status of
  the corresponding subscription is `active`, i.e. not `paused` nor `expired`."
- Subscription behaviour clause: "Implementations **shall ensure** that, when the Subscription
  expiration date is due, the status of the Subscription changes automatically to `expired`, so
  that notifications will no longer be sent."
- Create behaviour (5.8.1.x): expiresAt in the past ⇒ **BadRequestData**.
- 5.8.2.4 Update: expiresAt in the past ⇒ BadRequestData; status transitions defined.

Verified state of `SubscriptionManager` (grep -i expire over the whole module = one unrelated SQL
column list):
1. **Change-driven path IS gated** (compliant): `SubscriptionRequest.firstCheckToSendOut`
   (`Commons/.../SubscriptionRequest.java:154-157`) returns false when
   `!isActive || expiresAt < now`; called at SubscriptionService.java:910/925.
2. **Interval path is NOT gated** (SPEC-VIOLATION of 5.8.6): `checkIntervalSubs`
   (SubscriptionService.java:1847-1880) iterates `tenant2subscriptionId2IntervalSubscription`
   checking only `lastNotification + timeInterval` — an expired interval subscription keeps
   notifying forever.
3. **No create/update-time rejection of past expiresAt** in SubscriptionService (MISSING-IMPL of
   5.8.1.x/5.8.2.4) — RegistrySubscriptionService HAS this (with a 5.11.2 comment) and also derives
   status `expired` on read; SubscriptionService has no counterpart (866-867 returns stored status
   as-is → retrieving an expired subscription shows `active`, contradicting 5.8.2.4/5.2.12).
4. **No eviction** of expired subscriptions from any of the three in-VM maps (memory: entries pin a
   parsed Context each; spec is satisfied by status semantics alone, so eviction is the
   implementation half of the fix).
**Fix:** port the two RegistrySubscriptionService behaviours over; gate `checkIntervalSubs` on
expiry; sweep expired entries in the same scheduler.

## L4b — Registration expiry — first-pass claim CORRECTED; two NEW bugs — 100% verified
The claim "expiresAt checked only in EntityService, merely skipped" is **wrong**. Verified reality:
- Checked AND lazily evicted (`it.remove()` on live row views) at EntityService.java:1348/2863,
  HistoryEntityService.java:602, via `EntityTools.getRemoteQueries`
  (`EntityTools.java:1008-1011`, feeding QueryService:1716/1986) and via
  `SubscriptionTools.getRemoteSubscriptions` (`SubscriptionTools.java:624`, feeding
  SubscriptionService:605-608).
- **NEW BUG (over-eviction):** at SubscriptionTools:620-627, EntityTools:1004-1011 and
  EntityService:2858-2865 the `it.remove()` runs on the **outer** iterator — one expired
  RegistrationEntry evicts the csource's ENTIRE registration list. A csource with one expired and
  nine live registrations loses all ten from the in-VM cache.
- **Remaining MISSING-IMPL:** HistoryQueryService.java:389-391 and 438-439 iterate registration
  rows with **no expiry check** — temporal queries still forward to expired csources. Spec 5.2.9:
  past `expiresAt` the Context Source Registration "will become invalid" ⇒ forwarding to it
  contradicts the spec.
**Fix:** flip the eviction to the inner entry (or filter without removing), add the expiry check to
HistoryQueryService.

## L4c — Seven in-VM registration table copies — DESIGN-DEBT, 100% verified
`HashBasedTable` mirrors at EntityService.java:113-114 (×2), QueryService.java:95,
HistoryEntityService.java:92, HistoryQueryService.java:87, SubscriptionService.java:150-151 (×2).
Removal only on registration DELETE/change + the lazy expiry eviction above; no scheduled sweep.
Memory = registrations × tenants × 7. Not spec-related. **Fix (structural):** one shared store bean.

## L5 — MQTT client lifecycle — BUG (leak), 100% verified
SubscriptionService.java:163 `ConcurrentHashMap`, refs only at 163/1772/1818/1822 — put happens
inside the async `connect().onItem()` callback after a `containsKey` check (check-then-act: two
concurrent first notifications to a new endpoint create two connected clients; the loser stays
connected and unreferenced). **No remove/disconnect anywhere**; the only `@PreDestroy`
(`unsubscribeToAllRemote`, 2224-2234) does HTTP unsubscribes only. RegistrySubscriptionService.java:103
is the same pattern on a **plain HashMap** mutated from reactive callbacks (646-653) — corruption
possible. MQTT binding (spec clause 7) doesn't govern client pooling — pure implementation bug.
**Fix:** single-flight creation, disconnect-on-evict cache, close all on shutdown, concurrent map.

## L6 — Remote-subscription callback/table bugs — BUG (leak + federation), 100% verified
Field types verified (SubscriptionService.java:146,157-158; column type `SubscriptionRemoteHost`).
1. `prepareNotificationServlet` (679-684): `put(remoteHost, uuid)` discards the previous uuid whose
   entry stays in `remoteNotifyCallbackId2SubRequest` forever (cleanups at 234-236/809-811 remove
   only the current mapping). Leak per registration churn.
2. Line 626: `tenant2RemoteHost2SubIds.get(tenant, remoteHosts)` passes the whole **Collection** as
   the column key → always null → every host treated as new: re-subscribe churn + fresh-set clobber
   of accumulated subIds.
3. Line 695: `get(tenant, subId)` passes a String where the column is `SubscriptionRemoteHost` →
   always null → line 696 is a guaranteed NPE when reached with non-empty subs; remote unsubscribe
   cleanup aborts, rows + remote subscriptions stranded (correct-key contrast at line 803).
Federation-facing: this breaks distributed-subscription bookkeeping (5.11 Context Source
Registration Subscriptions) — the broker re-subscribes to remote csources on every update instead
of reusing, and can fail to unsubscribe. **Fix:** correct both keys; remove superseded uuid on put.

## L7 — ContextCache side maps — BUG (leak), 100% verified
`ContextCache.java:59-60` plain HashMaps; writes at 129-132/169-172; removal only in `invalidate()`
188-191; the TTL sweep is **commented out** (194-199). Keys are client-supplied context URLs (same
attack surface as R4), unsynchronized concurrent mutation. **Fix:** fold into the cached value or a
TTL Caffeine cache; ConcurrentHashMap regardless.

## L8 — Minor leaks — BUG (minor), 100% verified
(a) `HistoryMessagingBase.java:40-41` tenant keys never removed (bounded by tenant count — low).
(b) RegistrySubscriptionService.java:1389-1393 internal-channel delete removes only
`tenant2subscriptionId2Subscription`; API delete (318-319) and sync delete (1519-1527) remove both
tables — an interval reg-subscription deleted via the internal channel **keeps firing on the
scheduler**. (c) Startup buffers (168-170) unbounded but only until `ready=true` at 492 — bounded
in time, acceptable with a cap as hardening.

## U1 — Notification/forwarding WebClients unbounded — BUG (unbounded queue), 100% verified
All **11** creation sites enumerated (SubscriptionService:397, JsonLDService:52-54 — empty options
object, ContextCache:66, ContextService:41, HistoryEntityService:100, QueryService:111,
HistoryQueryService:91, EntityService:130, RegistrySubscriptionInfoDAO:51,
RegistrySubscriptionService:122, CSourceService:108). Repo-wide grep for
`setConnectTimeout|setIdleTimeout|setMaxWaitQueueSize|setMaxPoolSize`: **zero hits** — Vert.x
defaults everywhere: maxPoolSize 5/host, `maxWaitQueueSize=-1` (unbounded), no timeouts. The
notification send (1640-1644) adds `.retry().atMost(3)` with no per-request `.timeout(...)` (the
only timeout in the codebase is the 10 s @context fetch, JsonUtils.java:383). One dead endpoint ⇒
5 saturated connections ⇒ every later notification for that host parks forever, pinning its full
pretty-printed Buffer, ×3 retries. Spec note: 5.8.6 failure handling = update `lastFailure`/status
`failed` in DB (Scorpio does this — compliant); nothing mandates unbounded queueing — pure bug.
**Fix:** shared WebClient with connect/idle timeouts, bounded wait queue, per-request timeout;
optional auto-pause after K consecutive failures (status `failed` already exists in the data model).

## U2 — History write buffer — BUG (unbounded, no backpressure), 100% verified
`HistoryMessagingBase.java:40` per-tenant `ConcurrentLinkedQueue<BaseRequest>`; adds at 129/180/185
with no cap; `autorecordingbuffersize` only triggers flush (line 215); kafka-mode flush is
fire-and-forget (224-228). **All** consumers ack `PRE_PROCESSING` (HistoryMessagingString:20,26;
HistoryMessagingByteArray:23,29; also InMemory:20,26) — offsets commit before processing: no
backpressure, message loss on crash. In-memory profile flushes synchronously (62, 132-134, 189-191)
so exposure is kafka/amqp/sqs. Temporal storage behavior itself (5.6.x) is met; the buffer is
implementation. **Fix:** cap + POST_PROCESSING/MANUAL ack + bounded in-flight flushes.

## U3 — Temporal instance aggregation unbounded — DESIGN-DEBT with spec-sanctioned fix, 100% verified
`HistoryDAO.java:769` aggregates ALL instances per attribute (`array_agg(teai.data ORDER BY …)`);
slice `[1+offsetN:n]` (774-780) only when lastN present and only AFTER full aggregation; **no
instance ceiling config exists** (verified: HistoryQueryManager properties contain only the topic
name). Java side: whole jsonb array per row (`getJsonArray(7).getList()`, line 616), page sorted in
Java when orderBy set (679-681), window branch over-fetches limit×4/limit×2 (892-905).
Spec: 5.7.4 mandates returning matching instances, but **6.3.10 Pagination behaviour explicitly
defines temporal pagination** via `Content-Range` (unit DateTime; backwards when lastN present,
forwards otherwise, `size="*"`) — i.e. a broker MAY truncate a temporal response and signal the
served range. Bounding instances server-side is therefore spec-compliant, not a hack.
**Fix:** lateral `ORDER BY <timeprop> LIMIT n` push-down; default instance ceiling + Content-Range
signalling per 6.3.10.

## U4 — Federated page accumulation — PARTIAL (retrieve fixed; query unbounded), 100% verified
ALREADY-IMPLEMENTED: retrieve-mode next-link recursion is gated
(`EntityTools.java:950-954` — comment documents the IOP federated-retrieve hang fix;
recursion only when `isCanDoBatchQuery()||isCanDoQuery()`).
CONFIRMED remaining: in query mode, lines 956-965 recurse and `expanded.addAll(nextResult)` across
ALL remote pages; the caller's `limit` is only sent as a per-page param (line 703), never used to
stop following next-links — a remote broker with many pages (or a hostile one with endless
next-links) inflates heap without cap. Three representations (body String, parsed list, expanded)
coexist per page (938-946). `QueryService.mergeMultipleQueryResults` (399-480) re-materializes into
six accumulator maps + EntityCache double-storage (422/472); temporal twins at
HistoryQueryService:157-173/288-314. Spec: distributed operations (4.3.6) don't require exhaustive
eager page-following; local pagination semantics can be preserved while capping.
**Fix:** stop paging at caller's limit; cap total accumulated entities; drop raw refs pre-expansion.

## U5 — entitymap machinery — DESIGN-DEBT + NEW BUG B1, 100% verified
Distributed path inserts the FULL match set into entitymap with no LIMIT (`QueryDAO.java:1287-1299`;
the LIMIT at 1304-1312 applies only to the subsequent page read). `storeEntityMap` (776-813) builds
one Tuple per entityId×csource in heap. `getEntityMap` (857-888) reloads the entire map per token.
Local-only path verified CORRECT (limit+1 probe row + `count(*) over()`, 1225/1245-1256) — matches
spec 5.5.9.3 entity-map pagination intent.
**NEW BUG B1 (correctness):** `QueryDAO.java:883` reads `cIds.add(first.getString(1))` inside the
`while (it.hasNext())` loop — `first` instead of the loop row: every entity in a reloaded entity map
is attributed to the FIRST row's csourceid. Distributed follow-up-page queries go to the wrong
csources. **Fix:** use the loop row; LIMIT the insert; page the reload.

## U6 — 11 WebClient pools in one JVM — DESIGN-DEBT, 100% verified
List under U1. Each has its own connection pool + Netty buffers in the AAIO. **Fix:** one shared
`@Singleton` WebClient with tuned options (merges with U1's fix).

## J1a — Core-context clone per parse for a keySet — DESIGN-DEBT, 100% verified
`Context.java:193-196`: `coreTermDefs = JsonLdProcessor.getCoreContextClone().termDefinitions.keySet()`
— `clone()` copies the entire core termDefinitions LinkedHashMap (1041-1048); the clone is used only
for `contains` at line 358. Runs per `Context.parse` with the NGSI-LD check enabled = per request /
per batch entity. **Fix:** static immutable `CORE_TERM_KEYS`.

## J1b — No in-VM parsed-@context cache — DESIGN-DEBT, 100% verified
`DocumentLoader.java:56-79`: only "cache" is `m_injectedDocs` whose feeder `addInjectedDoc` has
**zero callers repo-wide**. Every load = HTTP round-trip (`JsonUtils.fromURL`, raw WebClient send,
no memoization) + full parse + `createTermDefinition` per key. The `createcache/` rewrite
(Context.java:243-262) caches the *document* in Postgres server-side but does not remove the
per-request HTTP + parse cost. Consequence measured in J9: batch of N entities with one shared Link
header ⇒ N clones + N HTTP fetches + N term-definition rebuilds. Spec: context handling (5.5.7 term
expansion) doesn't prohibit caching parsed contexts. **Fix:** bounded Caffeine LRU keyed by
canonical @context value → immutable parsed Context, invalidated on atcontext updates.

## J1c — inverse rebuilt per compaction — DESIGN-DEBT, 100% verified
`clone.inverse = null` on every core clone (JsonLDService:77-81, JsonLdProcessor:80-84);
`getInverse()` (Context.java:1060-1065 lazy check, then full rebuild sorting ALL termDefinitions
keys 1078-1084) fires on first `compactIri` of each per-request context. **Fix:** share inverse
copy-on-write between clones.

## J1d — Notification context re-parse — CORRECTED (mostly implemented)
Current `SubscriptionTools.java:499-510`: default path returns `potentialSub.getContext()` — the
parsed Context already stored on `SubscriptionRequest` (field line 41). Re-parse per delivery
happens ONLY when the subscription carries an explicit `jsonldContext` (`ldService.parse(explicit)`
per notification, uncached). First-pass claim overstated. Residual fix: cache the parsed explicit
context on the request object at create/update.

## J2 — Whole-payload `toString()` twice per write — BUG (perf), 100% verified
`HttpUtils.java:1454-1455` verbatim: two independent `originalPayload.toString()` calls doing
`contains("value=null")` / `contains("type=null")` per `expandBody` — the universal write entry
point (callers enumerated: EntityController create/update/merge/replace, EntityBatchController ×4,
HistoryController ×3, SubscriptionController ×2, NotificationController, RegistrySubscriptionController
×2, RegistryController ×2). ~2-4 MB transient char[] per 1 MB entity, twice. Note: Java `Map.toString`
output is also not JSON — matching `value=null` this way is fragile (a literal string value
"value=null" inside user data false-positives). **Fix:** one recursive walk.

## J3 — Response assembly copies — DESIGN-DEBT, 100% verified
`toPrettyString` at HttpUtils.java:891/940/986-988 (+1541) inside lambdas still holding
expanded+compacted trees; `zipResult` (292-308) adds getBytes → zip baos → toByteArray copies;
repo-wide: zero `StreamingOutput`/`Multi<Buffer>` — nothing streams. Spec does not require pretty
printing. **Fix:** stream via JsonGenerator, non-pretty default.

## J4 — deepCopyMap fan-out + verified dead-store — BUG + DESIGN-DEBT, 100% verified
Impl 436-469 (+`deppCopyList` 472-501) with `toString()` fallback (463) that silently mangles
non-standard scalar types. Verified hot callers: `sendObjectInMemory` 351-359 → `BaseRequest.copy()`
deep-copies payload AND prevPayload per extra in-JVM receiver on every CRUD op; SubscriptionService
:1086 (deliberate CME guard), :1277, :2158-2195; QueryService :854/:896 per inlined linked entity
per join level. **Dead-store CONFIRMED at SubscriptionService.java:1919-1936**: both loops allocate
`dupl = deepCopyMap(entity)`, call `mergePrevIntoQueryResult(..., dupl)` (verified side-effect-free
beyond `dupl` itself, 2026-2064), and never read `dupl` — up to 1000 full entity copies per
interval-sub firing thrown away; the merge result being discarded is very likely also a functional
bug (join/showChanges data never reaches `dataToNotify`). CORRECTION vs first pass: the `:971` copy
IS used (`dupl.putAll` :972, `toAddLater` :981) — not dead. **Fix:** delete the dead loops or wire
`dupl` into `payloadToUse`; then J5 unlocks removing most remaining copies.

## J5 — Compaction mutates its input — BUG (root cause), 100% verified
Data flow traced: `JsonLdApi.compact` casts the caller's map (line 208) and `compactEntity` passes
`elem.get(key)` straight in (797→850). keyValues branch: direct cast + `attribMap.remove(JSON_LD_TYPE)`
(880-882). Concise branch: same + bulk removes 980-999. Language-map handling removes `@language`
from input entries (349/356/368/385 and duplicated logic at 3478-3517). Only the normal branch
defensively copies (1059). This input mutation is WHY the defensive deep copies (J4) exist — shared
expanded maps get corrupted otherwise. **NEW BUG B2 (latent):** lines 988-991 remove
`NGSI_LD_INSTANCE_ID` twice — the second remove is on the `unitCode` constant position (copy-paste),
so `unitCode` survives concise output while a second no-op instanceId remove runs. **Fix:** make
compaction read-only; fix the unitCode remove.

## J6 — Notification fan-out + throttle ordering — DESIGN-DEBT + BUG, 100% verified
Per matching sub (loop :893): deep copy (:971/:1086/:1277) → compaction
(`generateNotification` → `ldService.compact`, SubscriptionTools:335-366) → `:1643`
`sendBuffer(Buffer.buffer(JsonUtils.toPrettyString(notification)))`. Ordering verified end-to-end:
the throttle check `sinceLast < getThrottling() * 1000L` sits at :1698-1707, AFTER `generated`
(:1563) and after the eager `toPrettyString`/Buffer argument evaluation at :1643 — a throttled
notification is fully built, then dropped (the Uni is never subscribed, so no HTTP goes out; all
allocation/CPU already spent). Spec: 5.2.14 throttling = "minimal period of time in seconds which
shall elapse between two consecutive notifications" — dropping intermediate notifications is a
legitimate reading; building-then-dropping is purely wasted memory. **Fix:** hoist the check to the
top of `sendNotification`; group subs by (context, format, sysAttrs, showChanges) to share one
compaction+Buffer per group.

## J7 — Kafka emit copies — DESIGN-DEBT, 100% verified
`MutinyEmitter<String>` (signature line 100-101); chunks gathered in `List<byte[]> toSend` (145);
emit at 334-337 `sendAndForget(new String(entry))` — byte[]→String→byte[] round trip per chunk;
payload (166) AND prevPayload (185) serialized per entity; `scorpio.topics.entity.zip=false`
default. prevPayload is REQUIRED downstream (see J11b) — the waste is the copies, not the content.
**Fix:** byte[] emitter/serializer; emit chunks as finalized.

## J8 — Expansion internals — DESIGN-DEBT, all 5 verified
(a) two eager HashSets per NGSIObject (103/108), instantiated per JSON node across ~17 JsonLdApi
sites. (b) `new ObjectMapper()` + double parse + full core-context clone **per string GeoProperty
value** (NGSIObject:864-868 → JsonLdApi:1286-1289 → getCoreContextClone). (c) 18 non-test
`new JsonLdOptions` sites each allocating an unused DocumentLoader+HashMap — including every
`new Context()` (Context.java:57/74). (d) compactIri miss-path iterates ALL termDefinitions with
per-candidate String concat (Context.java:943-959). (e) context fetch does
Buffer→String→byte[]→ByteArrayInputStream→parse (JsonUtils.java:427-428).
**Fixes:** lazy sets; static ObjectMapper + shared immutable core context; static comparator;
iri→term memo; parse from bytes once.

## J9 — Batch amplification — DESIGN-DEBT, verified (History part corrected)
`EntityBatchController.java:83-160` (+172-179 upsert and siblings): raw body String + full
`new JsonArray(body).getList()` + per-entity `expandBody` accumulating ALL expanded entities +
**one Context parse per entity — verified: no `contexts.get(0)` reuse, no cache in the loop, and
`ldService.parse` re-parses per call** (`getContextFromPayload` → `parse` → core clone). The whole
batch is then wrapped in `BatchRequest extends BaseRequest` and serialized/deep-copied by messaging.
Peak ≈ 6-10× wire size per in-flight batch. CORRECTION: HistoryController :61/:109/:171 are
single-entity endpoints (expandBody per request — normal), no batch amplification there.
Spec: batch behaviour 5.5.11 constrains semantics, not implementation; a shared context parse for a
shared Link header is fully compliant (5.5.7 expansion is per-document semantics).
**Fix:** parse the Link-header context once per batch; stream-expand; cap in-flight expanded size.

## J10 — Distributed-op per-host copies — DESIGN-DEBT, 100% verified
`EntityService.java:317-346` (partialUpdateAttribute; siblings verified at 995-1014, 2654-2673, plus
~8 more): per matching registration → `prepareSplitUpEntityForSending` → compacted map →
`JsonUtils.toString` body String → compacted map ALSO captured by the response-handler closure
(`getAttribsFromCompactedPayload(compacted)`, :344) until the HTTP call completes. **Fix:** extract
attrib names pre-send, serialize straight into the request Buffer.

## J11a — Double row materialization — BUG (trivial), 100% verified
`QueryDAO.java:106` and `:110`: `t.iterator().next().getJsonObject(0).getMap()` executed twice for
the same row (the :106 result used only for a size check). **Fix:** local variable.

## J11b — Batch upsert returns old+new — verified NON-ISSUE (justified)
`EntityInfoDAO.java:117-154` (+292-303, 458, 491, 524, 623-628, 648-669) returns OLD_ENTITY
alongside the new. Justification verified end-to-end: old feeds `request.setPrevPayload(...)`
(EntityService:1966→2003, 2134→2145, 2382→2406, 2558→2568/2578/2624), and prevPayload is consumed
by SubscriptionManager for showChanges / attributeDeleted / entityDeleted notifications
(SubscriptionService:903-935, 1046-1070 — spec 5.8.6 notification content). Keep; optimize only by
skipping prev when no subscription needs it.

## J11c — No cursor/streamed reads — DESIGN-DEBT, 100% verified
Repo-wide grep `RowStream|\.cursor(|Cursor `: zero hits. Every DB read materializes the full
RowSet in heap. Long-term: pg `Cursor.read(batch)` for temporal + large query paths.

## J11d — Registry-subscription throttling — BUG + SPEC-VIOLATION, 100% verified
`RegistrySubscriptionService.java:629-638`: `delay = getThrottling() - (now - lastNotification)` —
`getThrottling()` returns **seconds** (spec 5.2.14: "Minimal period of time in seconds which shall
elapse between two consecutive notifications"; table row verified verbatim from the spec PDF p.118)
but is subtracted from a **millisecond** difference with no ×1000 (contrast SubscriptionService
:1704-1705 which multiplies and documents it). Effect: the enforced window is 1000× too short —
throttling=5 yields a 5 ms window, i.e. effectively no throttling ⇒ violates the "shall elapse"
requirement. Secondary (memory): this path DEFERS via `delayIt().by(...)` holding every fully-built
notification + a Vert.x timer for the delay (unbounded count under registry churn), while the
entity-subscription path DROPS — inconsistent semantics. **Fix:** `getThrottling() * 1000L`; prefer
drop-or-coalesce over defer.

---

# Consolidated: what is a spec matter vs. pure implementation

**Spec violations / forgotten implementations (ETSI GS CIM 009 V1.9.1):**
1. Expired **interval** subscriptions keep notifying (L4a-2) — violates 5.8.6.
2. Subscription create/update accepts past `expiresAt`; retrieval shows stale non-`expired` status
   (L4a-3) — violates 5.8.1.x/5.8.2.4 behaviour (BadRequestData + automatic status change).
   RegistrySubscriptionManager implements both; SubscriptionManager forgot them.
3. Temporal queries forward to **expired** csource registrations (L4b, HistoryQueryService) —
   contradicts 5.2.9 (`expiresAt` ⇒ registration invalid).
4. Registry-subscription throttling window is 1000× too short (J11d) — violates 5.2.14.
5. (Correctness, spec-adjacent) B1 wrong csourceid in entity-map reload breaks 5.5.9.3
   entity-map-based distributed pagination; L6 wrong-key lookups break 5.11 remote-subscription
   bookkeeping; R10 shared literal group.id breaks multi-instance sync.

**Verified compliant (no action needed):** change-driven notification gating on expiry
(firstCheckToSendOut), notification failure handling via DB status (5.8.6/5.11.7), local
pagination push-down (5.5.9.2), old-entity return feeding prevPayload/showChanges (5.8.6),
Micrometer metrics.

**Everything else** (R1-R9, L1-L3, L5, L7, L8, U1-U6, J1-J10, J11a/c) is implementation-level:
config gaps, true leaks, unbounded buffers, allocation churn — none mandated or prohibited by the
spec, all safe to fix without compliance risk; U3's bounding fix is explicitly spec-sanctioned by
6.3.10 temporal pagination.

# Fix order (unchanged phases, now verified-safe)
Phase 0 (config): R1-R9. Phase 1 (small code): L1, L3, U1, J2, J6-throttle-hoist, J4-dead-store,
B1, J11a, J11d ×1000 fix, R10. Phase 2 (bounded resources): J1 cache cluster, U3, U4, U2, L5, L6,
L4a/L4b spec fixes. Phase 3 (structural): J5→J4 removal, J3 streaming, L4c shared store, J7, J9.
Verification per CLAUDE.md: build-success grep, ETSI suite via dev/etsi-serial.sh from a torn-down
stack, jcmd heap sampling before/after.
