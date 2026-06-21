# Security Vulnerability Lookup — Reusable Queries & Prompt

Reference for the next deep security audit of the Scorpio NGSI-LD broker.
Re-run these to re-find (or regression-check) the known vulnerability classes.
All queries are **read-only** except the explicitly-marked SQLite tracking DB and the live PoCs.

> Known root cause: query terms / values that cannot be a `$n` bind (SQL identifiers,
> GeoJSON literals, scope regexes, NOTIFY payloads, cross-tenant `SELECT '<tenant>'`) are
> built by **raw string concatenation**. Operands, attribute paths, `type`, `id`, `idPattern`,
> `timeproperty`, `georel`, `aggrMethods` are correctly bound — verify they stay that way.

---

## 0. Reusable prompt (paste to an AI assistant next time)

```
Do a deep security audit of this NGSI-LD broker. Read-only on source; you may run
non-destructive live PoCs against a dev broker on :9090 and clean up after.
Track file coverage + findings in a SQLite DB at ./secaudit.db (schema in
docs/SECURITY_AUDIT_QUERIES.md). Focus on SQL injection in query-term -> SQL builders
(collation, scopeQ, geometry/coordinates, ORDER BY, NOTIFY, cross-tenant SELECT),
SSRF in @context/document loading, and error-message info disclosure. Confirm operand
values stay parameterized via Vert.x Tuple ($n). Report file:line, severity, confidence,
and a live-tested PoC for each finding.
```

---

## 1. SQL injection sinks — grep queries

```bash
# Raw concatenation of a variable into SQL string literals (the core bug pattern)
grep -rnE "\" *\+ *[a-z]|\+ *\"|append\([a-z][A-Za-z0-9_.]*\)" --include=*.java \
  Commons/src/main/java/eu/neclab/ngsildbroker/commons/datatypes/terms/ \
  | grep -viE "dollar|tuple|\\\$|//|logger|\.length|StringBuilder\("

# DAO/query string building with concatenation
grep -rnE "executeQuery\([^,]+,\s*\"[^\"]*\"\s*\+|\"SELECT |\"INSERT |\"UPDATE |\"DELETE |create database|create schema|NOTIFY |pg_notify" \
  --include=*.java . | grep -v /target/ | grep -iE "\" *\+ *[a-z]" \
  | grep -viE "DBConstants|DBTABLE|DBCOLUMN|NGSIConstants|AppConstants|UUID|hashCode"

# COLLATE / ORDER BY identifier injection
grep -rnE "COLLATE|ORDER BY|orderDirection|collation" --include=*.java Commons QueryManager | grep -v /target/

# Scope query literal building (matchScope)
grep -rn "matchScope\|getSQLScopeQuery\|matchscope" --include=*.java . | grep -v /target/

# GeoJSON literal building (geometry/coordinates concatenated)
grep -rn "ST_GeomFromGeoJSON\|referenceValue\|coordinates +\|geometry +" --include=*.java Commons | grep -v /target/

# Tenant -> SQL (schema/db name/second-order)
grep -rnE "search_path|create database|create schema|\"ngb\" *\+|getTenant\(\) *\+|\+ *tenant|SELECT '\" *\+" \
  --include=*.java . | grep -v /target/
```

### Confirm the SAFE paths are still safe (regression)
```bash
# These MUST stay bound via tuple.add* / $n :
grep -rn "tuple.addString\|tuple.addArrayOfString\|::jsonpath" --include=*.java Commons QueryManager | grep -v /target/
# Whitelists that must remain enforced:
grep -rn "ALLOWED_AGGR_METH\|ALLOWED_TIME_PROPERTIES\|GEO_REL_NEAR" --include=*.java Commons | grep -v /target/
```

## 2. SSRF — grep queries
```bash
grep -rnE "loadDocument|fromURL|openConnection|openStream|webClient\.(get|getAbs|post|patchAbs)|new URI\(.*toURL" \
  --include=*.java . | grep -v /target/
# Check for any allow-list / internal-IP guard (currently absent = SSRF):
grep -rniE "169.254|metadata|private.*address|isLoopback|isSiteLocal|allowlist|blocklist|DISALLOW_REMOTE" \
  --include=*.java . | grep -v /target/
```

## 3. Info disclosure / other classes
```bash
# Raw error messages returned to clients
grep -rnE "getErrorMessage\(\)|getMessage\(\)\)|printStackTrace" --include=*.java \
  Commons/src/main/java/eu/neclab/ngsildbroker/commons/tools/HttpUtils.java
# Negative-result sweeps (should stay empty):
grep -rnE "ObjectInputStream|enableDefaultTyping|@JsonTypeInfo|Runtime.getRuntime|ProcessBuilder|ScriptEngine|Class.forName" \
  --include=*.java . | grep -v /target/
# Config secrets (expect only dev defaults ngb/guest + placeholders):
grep -rniE "password|secret|token|access_key" --include=*.yml --include=*.yaml --include=*.properties . | grep -v /target/
```

---

## 4. Live PoC templates (broker on :9090, non-destructive)

```bash
BASE=http://localhost:9090/ngsi-ld/v1
# #1 collation SQLi
curl -s "$BASE/entities?type=Room&orderBy=id&collation=C%22zzz"        # -> "syntax error at or near zzz"
# #2 scopeQ SQLi
curl -s "$BASE/entities?type=Room&scopeQ=%2Fx%27zzz"                    # -> "syntax error ... zzz$"
# #3 geometry SQLi
curl -s "$BASE/entities?type=Room&georel=within&geometry=Point%27qqq&coordinates=%5B0,0%5D"
# #9 second-order tenant (storage half; clean up after!)
curl -s -X POST "$BASE/entities" -H "Content-Type: application/ld+json" -H "NGSILD-Tenant: evil'zzz" \
  -d '{"id":"urn:ngsi-ld:Sec:1","type":"Room","@context":["https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context-v1.8.jsonld"]}'
#   then: SELECT tenant_id FROM tenant;  (raw quote stored) -- DELETE it + DROP its ngb<hash> DB afterwards
# #5 SSRF (start a canary listener on :8099 first)
curl -s "$BASE/jsonldContexts/createcache/$(python3 -c 'import urllib.parse;print(urllib.parse.quote("http://127.0.0.1:8099/x",safe=""))')"
curl -s -X POST "$BASE/entities" -H "Content-Type: application/ld+json" \
  -d '{"id":"urn:ngsi-ld:S:2","type":"Thing","@context":["http://127.0.0.1:8099/y"]}'
```

---

## 5. SQLite audit DB (coverage + findings tracking)

```bash
# Schema (create ./secaudit.db):
python3 - <<'PY'
import sqlite3
c=sqlite3.connect("secaudit.db")
c.executescript("""
CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY, path TEXT UNIQUE, module TEXT, ext TEXT,
  size_bytes INTEGER, visited INTEGER DEFAULT 0, risk TEXT DEFAULT 'unrated', notes TEXT DEFAULT '');
CREATE TABLE IF NOT EXISTS findings(id INTEGER PRIMARY KEY, file TEXT, line INTEGER, severity TEXT,
  category TEXT, confidence INTEGER, description TEXT, tested TEXT DEFAULT 'no', status TEXT DEFAULT 'open');
""")
c.commit()
PY

# Populate file inventory (run from repo root):
python3 - <<'PY'
import sqlite3,os
c=sqlite3.connect("secaudit.db");cur=c.cursor()
exts=(".java",".properties",".yml",".yaml",".xml")
for dp,dn,fn in os.walk("."):
    if "/target" in dp or "/.git" in dp: continue
    dn[:]=[d for d in dn if d not in ("target",".git")]
    for f in fn:
        if f.endswith(exts):
            rel=os.path.relpath(os.path.join(dp,f),".")
            cur.execute("INSERT OR IGNORE INTO files(path,module,ext,size_bytes) VALUES(?,?,?,?)",
                (rel, rel.split(os.sep)[0], f.rsplit('.',1)[-1], os.path.getsize(os.path.join(dp,f))))
c.commit();print("files:",cur.execute("SELECT COUNT(*) FROM files").fetchone()[0])
PY
```

```sql
-- visited: 0=untouched 1=pattern-swept 2=deep-read ; review status:
SELECT severity,category,confidence,status,tested,file FROM findings
 ORDER BY CASE severity WHEN 'HIGH' THEN 0 WHEN 'MEDIUM' THEN 1 WHEN 'LOW' THEN 2 ELSE 3 END;
SELECT visited,COUNT(*) FROM files WHERE ext='java' GROUP BY visited;
SELECT module,COUNT(*) FROM files WHERE visited=0 AND ext='java' GROUP BY module ORDER BY 2 DESC;
```

---

## 6. Known findings baseline (regression checklist)

| # | Sev | Class | Location |
|---|-----|-------|----------|
| 1 | HIGH | SQLi `collation` | `Commons/.../terms/OrderByTerm.java:434` |
| 2 | HIGH | SQLi `scopeQ` | `Commons/.../terms/ScopeQueryTerm.java:114` |
| 3 | HIGH | SQLi `geometry`/`coordinates` | `Commons/.../terms/GeoQueryTerm.java:338,391` |
| 4 | HIGH | SQLi subscription `scopeQ` | `SubscriptionManager/.../repository/SubscriptionInfoDAO.java:148` |
| 5 | HIGH | SSRF `@context`/createcache | `AtContextServer ContextController.java:161`, `Commons/.../core/DocumentLoader.java:72` |
| 9 | HIGH | 2nd-order SQLi `NGSILD-Tenant` | `SubscriptionInfoDAO.java:266`, `RegistrySubscriptionInfoDAO.java:163` |
| 11 | HIGH | SQLi NOTIFY (SQS profile) | `SubscriptionSyncSQS.java:97`, `RegistrySubscriptionSyncSQS.java:88` |
| 10 | MED | Info disclosure (DB errors) | `Commons/.../tools/HttpUtils.java:390,395` |

Until fixed, each of the above should still reproduce. After a fix, the matching PoC in §4 must return a normal response (no SQL error, no canary hit, generic error text).
