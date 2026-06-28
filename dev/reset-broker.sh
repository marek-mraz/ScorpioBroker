#!/usr/bin/env bash
# API-based broker reset — the "better truncate".
#
# Deletes all subscriptions, Context Source Registrations and entities via the NGSI-LD API (HTTP),
# NOT raw SQL. Because the deletes go THROUGH the broker, they also evict the broker's in-VM caches
# (entity/subscription/registry maps) and fire the normal delete handling — so NO broker restart is
# needed between measured runs. This fixes the stale-state symptoms that `clean_db.sh`'s raw SQL
# TRUNCATE leaves behind: phantom 409s on create, phantom subscription matches, and federated
# csource leakage. (Same idea as libraries/FederationReset.py, but type-complete via GET /types.)
#
# Usage:  dev/reset-broker.sh [base_url]      default http://scorpio1:9090/ngsi-ld/v1
#         dev/reset-broker.sh --temporal [base_url]   also wipe temporal history (DELETE /temporal/...)
#
# NOTE: an NGSI-LD entity DELETE leaves a temporal tombstone (deletedAt) — correct per spec, but it
# still shows up in /temporal queries. Pass --temporal to also remove temporal history, or run
# clean_db.sh afterwards if you need the temporal tables empty too.
set -uo pipefail

TEMPORAL=
[ "${1:-}" = "--temporal" ] && { TEMPORAL=1; shift; }
BASE="${1:-http://scorpio1:9090/ngsi-ld/v1}"
BASE="${BASE%/}"
CTX="https://forge.etsi.org/rep/cim/ngsi-ld-test-suite/-/raw/develop/resources/jsonld-contexts/ngsi-ld-test-suite-compound.jsonld"
LINK="Link: <$CTX>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\""
# Always bound every call so a slow/federating/overloaded broker can never hang the reset.
CURL=(curl -sS --connect-timeout 5 -m 20)

jget() { python3 -c "import sys,json
try: d=json.load(sys.stdin)
except Exception: sys.exit(0)
print('\n'.join($1))" 2>/dev/null; }

subs=0 regs=0 ents=0

# 1) Subscriptions — listable without a filter.
for id in $("${CURL[@]}" "$BASE/subscriptions?limit=1000" | jget '[s["id"] for s in d if isinstance(s,dict)]'); do
  "${CURL[@]}" -o /dev/null -X DELETE "$BASE/subscriptions/$id" && subs=$((subs+1))
done

# 2) Per type, delete registrations + entities. NGSI-LD list ops need a filter, so we need the type
#    set. GET /types would give it, but it can hang on a federating/overloaded broker, so use a known
#    ETSI superset and merge in /types only if it answers quickly (best-effort, short timeout).
TYPES="Building Vehicle City Country OffStreetParking Bus Feature Car Room Person Streetlight Device NewType RandomType T1 Testing"
extra=$(curl -sS --connect-timeout 3 -m 6 "$BASE/types" -H "$LINK" 2>/dev/null | jget '[t for t in d.get("typeList",[])]')
TYPES=$(printf '%s\n%s\n' "$TYPES" "$extra" | tr ' ' '\n' | grep -v '^$' | sort -u)
for t in $TYPES; do
  # registrations for this type
  for id in $("${CURL[@]}" "$BASE/csourceRegistrations?type=$t&limit=1000" -H "$LINK" | jget '[r["id"] for r in d if isinstance(r,dict)]'); do
    "${CURL[@]}" -o /dev/null -X DELETE "$BASE/csourceRegistrations/$id" && regs=$((regs+1))
  done
  # entities for this type (local only) -> batch delete
  ids=$("${CURL[@]}" "$BASE/entities?type=$t&local=true&limit=1000" -H "$LINK" | jget '[e["id"] for e in d if isinstance(e,dict)]')
  if [ -n "$ids" ]; then
    body=$(printf '%s\n' "$ids" | python3 -c "import sys,json;print(json.dumps([x for x in sys.stdin.read().split() if x]))")
    n=$(printf '%s' "$ids" | grep -c .)
    if [ -n "$TEMPORAL" ]; then
      for id in $ids; do "${CURL[@]}" -o /dev/null -X DELETE "$BASE/temporal/entities/$id"; done
    fi
    "${CURL[@]}" -o /dev/null -X POST "$BASE/entityOperations/delete" -H "Content-Type: application/json" -d "$body" && ents=$((ents+n))
  fi
done

echo "reset $BASE : deleted $subs subscription(s), $regs registration(s), $ents entit(y/ies)${TEMPORAL:+ + temporal history}"
