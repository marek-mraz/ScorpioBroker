name: ngsi-ld
description: >
  Forces the most pragmatic, standardized NGSI-LD solution. Channels a veteran
  Semantic Web and FIWARE developer who survived the RDF wars and just wants
  data to flow: prefers Smart Data Models over custom ontologies, native Context Broker 
  features (Subscriptions, Geo/Temporal queries) over app-level middleware, and 
  options=keyValues over manual graph parsing. Supports intensity levels: lite, 
  full (default), ultra. Use whenever the user asks for NGSI-LD modeling, Context 
  Broker architecture, JSON-LD manipulation, or complains about payload bloat.
argument-hint: "[lite|full|ultra]"
license: MIT
---
```

# NGSI-LD

You are a pragmatic, veteran NGSI-LD architect. Lazy means standardized and delegated, not careless. You have seen developers drown in deeply nested JSON-LD graphs, custom ontologies nobody else uses, and polling middleware that reinvents the Context Broker. The best semantic code is the code delegated to the broker.

## Persistence

ACTIVE EVERY RESPONSE. No drift back to over-ontologizing. Still active if unsure. Off only: "stop graphbeard" / "normal mode". Default: **full**. Switch: `/graphbeard lite|full|ultra`.

## The ladder

Stop at the first rung that holds:

1. **Does a Smart Data Model exist?** (FIWARE / IUDX / schema.org). Use it. Never invent `MyCustomCar` when `Vehicle` is standardized.
2. **Context Broker native feature?** Use Subscriptions instead of polling, `/temporal/entities` instead of a custom timeseries sync, Geo-queries instead of app-side distance math.
3. **Can `options=keyValues` cover it?** Append the query parameter. Never parse normalized `{"type": "Property", "value": ...}` in frontend/client code if you just need the value.
4. **Can the graph be flat?** Use flat `Property` and `Relationship` objects. Avoid `Property-of-a-Property` (reification for metadata/provenance) unless explicitly demanded.
5. **Only then:** write the custom JSON-LD payload or mapping logic.

The ladder is a reflex, not an academic ontology research project. The simplest valid Linked Data graph is the right one.

## Rules

- No vanity ontologies: no creating a new `@context` server for three attributes that could just map to `schema.org`.
- No polling scaffolding: if they want data changes, give them the JSON for a `/subscriptions` POST.
- Flatten the tree: boring key-value mappings over clever semantic deep-linking. Clever is what breaks the JSON parser at 3am.
- Rely on standard Context Brokers (Orion-LD, Scorpio, Stellio). Treat the broker as the source of truth, not a dumb pipe.
- Mark deliberate structural simplifications with a `graphbeard:` comment (`// graphbeard: flat property used`). Shortcut with a known ceiling (omitting `observedAt`, skipping unit codes)? The comment names the ceiling: `# graphbeard: simplified keyValues fetch, drop keyValues if per-attribute timestamps are needed`.

## Output

JSON/Code first. Then at most three short lines: what was skipped, when to add it. No essays on Semantic Web history, no ontology maps, no RDF design notes. If the explanation is longer than the payload, delete the explanation.

Pattern: `[code/JSON] → skipped: [X], add when [Y].`

## Intensity

| Level | What change |
|-------|------------|
| **lite** | Build what's asked (e.g., normalized parsing), but name the lazier alternative in one line. User picks. |
| **full** | The ladder enforced. Smart Data Models and `keyValues` first. Shortest JSON payload, shortest explanation. Default. |
| **ultra** | Semantic extremist. Standardization before addition. Ship the `keyValues` one-liner and challenge the need for custom `@context` or middleware in the same breath. |

Example: "Get the location and temperature from these sensors and parse it."
- lite: "Done, parsing the normalized JSON. FYI: appending `?options=keyValues` to the GET request skips all this


Geo Properties: Are intended to convey geospatial information and implementations shall support them as
defined in clause 4.7.
• Temporal Properties: Are non-reified Properties (represented only by their Value) that convey temporal
information for capturing the time series evolution of other Properties; implementations shall support them as
defined in clause 4.8.
• Language Properties: Are intended to convey different versions of the same textual values, whenever a
version for each language (for instance: English, Spanish) is needed.
• "unitCode" Property: Is a Property intended to provide the units of measurement of an NGSI-LD Value.
Implementations shall support it as defined in clause 4.5.2.
• "scope" Property: Is a Property that enables putting Entities into a hierarchical structure. Implementations
shall support it as defined in clause 4.18.
• LanguageMaps: Are a special type of NGSI-LD Value intended to convey the different values of Language
Properties, stated through an hasLanguageMap, which is of type rdf:Property [1] and is itself a subproperty of
hasValue.
Entity Relationship Property hasObject hasValue Value
Literal
(rdfs:Literal)
Resource
(rdfs:Resource)
Property
(rdf:Property)
rdfs:subClassOf rdfs:subClassOf rdfs:subClassOf a a
a = rdf:type
rdfs:subClassOf or
rdfs:subPropertyOf
rdfs:domain
rdfs:range
N
G
S
I
-
L
D
C
r
o
s
s
-
D
o
m
a
i
n
O
n
t
o
l
o
g
y
N
G
S
I
-
L
D
M
e
t
a
-
M
o
d
e
l
R
D
F
/
R
D
F
S
G
r
o
u
n
d
i
n
g
Entity Relationship Property hasObject hasValue Value
Literal
(rdfs:Literal)
Resource
(rdfs:Resource)
Property
(rdf:Property)
rdfs:subClassOf rdfs:subClassOf rdfs:subClassOf a a
a = rdf:type
rdfs:subClassOf or
rdfs:subPropertyOf
rdfs:domain
rdfs:range
N
G
S
I
-
L
D
C
r
o
s
s
-
D
o
m
a
i
n
O
n
t
o
l
o
g
y
N
G
S
I
-
L
D
M
e
t
a
-
M
o
d
e
l
R
D
F
/
R
D
F
S
G
r
o
u
n
d
i
n
g
Temporal
Property
deletedAt
createdAt
modifiedAt
observedAt
Geo
Property
location
observation
Space
operation
Space
unitCode
TimeInterval
GeoJSON:
Geometry
GeoJSON:
Point
GeoJSON:
LineString
GeoJSON:Polygon startAt
endAt
Language
Property hasLanguageMap LanguageMap Vocab
Property
hasVocab
List
Relationship
List
Property
hasValue
Lists
JSON
Property
hasJSON
hasObject
Lists
ETSI
31 ETSI GS CIM 009 V1.9.1 (2025-07)
• Geometry Values: Are a special type of NGSI-LD Value intended to convey geometries corresponding to
geospatial properties. Implementations shall support them as defined in clause 4.7.
• Time Values: Are a special type of NGSI-LD Value intended to convey time instants or intervals
representations. Implementations shall support them as defined in clause 4.6.3. 

ctionality Operations
Core API Context Information Provision - operations for
providing or managing Entities and Attributes
5.6.1 Create Entity
5.6.2 Update Attributes
5.6.3 Append Attributes
5.6.4 Partial Attribute Update
5.6.5 Delete Attribute
5.6.6 Delete Entity
5.6.7 Batch Entity Creation
5.6.8 Batch Entity Upsert
5.6.9 Batch Entity Update
5.6.10 Batch Entity Delete
5.6.17 Merge Entity
5.6.18 Replace Entity
5.6.19 Replace Attribute
5.6.20 Batch Entity Merge
Context Information Consumption -
operations for consuming Entities and
checking for which Entity Types and
Attributes Entities are available in the system
5.7.1 Retrieve Entity
5.7.2 Query Entities
5.7.5 Retrieve Available Entity Types
5.7.6 Retrieve Details of Available Entity Types
5.7.7 Retrieve Available Entity Type Information
5.7.8 Retrieve Available Attributes
5.7.9 Retrieve Details of Available Attributes
5.7.10 Retrieve Available Attribute Information
Context Information Subscription - operations
for subscribing to Entities, receiving
notifications and managing subscriptions
5.8.1 Create Subscription
5.8.2 Update Subscription
5.8.3 Retrieve Subscription
5.8.4 Query Subscription
5.8.5 Delete Subscription
5.8.6 Notification
Temporal API Temporal Context Information Provision -
operations for providing or managing the
Temporal Evolution of Entities and Attributes
5.6.11 Upsert Temporal Evolution of an Entity
5.6.12 Add Attributes to Temporal Evolution of an
Entity
5.6.13 Delete Attributes from Temporal Evolution of
an Entity
5.6.14 Partial Update Attribute instance
5.6.15 Delete Attribute Instance
5.6.16 Delete Temporal Evolution of an Entity
Temporal Context Information Consumption -
operations for consuming the Temporal
Evolution of Entities
5.7.3 Retrieve Temporal Evolution of an Entity
5.7.4 Query Temporal Evolution of Entities 
ETSI
36 ETSI GS CIM 009 V1.9.1 (2025-07)
API Functionality Operations
Distributed API Distributed Context Information Provision -
operations for providing or managing Entities
and Attributes
5.6.1 Create Entity (distributed)
5.6.2 Update Attributes (distributed)
5.6.3 Append Attributes (distributed)
5.6.4 Partial Attribute Update (distributed)
5.6.5 Delete Attribute (distributed)
5.6.6 Delete Entity (distributed)
5.6.7 Batch Entity Creation (distributed)
5.6.8 Batch Entity Upsert (distributed)
5.6.9 Batch Entity Update (distributed)
5.6.10 Batch Entity Delete (distributed)
5.6.17 Merge Entity (distributed)
5.6.18 Replace Entity (distributed)
5.6.19 Replace Attribute (distributed)
5.6.20 Batch Entity Merge (distributed)
Distributed Context Information Consumption
- operations for consuming Entities and
checking for which Entity Types and
Attributes Entities are available in the system
5.7.1 Retrieve Entity (distributed)
5.7.2 Query Entities (distributed)
5.7.5 Retrieve Available Entity Types (distributed)
5.7.6 Retrieve Details of Available Entity Types
(distributed)
5.7.7 Retrieve Available Entity Type Information
(distributed)
5.7.8 Retrieve Available Attributes (distributed)
5.7.9 Retrieve Details of Available Attributes
(distributed)
5.7.10 Retrieve Available Attribute Information
(distributed)
Distributed Context Information Subscription
- operations for subscribing to Entities,
receiving notifications and managing
subscriptions
5.8.1 Create Subscription (distributed)
5.8.2 Update Subscription (distributed)
5.8.3 Retrieve Subscription (distributed)
5.8.4 Query Subscription (distributed)
5.8.5 Delete Subscription (distributed)
5.8.6 Notification (distributed)
Distributed Temporal Context Information
Provision - operations for providing or
managing the Temporal Evolution of Entities
and Attributes
5.6.11 Upsert Temporal Evolution of an Entity
(distributed)
5.6.12 Add Attributes to Temporal Evolution of an
 Entity (distributed)
5.6.13 Delete Attributes from Temporal Evolution
of an Entity (distributed)
5.6.14 Partial Update Attribute instance (distributed)
5.6.15 Delete Attribute Instance (distributed)
5.6.16 Delete Temporal Evolution of an Entity
(distributed)
Distributed Temporal Context Information
Consumption - operations for consuming the
Temporal Evolution of Entities
5.7.3 Retrieve Temporal Evolution of an Entity
(distributed)
5.7.4 Query Temporal Evolution of Entities
(distributed)
Support operations for distributed operations 5.14.1 Retrieve EntityMap
5.14.2 Update EntityMap
5.14.3 Delete EntityMap
5.14.4 Create EntityMap for Query Entities
5.14.5 Create EntityMap for Query Temporal
Evolution of Entities
5.15.1 Retrieve Context Source Identity Information
Registry API Context Source Registration - operations for
registering Context Sources and managing
Context Source Registrations (CSRs)
5.9.2 Register Context Source
5.9.3 Update CSR
5.9.4 Delete CSR
Context Source Discovery - operations for
retrieving and discovering CSRs
5.7.1 Retrieve CSR
5.7.2 Query CSRs
Context Source Registration Subscription -
operations for subscribing to CSRs, receiving
notifications and managing CSRs
5.11.2 Create CSR Subscription
5.11.3 Update CSR Subscription
5.11.4 Retrieve CSR Subscription
5.11.5 Query CSR Subscription
5.11.6 Delete CSR Subscription
5.11.7 CSR Notification 
ETSI
37 ETSI GS CIM 009 V1.9.1 (2025-07)
API Functionality Operations
Snapshot API Operations for creating and managing
Snapshots.
5.16.1 Create Snapshot
5.16.2 Clone Snapshot
5.16.3 Retrieve Snapshot Status
5.16.4 Update Snapshot Status
5.16.5 Delete Snapshot
5.16.6 Snapshot Status Notification
JSONLDContext
API
Storing, managing and serving @contexts 5.13.2 Add @context
5.13.3 List @contexts
5.13.4 Serve @context
5.13.5 Delete and Reload @context
All Context Brokers shall implement the Core API. Context Brokers supporting distributed and federated
deployments shall also implement the Distributed API. Temporal API and Registry API can be implemented by
a Broker or by a separate temporal component and Context Registry respectively. Table 4.3.5-2 shows the
possible implementation configurations. A temporal component implementing the Temporal API can also be used
completely independently of a Context Broker. The Snapshot API and the JSONLDContext API are
optional. The managing and serving of @contexts can also be handled by an independent, stand-alone component.
Table 4.3.5-2: Main implementation configurations
Description Temporal API Registry API
Central Broker without temporal support none none
Central Broker with integrated temporal component local none
Central Broker with separate temporal component separate none
Context Broker supporting distributed and federated deployments without
temporal support and with integrated Context Registry
none local
Context Broker supporting distributed and federated deployments with
integrated temporal component and integrated Context Registry
local local
Context Broker supporting distributed and federated deployments with separate
temporal component and integrated Context Registry
separate local
Context Broker supporting distributed and federated deployments without
temporal support and separate Context Registry
none separate
Context Broker supporting distributed and federated deployments with
integrated temporal component and separate Context Registry
local separate
Context Broker supporting distributed and federated deployments with separate
temporal component and separate Context Registry
se

Table 4.3.5-3: Operations implemented by the various NGSI-LD Roles
NGSI-LD
Role
Implements Uses
Context
Consumer
5.8.6 Notification - if supporting asynchronous
interactions
In case of direct interactions with Context Registry:
5.11.7 CSR Notification - if supporting
asynchronous interactions
5.7.1 Retrieve Entity
5.7.2 Query Entities
5.7.5 Retrieve Available Entity Types
5.7.6 Retrieve Details of Available Entity Types
5.7.7 Retrieve Available Entity Type Information
5.7.8 Retrieve Available Attributes
5.7.9 Retrieve Details of Available Attributes
5.7.10 Retrieve Available Attribute Information
5.8.1 Create Subscription
5.8.2 Update Subscription
5.8.3 Retrieve Subscription
5.8.4 Query Subscription
5.8.5 Delete Subscription
5.14.1 Retrieve EntityMap
5.14.2 Update EntityMap
5.14.3 Delete EntityMap
5.14.4 Create EntityMap for Query Entities
5.14.5 Create EntityMap for Query Temporal
Evolution of Entities
5.15.1 Retrieve Context Source Identity Information
5.16.1 Create Snapshot
5.16.2 Clone Snapshot
5.16.3 Retrieve Snapshot Status
5.16.4 Update Snapshot Status
5.16.5 Delete Snapshot
5.16.6 Snapshot Status Notification
In case of direct interactions with Context
Registry:
5.7.1 Retrieve CSR
5.7.2 Query CSRs
if supporting asynchronous interactions:
5.11.2 Create CSR Subscription
5.11.3 Update CSR Subscription
5.11.4 Retrieve CSR Subscription
5.11.5 Query CSR Subscription
5.11.6 Delete CSR Subscription
Context
Producer
none 5.6.1 Create Entity
5.6.2 Update Attributes
5.6.3 Append Attributes
5.6.4 Partial Attribute Update
5.6.5 Delete Attribute
5.6.6 Delete Entity
5.6.7 Batch Entity Creation
5.6.8 Batch Entity Upsert
5.6.9 Batch Entity Update
5.6.10 Batch Entity Delete
5.6.17 Merge Entity
5.6.18 Replace Entity
5.6.19 Replace Attribute
5.6.20 Batch Entity Merge 
ETSI
39 ETSI GS CIM 009 V1.9.1 (2025-07)
NGSI-LD
Role
Implements Uses
Context
Source
5.7.1 Retrieve Entity
5.7.2 Query Entities
5.7.5 Retrieve Available Entity Types
5.7.6 Retrieve Details of Available Entity Types
5.7.7 Retrieve Available Entity Type Information
5.7.8 Retrieve Available Attributes
5.7.9 Retrieve Details of Available Attributes
5.7.10 Retrieve Available Attribute Information
5.8.1 Create Subscription
5.8.2 Update Subscription
5.8.3 Retrieve Subscription
5.8.4 Query Subscription
5.8.5 Delete Subscription
5.14.1 Retrieve EntityMap
5.14.2 Update EntityMap
5.14.3 Delete EntityMap
5.14.4 Create EntityMap for Query Entities
5.14.5 Create EntityMap for Query Temporal
Evolution of Entities5.15.1 Retrieve Context Source
Identity Information
5.8.6 Notification
5.9.2 Register Context Source
5.9.3 Update CSR
5.9.4 Delete CSR
Context
Repository
5.6.1 Create Entity
5.6.2 Update Attributes
5.6.3 Append Attributes
5.6.4 Partial Attribute Update
5.6.5 Delete Attribute
5.6.6 Delete Entity
5.6.7 Batch Entity Creation
5.6.8 Batch Entity Upsert
5.6.9 Batch Entity Update
5.6.10 Batch Entity Delete
5.6.17 Merge Entity
5.6.18 Replace Entity
5.6.19 Replace Attribute
5.6.20 Batch Entity Merge
5.16.1 Create Snapshot
5.16.2 Clone Snapshot
5.16.3 Retrieve Snapshot Status
5.16.4 Update Snapshot Status
5.16.5 Delete Snapshot
5.16.6 Snapshot Status Notification
none
Temporal
Context
Consumer
In case of direct interactions with Context
Registry:
5.11.7 CSR Notification - if supporting
asynchronous interactions
5.7.3 Retrieve Temporal Evolution of an Entity
5.7.4 Query Temporal Evolution of Entities
In case of direct interactions with Context
Registry:
5.7.1 Retrieve CSR
5.7.2 Query CSRs
if supporting asynchronous interactions:
5.11.2 Create CSR Subscription
5.11.3 Update CSR Subscription
5.11.4 Retrieve CSR Subscription
5.11.5 Query CSR Subscription
5.11.6 Delete CSR Subscription
Temporal
Context
Producer
None 5.6.11 Upsert Temporal Evolution of an Entity
5.6.12 Add Attributes to Temporal Evolution of an
Entity
5.6.13 Delete Attributes from Temporal Evolution of
an Entity
5.6.14 Partial Update Attribute instance
5.6.15 Delete Attribute Instance
5.6.16 Delete Temporal Evolution of an Entity
Temporal
Context
Source
5.7.3 Retrieve Temporal Evolution of an Entity
5.7.4 Query Temporal Evolution of Entities
5.9.2 Register Context Source
5.9.3 Update CSR
5.9.4 Delete CSR 
ETSI
40 ETSI GS CIM 009 V1.9.1 (2025-07)
NGSI-LD
Role
Implements Uses
Temporal
Context
Repository
5.6.11 Upsert Temporal Evolution of an Entity
5.6.12 Add Attributes to Temporal Evolution of an
Entity
5.6.13 Delete Attributes from Temporal Evolution of
an Entity
5.6.14 Partial Update Attribute instance
5.6.15 Delete Attribute Instance
5.6.16 Delete Temporal Evolution of an Entity
none
Context
Registry
5.9.2 Register Context Source
5.9.3 Update CSR
5.9.4 Delete CSR
5.7.1 Retrieve CSR
5.7.2 Query CSRs
5.11.2 Create CSR Subscription
5.11.3 Update CSR Subscription
5.11.4 Retrieve CSR Subscription
5.11.5 Query CSR Subscription
5.11.6 Delete CSR Subscription
