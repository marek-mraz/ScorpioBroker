.3 NGSI-LD Architectural Considerations
4.3.1 Introduction
The NGSI-LD API is intended to be primarily an API and does not define a specific architecture. It is envisioned that
the NGSI-LD API can be used in different architectural settings and the architectural assumptions of the API are kept to
a minimum.
As it is not possible to elaborate all possible architectures in which the NGSI-LD API could be used, three prototypical
architectures are presented. The NGSI-LD API shall enable efficient support for all of them, i.e. the design decisions for
the NGSI-LD API take these prototypical architectures into consideration. A real system architecture utilizing the
NGSI-LD API can map to one, take elements from multiple or combine all of the prototypical architectures.
The NGSI-LD API implicitly defines two sets of Entities:
• the "current state";
• the "temporal evolution" (both the past and possibly future predictions).
The NGSI-LD API is structured into a Core API and an optional Temporal API. The Core current state of Entities. The Temporal API is optional and manages the Temporal Evolution API manages the
of
Entities. Brokers that intend to implement the Temporal API should consider updating the Temporal
Evolution of an Entity whenever the "current state" is modified via the Core API.
ETSI
33
ETSI GS CIM 009 V1.9.1 (2025-07)
4.3.2 Centralized architecture
Figure 4.3.2-1 shows a centralized architecture. In the centre is a Central Broker that stores all the context
information. There are Context Producers that use update operations to update the context information in the
Central Broker and there are Context Consumers that request context information from the Central
Broker, either using synchronous one-time query or asynchronous subscribe/notify operations. The Central
Broker answers all requests from its storage. Figure 4.3.2-1 shows one component that acts as both Context
Producer and Context Consumer. The general assumption is that components can have multiple roles, so such
components are not explicitly shown in clauses 4.3.3 and 4.3.4.
Figure 4.3.2-1: Centralized architecture
4.3.3 Distributed architecture
Figure 4.3.3-1 shows a distributed architecture. The underlying idea here is that all information is stored by the
Context Sources. Context Sources implement the query and subscription part of the NGSI-LD API as a
Context Broker does. They register themselves with the Context Registry, providing information about
what context information they can provide, but not the context information itself, e.g. a certain Context Source
registers that it can provide the indoor temperature for Building A and Building B or that it can provide the speed of
cars in a geographic region covering the centre of a city.
Figure 4.3.3-1: Distributed architecture
ETSI
34
ETSI GS CIM 009 V1.9.1 (2025-07)
Context Consumers can query or subscribe to the Distribution Broker. On each request, the
Distribution Broker discovers or does a discovery subscription to the Context Registry for relevant
Context Sources, i.e. those that may provide context information relevant to the respective request from the
Context Consumer. The Distribution Broker then queries or subscribes to each relevant Context
Source, if possible it aggregates the context information retrieved from the Context Sources and provides them
to the Context Consumer. In this mode of operation, it is not visible to the Context Consumer, whether the
Context Broker is a Central Broker or a Distribution Broker. Alternatively, the architecture allows
that Context Consumers can discover Context Sources through the Context Registry themselves and
then directly request from Context Sources. This is shown in Figure 4.3.3-1 with the fine dashed arrows.
4.3.4 Federated architecture
The federated architecture shown in Figure 4.3.4-1 is used in cases where existing domains are to be federated. For
example, different departments in a city operate their own Context Broker-based NGSI-LD infrastructure, but
applications should be able to easily access all available information using just one point of access. The architecture
works in the same way as the distributed architecture described in clause 4.3.3, except that instead of simple Context
Sources, whole domains are registered with the respective Context Broker as point of access. Typically, the
domains will be registered to the federation Context Registry on a more coarse-grained level, providing scopes,
in particular geographic scopes, that can then be matched to the scopes provided in the requests. For example, instead of
registering individual entities like buildings, the domain would be registered with having information about entities of
type building within a geographic area. Applications then query or subscribe for entities within a geographic scope,
e.g. buildings in a certain area of the city. The Federation Broker discovers the domain Context Brokers
that can provide relevant information, forwards the request to these Context Brokers and aggregates the results, so
the application gets the result in the same way as in the centralized and distributed cases.
Figure 4.3.4-1: Federated architecture
A domain itself can use a centralized or distributed architecture or could even utilize a federated architecture that
federates sub-domains.
As in the distributed case, it is also possible that applications discover relevant domains through the federation-level
Context Registry and directly contact the Context Brokers in the individual domains.
ETSI
35
ETSI GS CIM 009 V1.9.1 (2025-07)
4.3.5 NGSI-LD API Structure and Implementation Options
As stated in clause 4.3.1, the NGSI-LD API is structured into a Core API and an optional Temporal API. To support the
distributed and federated architectures described in clauses 4.3.3 and 4.3.4 respectively, distributed versions of the
operations are needed. They are listed separately under Distributed API and require the operations of the Registry API.
The Registry API consists of the operations to be implemented by the Context Registry. Furthermore, the
JSONLDContext API provides functionality for storing, managing, and serving JSON-LD @contexts. The APIs are
structured according to their functionalities, which is also reflected in how the operations are structured in clause 5.
Table 4.3.5-1 introduces the API structure, the respective functionalities and lists the operations for each functionality,
pointing to the clauses in which they are defined. The distributed versions of the operations are separately shown in
Table 4.3.5-1 under Distributed API, but there is a single clause for each of the operations describing both the
centralized and distributed behaviour. In addition, the Distributed API has support operations only needed in distributed
and federated architectures.
Table 4.3.5-1: NGSI-LD API structure
API Functionality Operations
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
36
API Functionality Distributed API Distributed Context Information Provision -
operations for providing or managing Entities
and Attributes
Distributed Context Information Consumption
- operations for consuming Entities and
checking for which Entity Types and
Attributes Entities are available in the system
Distributed Context Information Subscription
- operations for subscribing to Entities,
receiving notifications and managing
subscriptions
Distributed Temporal Context Information
Provision - operations for providing or
managing the Temporal Evolution of Entities
and Attributes
Distributed Temporal Context Information
Consumption - operations for consuming the
Temporal Evolution of Entities
Support operations for distributed operations Registry API Context Source Registration - operations for
registering Context Sources and managing
Context Source Registrations (CSRs)
Context Source Discovery - operations for
retrieving and discovering CSRs
Context Source Registration Subscription -
operations for subscribing to CSRs, receiving
notifications and managing CSRs
ETSI
ETSI GS CIM 009 V1.9.1 (2025-07)
Operations
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
5.8.1 Create Subscription (distributed)
5.8.2 Update Subscription (distributed)
5.8.3 Retrieve Subscription (distributed)
5.8.4 Query Subscription (distributed)
5.8.5 Delete Subscription (distributed)
5.8.6 Notification (distributed)
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
5.7.3 Retrieve Temporal Evolution of an Entity
(distributed)
5.7.4 Query Temporal Evolution of Entities
(distributed)
5.14.1 Retrieve EntityMap
5.14.2 Update EntityMap
5.14.3 Delete EntityMap
5.14.4 Create EntityMap for Query Entities
5.14.5 Create EntityMap for Query Temporal
Evolution of Entities
5.15.1 Retrieve Context Source Identity Information
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
5.11.7 CSR Notification
37
ETSI GS CIM 009 V1.9.1 (2025-07)
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
separate separate
ETSI
38
ETSI GS CIM 009 V1.9.1 (2025-07)
Table 4.3.5-3 shows which operations are implemented and used by the other architectural roles as introduced in
clause 4.3.2, clause 4.3.3 and clause 4.3.4. In addition, there are separate roles for the Temporal API, i.e. Temporal
Context Producer, Temporal Context Source and Temporal Context Consumer. For completeness, the
roles of Context Repository and Temporal Context Repository have been introduced, implementing the Context
Information Provision and Temporal Context Information Provision functionalities, respectively. In practice,
components implementing the latter roles will also implement functionalities for consuming or processing the stored
information. Actual components can have multiple roles at the same time, e.g. a Context Broker can implement all
roles at the same time. Context Consumers typically only interact with Context Brokers, but in alternative
setups, as shown in Figure 4.3.3-1, they can also directly interact with the Context Registry and then directly
contact Context Sources.
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
NGSI-LD
Role
Context
Source
Context
Repository
Temporal
Context
Consumer
Temporal
Context
Producer
Temporal
Context
Source
39
Implements 5.7.1 Retrieve Entity
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
In case of direct interactions with Context
Registry:
5.11.7 CSR Notification - if supporting
asynchronous interactions
None 5.7.3 Retrieve Temporal Evolution of an Entity
5.7.4 Query Temporal Evolution of Entities
ETSI
ETSI GS CIM 009 V1.9.1 (2025-07)
Uses
5.8.6 Notification
5.9.2 Register Context Source
5.9.3 Update CSR
5.9.4 Delete CSR
none
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
5.6.11 Upsert Temporal Evolution of an Entity
5.6.12 Add Attributes to Temporal Evolution of an
Entity
5.6.13 Delete Attributes from Temporal Evolution of
an Entity
5.6.14 Partial Update Attribute instance
5.6.15 Delete Attribute Instance
5.6.16 Delete Temporal Evolution of an Entity
5.9.2 Register Context Source
5.9.3 Update CSR
5.9.4 Delete CSR
40
ETSI GS CIM 009 V1.9.1 (2025-07)
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
5.11.7 CSR Notification
4.3.6 Distributed Operations
4.3.6.1 Introduction
One fundamental concept underpinning all of the prototypical architectures described above (clauses 4.3.2, 4.3.3 and
4.3.4) is the idea that Entity data does not need to be centralized within a single Context Broker. When reading
context information, a Context Broker can be used as a single point of access to retrieve Entity data found
distributed across multiple associated Context Brokers each receiving a context consumption request. Similarly,
when modifying an Entity, a single request to a Context Broker may result in the operation being distributed and
different parts of that Entity being updated across multiple Context Brokers each receiving a context provision
request.
As long as there is only a centralized Context Broker, i.e. there are no Context Sources registered, all NGSI-
LD requests, with few exceptions such as Update Attributes (see clause 5.6.2) and the batch operations (see clauses
5.6.7, 5.6.8, 5.6.9, 5.6.10 and 5.6.20), can either be successfully executed completely, or result in an error. In the
distributed case, all requests can be partially successful. For the centralized case described above, only specific
operations, such as Update Attributes and the batch operations, can be partially successful.
It is the responsibility of the Context Broker to respect the registration parameters when issuing distributed
requests. For instance, if a registration states that only Entities of a given type are offered, the distributed request does
not contain additional types. Such a strict requirement is justified because Context Sources (the receivers of the
distributed request) are not in a position to determine whether a request has been triggered by a registration, rendering
them unable to ensure that the registration parameters are respected in the first place. This applies for any kind of
context data a Context Broker can exchange such as Entity IDs, entity types, attribute names, geofenced areas, etc.
Ultimately, all constraints specified in the registration shall be respected.
When a Context Source is registered, an operation mode is selected. This defines the basis for distributed
operations and also defines whether or not the Context Broker is permitted to hold context data about the Entities
and Attributes locally itself.
If two registered Context Sources are providing context data for the same Attribute, the Attribute instances can be
distinguished by datasetId. The mechanism for determining which data shall be returned is defined in clause 4.5.5.
It is possible to restrict a registered Context Source to operate on a specific Entity type or list of Entity types. In
order for Context Broker hierarchies to support and restrict the distribution of such limited operations, the Entity
type selector (see clause 4.17) can be added as a filter on forwarded requests even where its presence initially seems
redundant.
ETSI
41
ETSI GS CIM 009 V1.9.1 (2025-07)
Furthermore, registered Context Sources may indicate that they are only willing to respond to a limited subset of
API operations. Context Brokers shall respect this, to avoid unnecessarily sending distributed operation requests
which are always guaranteed to fail. For example, a Context Source may consistently refuse certain API
operations since it does not support them. Alternatively, some Context Source endpoints (such as updates) may be
protected for use by authorized users only, and not accessible to a Context Broker without those rights. Limited
access is likely to be the case in extended data sharing scenarios, where a registered Context Source, and the data
held within it, may belong to an external third party.
For the endpoints served, all registered Context Sources shall support the normalized representation of Entities as
default. Support of additional representation formats is optional and will depend on the implementation. System
generated attributes such as modifiedAt and createdAt (see clause 4.8) should be supported by registered Context
Sources, at a minimum no error shall be returned if they are not available when requested.
4.3.6.2 Additive Registrations
For additive registrations, the Context Broker is permitted to hold context data about the Entities and Attributes
locally itself, and also obtain data from external sources. Context provisioning operations are serviced both locally by
the Context Broker itself, and also distributed on to the registered sources.
An inclusive Context Source Registration specifies that the Context Broker considers all registered
Context Sources as equals and will distribute operations to those Context Sources even if relevant context
data is available directly within the Context Broker itself (in which case, all results will be integrated in the final
response). Data from every Context Source registered by an inclusive Context Source Registration is
requested with an equal priority. This is the default mode of operation.
An auxiliary Context Source Registration never overrides data held directly within a Context Broker.
Auxiliary distributed operations are limited to context information consumption operations (see clause 5.7). Context
data from auxiliary context sources is only included if it is supplementary to the context data otherwise available to the
Context Broker. Auxiliary Context Source Registrations are always accepted as there can never be a
conflict.
4.3.6.3 Proxied Registrations
For proxied registrations, the Context Broker itself is not permitted to hold context data about the registered
Entities and Attributes locally (thus all registered context data is obtained from the external registered sources).
Unregistered Attributes of an Entity are permitted to be held locally; when context provisioning operations are received,
registered Attributes are distributed on to the registered sources and never serviced directly by the Context Broker
itself.
An exclusive Context Source Registration specifies that all of the registered context data is held in a single
location external to the Context Broker. The Context Broker itself holds no data locally about the registered
Attributes and no overlapping proxied Context Source Registrations shall be supported for the same
combination of registered Attributes on the Entity. An exclusive registration shall always relate to specific Attributes
found on a single Entity. Thus, the registration shall define both:
• An entity id (i.e. an id pattern or Entity type defining a group of entities is not supported for exclusive
registrations).
• Attributes.
Once an exclusive Context Source Source Registration has been created, no further exclusive or redirect Context
Registrations can be created for that same combination of Entity ID and Attributes.
A redirect Context external to the Context Source Registration also specifies that the registered context data is held in a location
Broker. It is possible to register (any combination of):
• A whole Entity by id or id pattern (i.e. without specifying individual Attributes in the registration; in this case,
all Attributes are held externally).
• Entities by Entity type only (with or without specifying individual Attributes).
ETSI
42
ETSI GS CIM 009 V1.9.1 (2025-07)
• Attributes only.
Potentially multiple distinct redirect registrations can apply at the same time. The Context Broker itself holds no
data locally in conflict to the registration. In the case that multiple overlapping redirect registrations are defined,
operations are distributed to all registered Context Sources.
4.3.6.4 Limiting Cascading Distributed Operations
When creating a registration, it is unknown whether the requested data is held at the distributed endpoint, or it is in turn
distributed via further registrations. It is necessary to include a binding-specific mechanism to request operations only
on the registered endpoint itself to avoid cascades of an excessive lengths, duplicates or loops.
Furthermore, it is not known if any distributed endpoints of a registered Context Source are in turn reliant on
previously encountered Context Sources thus causing an infinite loop. Therefore, when processing a distributed
operation, a specific field listing all previously encountered Context Sources (e.g. a Via header in the response in
case of HTTP binding (IETF RFC 7230 [27])) shall be passed as part of the request and this field can be used to exclude
duplicated sources from matching as context source registrations.
In the case of multi-tenancy (see clause 4.14) each Tenant found within each registered Context considered separately.
Source shall be
4.3.6.5 Extra information to provide when contacting Context Source
If the optional array (of KeyValuePair type, as defined by clause 5.2.22) contextSourceInfo of the CSourceRegistration
is present, it contains, whatever extra information the Context Broker shall convey when contacting the Context
Source. This can be information the Context Broker needs to successfully communicate with the Context
Source (e.g. Authorization material), or for the Context Source to correctly interpret the received content
(e.g. the Link URL to fetch an @context). The method for conveying this information is binding-specific, e.g. using
headers in the case of HTTP.
Instead of providing the actual value, the special value "urn:ngsi-ld:request" can be used to indicate that the
respective value is to be taken from the request that triggered the given request, if present.
EXAMPLE: If the key value pair "user": "urn:ngsi-ld:request" is part of contextSourceInfo of
the CSourceRegistration, the Context Broker checks if "user" was conveyed in the
triggering request. If this is the case, e.g. "user": "abcd", "user": "abcd" is also
conveyed when contacting the Context Source.
As Tenant information, if applicable, is directly specified in the CSourceRegistration, it shall not be part of
contextSourceInfo. Binding-specific information that is used for setting up the connection or is specific for an
interaction, e.g. Content-length in HTTP, cannot be overridden by contextSourceInfo. If present, such information shall
be ignored.
4.3.6.6 Additional pre- and post-processing of extra information when contacting
Context Source
The following key-values have a specific well-defined meaning when defined as elements within the optional array
contextSourceInfo of the CSourceRegistration.
If the key "accept" is defined:
• the value shall be a MIME type acceptable to the Context "application/ld+json").
Broker (one of: "application/json",
• the response from the distributed endpoint shall be returned in this defined format and if necessary, the
Context Broker shall be responsible for converting this to the desired content type when aggregating
responses to the initial request.
ETSI
43
ETSI GS CIM 009 V1.9.1 (2025-07)
If the key "conte


5.2.9 CSourceRegistration
This type represents the data needed to register a new Context Source.
The supported JSON members shall follow the indications provided in Table 5.2.9-1.
ETSI
Name Data Type Restriction Cardinality Description
id String Valid URI.
Unique registration identifier.
(JSON-LD @id).
0..1 Generated at creation
time, if it is not provided, it
will be assigned during
registration process and
returned to client.
It cannot be later modified
in update operations.
type String It shall be equal to
"ContextSourceRegistra
tion"
1 JSON-LD @type
Use reserved type for
identifying Context Source
Registration.
endpoint String It shall be a dereferenceable URI 1 Endpoint expressed as
dereferenceable URI
through which the
Context Source
exposes its NGSI-LD
interface.
contextSourceInfo KeyValuePair[] 0..1 Generic {key, value} array
to convey optional
information to provide
when contacting the
registered Context
Source.
information RegistrationInfo[] See data type definition in clause
5.2.10. Empty array (0 length) is
not allowed
1 Describes the Entities,
Properties and
Relationships for which
the Context Source
may be able to provide
information.
contextSourceAlias String Non-empty string. Pseudonym
field as defined in IETF
RFC 7230 [27]
0..1 A previously retrieved
unique id for a registered
Context Source
which is used to identify
loops.
In the multi-tenancy use
case (see clause 4.14),
this id shall be used to
identify a specific
Tenant within a
registered Context
Source.
description String Non-empty string 0..1 A description of this
Context Source
Registration.
datasetId String[] Valid URIs, "@none" for including
the default Attribute instances.
0..1 Specifies the datasetIds of
Attributes that the
Context Source can
provide, defined as per
clause 4.5.5.
expiresAt String DateTime (clause 4.6.3) 0..1 Provides an expiration
date. When passed the
Context Source
Registration will
become invalid and the
Context Source
might no longer be
available.
location GeoJSON
Geometry as
mandated by
clause 4.7
0..1 Location for which the
Context Source may
be able to provide
information.
112
ETSI GS CIM 009 V1.9.1 (2025-07)
Table 5.2.9-1: CSourceRegistration data type definition
ETSI
Name management managementInterval mode observationInterval 113
Data Type Restriction Registration
Management
Info
See data type definition in clause
5.2.34
TimeInterval See data type definition in clause
5.2.11
String It shall be one of:
"inclusive",
"exclusive", "redirect"
or "auxiliary"
The mode is assumed to be
"inclusive" if not explicitly
defined
TimeInterval See data type definition in
clause 5.2.11
ETSI
ETSI GS CIM 009 V1.9.1 (2025-07)
Cardinality Description
0..1 Holds additional optional
registration management
information that can be
used to limit unnecessary
distributed operation
requests.
0..1 If present, the Context
Source can be queried
for Temporal Entity
Representations. (If latest
Entity information is also
provided, a separate
Context Registration is
needed for this purpose).
The managementInterval
specifies the time interval
for which the Context
Source can provide
Entity information as
specified by the createdAt,
modifiedAt and deletedAt
Temporal Properties. A
temporal query based on
the createdAt, modifiedAt
or deletedAt Temporal
Property is matched
against the
managementInterval for
overlap.
0..1 The definition of the mode
of distributed operation
(see clause 4.3.6)
supported by the
registered Context
Source.
0..1 If present, the Context
Source can be queried
for Temporal Entity
Representations. (If latest
Entity information is also
provided, a separate
Context Registration is
needed for this purpose).
The observationInterval
specifies the time interval
for which the Context
Source can provide
Entity information as
specified by the
observedAt Temporal
Property. A temporal
query based on the
observedAt Temporal
Property, which is the
default, is matched
against the
observationInterval for
overlap.
114
ETSI GS CIM 009 V1.9.1 (2025-07)
Name Data Type Restriction Cardinality Description
observationSpace GeoJSON
Geometry as
mandated by
clause 4.7
0..1 Geographic location that
includes the observation
spaces of all entities as
specified by their
respective
observationSpace
GeoProperty for which the
Context Source may
be able to provide
information.
operations String[] Entries are limited to the named
API operations and named
operation groups (see clause
4.20)
0..1 The definition limited
subset of API operations
supported by the
registered Context
Source.
If undefined, the default
set of operations is
"federationOps"
(see clause 4.20).
operationSpace GeoJSON
Geometry as
mandated by
clause 4.7
0..1 Geographic location that
includes the operation
spaces of all entities as
specified by their
respective operationSpace
GeoProperty for which the
Context Source may
be able to provide
information.
refreshRate String String representing a duration in
ISO 8601 [17] format
0..1 An indication of the likely
period of time to elapse
between updates at this
registered endpoint.
Brokers may optionally
use this information to
help implement caching.
registrationName String Non-empty string 0..1 A name given to this
Context Source
Registration.
scope String or
String[]
Scope(s) 0..1 Scopes (see clause 4.18)
for which the Context
Source has Entities.
tenant String 0..1 Identifies the Tenant
that has to be specified in
all requests to the
Context Source that
are related to the
information registered in
this Context Source
Registration. If not
present, the default
Tenant is assumed.
Should only be present in
systems supporting
multi-tenancy.
The members (defined by Table 5.2.9-2) of the CSourceRegistration data structure are also defined. They are read-only
and shall be automatically generated by NGSI-LD implementations. In the event that they are provided (in update or
create operations) NGSI-LD implementations shall ignore them.
ETSI
115
ETSI GS CIM 009 V1.9.1 (2025-07)
Table 5.2.9-2: Additional members of the CSourceRegistration data type
Name Data Type Restrictions Cardinality Description
lastFailure String DateTime (clause 4.6.3) 0..1 Timestamp corresponding to the
instant when the last distributed
operation resulting in a failure (for
instance, in the HTTP binding, an
HTTP response code other than 2xx)
was returned.
status String Allowed values:
"ok"
"failed"
0..1 Read-only., Status of the
Registration. It shall be "ok" if
the last attempt to perform a
distributed operation succeeded.
It shall be "failed" if the last
attempt to perform a distributed
operation failed.
timesFailed Number 0 or greater value 0..1 Number of times that the
registration triggered a
distributed operation request that
failed.
timesSent Number 0 or greater value 0..1 Number of times that the
registration triggered a
distributed operation, including
failed attempts.
lastSuccess String DateTime (clause 4.6.3) 0..1 Timestamp corresponding to the
instant when the last successfully
distributed operation was sent.
Created on first successful
operation.
5.2.10 RegistrationInfo
The supported JSON members shall follow the requirements provided in Table 5.2.10-1.
Table 5.2.10-1: RegistrationInfo data type definition
Name Data Type Restrictions Cardinality Description
entities EntityInfo[] See data type definition in
clause 5.2.8. Empty array (0
length) is not allowed.
Restrictions in clause 4.3.6
apply as well
0..1 Describes the entities for which the
CSource may be able to provide
information.
propertyNames String[] Property names as short
hand strings or URIs. Empty
array is not allowed.
Restrictions in clause 4.3.6
apply as well
0..1 Describes the Properties that the
CSource may be able to provide.
relationshipNames String[] Relationship names as short
hand strings or URIs. Empty
array is not allowed.
Restrictions in clause 4.3.6
apply as well
0..1 Describes the Relationships that the
CSou