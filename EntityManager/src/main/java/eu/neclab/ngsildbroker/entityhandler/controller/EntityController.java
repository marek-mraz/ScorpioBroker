package eu.neclab.ngsildbroker.entityhandler.controller;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.entityhandler.services.EntityService;
import io.quarkus.runtime.Startup;
import io.smallrye.mutiny.Uni;
import io.vertx.core.MultiMap;
import io.vertx.core.http.HttpServerRequest;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

/**
 * 
 * @version 1.0
 * @date 10-Jul-2018
 */
@ApplicationScoped
@Startup
@Path("/ngsi-ld/v1")
public class EntityController {// implements EntityHandlerInterface {

	private final static Logger logger = LoggerFactory.getLogger(EntityController.class);

	@Inject
	EntityService entityService;

	@ConfigProperty(name = "scorpio.ngsild.corecontext")
	String coreContext;

	@Inject
	JsonLDService ldService;

	@Inject
	MicroServiceUtils microServiceUtils;

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/" rest endpoint.
	 * 
	 * @param body jsonld message
	 * @return ResponseEntity object
	 */
	@Path("/entities")
	@POST
	@Counted(value = "entity_create_total", description = "Total number of entity create requests")
	@Timed(value = "entity_create_duration", description = "Duration of entity create requests")
	@Timed(value = "entity_create_concurrent", longTask = true, description = "Number of concurrent entity create requests")
	public Uni<RestResponse<Object>> createEntity(HttpServerRequest req, String bodyStr,
			@QueryParam(value = "local") String localOnlyS) {

		Map<String, Object> body;
		String tenant = HttpUtils.getTenant(req);
		try {
			body = new JsonObject(bodyStr).getMap();
		} catch (DecodeException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(req)));
		}
		// local=true (NGSI-LD 6.3.18): handle only on this broker, do not forward to Context Sources.
		boolean localOnly;
		ViaHeaders viaHeaders;
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			viaHeaders = new ViaHeaders(req.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		// try {
		// noConcise(body);
		// } catch (ResponseException e) {
		// return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e,
		// HttpUtils.getTenant(req)));
		// }
		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_CREATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					logger.debug("creating entity");

					// A type that expands to a JSON-LD keyword (e.g. "type" -> "@type") is not a
					// valid entity type -> reject rather than store it.
					Object expType = tuple.getItem2().get(NGSIConstants.JSON_LD_TYPE);
					if (expType instanceof List<?> expTypeList) {
						for (Object t : expTypeList) {
							if (t instanceof String ts && ts.startsWith("@")) {
								return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
										new ResponseException(ErrorType.BadRequestData, "Invalid entity type: " + ts),
										tenant));
							}
						}
					}

					return entityService
							.createEntity(tenant, tuple.getItem2(), tuple.getItem1(), req.headers(), viaHeaders,
									localOnly)
							.onItem().transform(opResult -> {
								logger.debug("Done creating entity");
								return HttpUtils.generateCreateResult(opResult, AppConstants.ENTITES_URL);
							});
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, tenant);
				});

	}

	/**
	 * Method(PATCH) for "/ngsi-ld/v1/entities/{entityId}/attrs" rest endpoint.
	 * 
	 * @param entityId
	 * @param body     json ld message
	 * @return ResponseEntity object
	 */

	@PATCH
	@Path("/entities/{entityId}/attrs")
	@Counted(value = "entity_patch_total", description = "Total number of entity patch requests")
	@Timed(value = "entity_patch_duration", description = "Duration of entity patch requests")
	@Timed(value = "entity_patch_concurrent", longTask = true, description = "Number of concurrent entity patch requests")
	public Uni<RestResponse<Object>> updateEntity(HttpServerRequest req, @PathParam("entityId") String entityId,
			String bodyStr) {
		Map<String, Object> body;
		String tenant = HttpUtils.getTenant(req);
		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(req)));
		}
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(req.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		// try {
		// noConcise(body);
		// } catch (ResponseException e) {
		// return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e,
		// HttpUtils.getTenant(req)));
		// }

		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {

					logger.debug("patch attrs");
					return entityService.updateEntity(HttpUtils.getTenant(req), entityId, tuple.getItem2(),
							tuple.getItem1(), req.headers(), viaHeaders).onItem()
							.transform(HttpUtils::generateUpdateResultResponse);
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(req));
				});
	}

	/**
	 * Method(POST) for "/ngsi-ld/v1/entities/{entityId}/attrs" rest endpoint.
	 * 
	 * @param entityId
	 * @param body     jsonld message
	 * @return ResponseEntity object
	 */

	@POST
	@Path("/entities/attrs")
	public Uni<RestResponse<Object>> appendEntityMissingId(HttpServerRequest req) {
		return Uni.createFrom().item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData, "Entity Id is required"), HttpUtils.getTenant(req)));
	}

	@PATCH
	@Path("/entities/attrs")
	public Uni<RestResponse<Object>> updateAttribsMissingId(HttpServerRequest req) {
		return Uni.createFrom().item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData, "Entity Id is required"), HttpUtils.getTenant(req)));
	}

	@POST
	@Path("/entities/{entityId}/attrs")
	@Counted(value = "entity_update_total", description = "Total number of entity update requests")
	@Timed(value = "entity_update_duration", description = "Duration of entity update requests")
	@Timed(value = "entity_update_concurrent", longTask = true, description = "Number of concurrent entity update requests")
	public Uni<RestResponse<Object>> appendEntity(HttpServerRequest req, @PathParam("entityId") String entityId,
			String bodyStr, @QueryParam("options") String options) {
		Map<String, Object> body;
		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(req)));
		}
		String tenant = HttpUtils.getTenant(req);
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(req.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		// try {
		// noConcise(body);
		// } catch (ResponseException e) {
		// return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e,
		// HttpUtils.getTenant(req)));
		// }
		boolean noOverwrite = options != null && options.contains(NGSIConstants.NO_OVERWRITE_OPTION);
		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					logger.debug("post attrs");
					return entityService
							.appendToEntity(HttpUtils.getTenant(req), entityId, tuple.getItem2(), noOverwrite,
									tuple.getItem1(), req.headers(), viaHeaders)
							.onItem().transform(HttpUtils::generateUpdateResultResponse);
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(req));
				});

	}

	/**
	 * Method(PATCH) for "/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}" rest
	 * endpoint.
	 * 
	 * @param entityId
	 * @param body
	 * @return
	 */
	@PATCH
	@Path("/entities/attrs/{attrId}")
	public Uni<RestResponse<Object>> partialUpdateAttributeMissingId(HttpServerRequest req, @PathParam("attrId") String attrId) {
		return Uni.createFrom().item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData, "Entity Id is required"), HttpUtils.getTenant(req)));
	}

	@PATCH
	@Path("/entities/{entityId}/attrs/{attrId}")
	@Counted(value = "attrs_patch_total", description = "Total number of attrs patch requests")
	@Timed(value = "attrs_patch_duration", description = "Duration of attrs patch requests")
	@Timed(value = "attrs_patch_concurrent", longTask = true, description = "Number of concurrent attrs patch requests")
	public Uni<RestResponse<Object>> partialUpdateAttribute(HttpServerRequest req,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrib, String bodyStr) {

		Map<String, Object> body;
		if (NGSIConstants.ENTITY_BASE_PROPS_SHORT.contains(attrib) || NGSIConstants.ENTITY_BASE_PROPS.contains(attrib)) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData, "Cannot update base property"), HttpUtils.getTenant(req)));
		}

		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(req)));
		}

		String tenant = HttpUtils.getTenant(req);
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(req.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		// try {
		// noConcise(body);
		// } catch (ResponseException e) {
		// return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e,
		// HttpUtils.getTenant(req)));
		// }

		return HttpUtils.expandBody(req, body, AppConstants.ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					String expAttrib = tuple.getItem1().expandIri(attrib, false, true, null, null);
					logger.debug("update entry :: started");

					Map<String, Object> expandedPayload = tuple.getItem2();
					Map<String, Object> finalPayload = new HashMap<>();
					if (!expandedPayload.containsKey(expAttrib)) {
						finalPayload.put(expAttrib, List.of(expandedPayload));
					} else {
						finalPayload = expandedPayload;
					}
					// ponytail: in a partial-update fragment the core term datasetId mis-expands to a
					// Property node {@type:Property, hasValue:[{@value:<id>}]} instead of {@id:<id>}, so
					// the SQL (ngsild_update_entity / ngsild_partialupdate) can't match the target
					// instance by datasetId -> null-delete and update-by-datasetId silently no-op.
					// Normalize back to {@id}. No-op when already correct.
					normalizeFragmentDatasetId(finalPayload.get(expAttrib));

					return entityService.partialUpdateAttribute(HttpUtils.getTenant(req), entityId, expAttrib,
							finalPayload, tuple.getItem1(), req.headers(), viaHeaders).onItem()
							.transform(updateResult -> {
								logger.trace("update entry :: completed");
								return HttpUtils.generateUpdateResultResponse(updateResult);
							});
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(req));
				});
	}

	@SuppressWarnings("unchecked")
	private static void normalizeFragmentDatasetId(Object attrValue) {
		if (!(attrValue instanceof List<?> instances)) {
			return;
		}
		for (Object inst : instances) {
			if (!(inst instanceof Map)) {
				continue;
			}
			Map<String, Object> instance = (Map<String, Object>) inst;
			Object ds = instance.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
			if (!(ds instanceof List<?> dsList) || dsList.isEmpty() || !(dsList.get(0) instanceof Map)) {
				continue;
			}
			Map<String, Object> dsMap = (Map<String, Object>) dsList.get(0);
			if (dsMap.containsKey(NGSIConstants.JSON_LD_ID)) {
				continue; // already {@id:<value>}
			}
			Object hv = dsMap.get(NGSIConstants.NGSI_LD_HAS_VALUE);
			if (hv instanceof List<?> hvList && !hvList.isEmpty() && hvList.get(0) instanceof Map) {
				Object val = ((Map<String, Object>) hvList.get(0)).get(NGSIConstants.JSON_LD_VALUE);
				if (val != null) {
					instance.put(NGSIConstants.NGSI_LD_DATA_SET_ID,
							List.of(Map.of(NGSIConstants.JSON_LD_ID, val)));
				}
			}
		}
	}

	/**
	 * Method(DELETE) for "/ngsi-ld/v1/entities/{entityId}/attrs/{attrId}" rest
	 * endpoint.
	 * 
	 * @param entityId
	 * @param attrId
	 * @return
	 */

	@DELETE
	@Path("/entities/attrs/{attrId}")
	public Uni<RestResponse<Object>> deleteAttributeMissingId(HttpServerRequest request, @PathParam("attrId") String attrId) {
		return Uni.createFrom().item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData, "Entity Id is required"), HttpUtils.getTenant(request)));
	}

	@DELETE
	@Path("/entities/{entityId}/attrs/{attrId}")
	@Counted(value = "attrs_delete_total", description = "Total number of attrs delete requests")
	@Timed(value = "attrs_delete_duration", description = "Duration of attrs delete requests")
	@Timed(value = "attrs_delete_concurrent", longTask = true, description = "Number of concurrent attrs delete requests")
	public Uni<RestResponse<Object>> deleteAttribute(HttpServerRequest request, @PathParam("entityId") String entityId,
			@PathParam("attrId") String attrId, @QueryParam("datasetId") String datasetId,
			@QueryParam("deleteAll") String deleteAllS) {
		// scope is a base member but, unlike id/type/timestamps, it IS deletable (NGSI-LD 5.6.17)
		boolean isScope = NGSIConstants.SCOPE.equals(attrId) || NGSIConstants.NGSI_LD_SCOPE.equals(attrId);
		if (!isScope && (NGSIConstants.ENTITY_BASE_PROPS_SHORT.contains(attrId)
				|| NGSIConstants.ENTITY_BASE_PROPS.contains(attrId))) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData, "Cannot delete base property"), HttpUtils.getTenant(request)));
		}
		boolean deleteAll;
		try {
			deleteAll = HttpUtils.parseBoolean(deleteAllS);
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		String tenant = HttpUtils.getTenant(request);
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			String finalAttrId = context.expandIri(attrId, false, true, null, null);
			logger.trace("delete attribute :: started");
			return entityService.deleteAttribute(HttpUtils.getTenant(request), entityId, finalAttrId, datasetId,
					deleteAll, context, request.headers(), viaHeaders).onItem().transform(opResult -> {
						logger.trace("delete attribute :: completed");
						return HttpUtils.generateDeleteResult(opResult);

					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
		});

	}

	/**
	 * Method(DELETE) for "/ngsi-ld/v1/entities/{entityId}" rest endpoint.
	 * 
	 * @param entityId
	 * @return
	 */
	@DELETE
	@Path("/entities/{entityId}")
	@Counted(value = "entity_delete_total", description = "Total number of entity delete requests")
	@Timed(value = "entity_delete_duration", description = "Duration of entity delete requests")
	@Timed(value = "entity_delete_concurrent", longTask = true, description = "Number of concurrent entity delete requests")
	public Uni<RestResponse<Object>> deleteEntity(HttpServerRequest request, @PathParam("entityId") String entityId,
			@QueryParam(value = "local") String localOnlyS) {
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		String tenant = HttpUtils.getTenant(request);
		// local=true (NGSI-LD 6.3.18): handle only on this broker, do not forward to Context Sources.
		boolean localOnly;
		ViaHeaders viaHeaders;
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			return entityService
					.deleteEntity(HttpUtils.getTenant(request), entityId, context, request.headers(), viaHeaders,
							localOnly)
					.onItem().transform(HttpUtils::generateDeleteResult);
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
		});

	}

	@PATCH
	@Path("/entities")
	@Counted(value = "entity_merge_patch_total", description = "Total number of entity merge patch requests")
	@Timed(value = "entity_merge_patch_duration", description = "Duration of entity merge patch requests")
	@Timed(value = "entity_merge_patch_concurrent", longTask = true, description = "Number of concurrent entity merge patch requests")
	public Uni<RestResponse<Object>> mergePatchPure(HttpServerRequest request,
			String bodyStr) {
		String id;
		try {
			Map<String, Object> body = new JsonObject(bodyStr).getMap();
			id = (String) body.get(NGSIConstants.ID);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return mergePatch(request, id, bodyStr);
	}

	@PATCH
	@Path("/entities/{entityId}")
	@Counted(value = "entity_merge_patch_total", description = "Total number of entity merge patch requests")
	@Timed(value = "entity_merge_patch_duration", description = "Duration of entity merge patch requests")
	@Timed(value = "entity_merge_patch_concurrent", longTask = true, description = "Number of concurrent entity merge patch requests")
	public Uni<RestResponse<Object>> mergePatch(HttpServerRequest request, @PathParam("entityId") String entityId,
			String bodyStr) {
		Map<String, Object> body;
		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		String tenant = HttpUtils.getTenant(request);
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		if (!entityId.equals(body.get(NGSIConstants.ID)) && body.get(NGSIConstants.ID) != null) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(
							new ResponseException(ErrorType.BadRequestData, "Id can not be updated"),
							HttpUtils.getTenant(request)));
		}
		// try {
		// noConcise(body);
		// } catch (ResponseException e) {
		// return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e,
		// HttpUtils.getTenant(request)));
		// }
		return HttpUtils.expandBody(request, body, AppConstants.MERGE_PATCH_REQUEST, ldService).onItem()
				.transformToUni(tuple -> {
					return entityService.mergePatch(HttpUtils.getTenant(request), entityId, tuple.getItem2(),
							tuple.getItem1(), request.headers(), viaHeaders).onItem()
							.transform(HttpUtils::generateUpdateResultResponse);
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
				});

	}

	@DELETE
	@Path("/entities")
	public Uni<RestResponse<Object>> purgeEntities(HttpServerRequest request, @QueryParam("keep") String keep,
			@QueryParam("drop") String drop, @QueryParam("local") String localOnlyS) {
		String tenant = HttpUtils.getTenant(request);
		// NGSI-LD 5.6.21: keep (exclusionary) and drop (restrictive) are mutually exclusive.
		if (keep != null && !keep.isEmpty() && drop != null && !drop.isEmpty()) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData, "keep and drop cannot be used together"), tenant));
		}
		boolean localOnly;
		ViaHeaders viaHeaders;
		try {
			localOnly = HttpUtils.parseBoolean(localOnlyS);
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		// NGSI-LD 5.6.21.4: at least one selector (type/attrs/q/geoquery/local) must be present, else
		// BadRequestData ("too wide query"). An explicit id list is the narrowest selector and is also
		// accepted; idPattern ALONE is not (it can be too wide, e.g. ".*"). The list-query endpoint needs
		// one of type/attrs/geometry/q, so an id-only selector is resolved by direct retrieval instead.
		MultiMap params = request.params();
		boolean broadFilter = params.contains("type") || params.contains("attrs") || params.contains("geometry")
				|| params.contains("q");
		String idParam = params.get("id");
		if (!localOnly && !broadFilter && idParam == null) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(new ResponseException(
					ErrorType.BadRequestData, "At least one of type, attrs, geometry, q or id is required, or local scope"),
					tenant));
		}
		// keep/drop are purge-only; strip them before reusing the query endpoint (which rejects them).
		String cleanedQuery = stripQueryParams(request.query(), "keep", "drop");
		boolean useListQuery = broadFilter || localOnly;
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			return entityService
					.purgeEntities(tenant, cleanedQuery, idParam, useListQuery, keep, drop, localOnly, context,
							request.headers(), viaHeaders, params)
					.onItem().transform(v -> RestResponse.noContent());
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, tenant);
		});
	}

	// Remove the named params (e.g. keep/drop) from a raw, URL-encoded query string, preserving the rest.
	private static String stripQueryParams(String query, String... names) {
		if (query == null || query.isEmpty()) {
			return "";
		}
		StringBuilder out = new StringBuilder();
		for (String part : query.split("&")) {
			String key = part.contains("=") ? part.substring(0, part.indexOf('=')) : part;
			boolean drop = false;
			for (String n : names) {
				if (key.equals(n)) {
					drop = true;
					break;
				}
			}
			if (!drop) {
				if (out.length() > 0) {
					out.append('&');
				}
				out.append(part);
			}
		}
		return out.toString();
	}

	@PUT
	@Path("/entities")
	public Uni<RestResponse<Object>> replaceEntityMissingId(HttpServerRequest request) {
		return Uni.createFrom().item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData, "Entity Id is required"), HttpUtils.getTenant(request)));
	}

	@Path("/entities/{entityId}")
	@PUT
	@Counted(value = "entity_replace_total", description = "Total number of entity replace requests")
	@Timed(value = "entity_replace_duration", description = "Duration of entity replace requests")
	@Timed(value = "entity_replace_concurrent", longTask = true, description = "Number of concurrent entity replace requests")
	public Uni<RestResponse<Object>> replaceEntity(@PathParam("entityId") String entityId, HttpServerRequest request,
			String bodyStr) {
		logger.debug("replacing entity");
		Map<String, Object> body;
		try {
			HttpUtils.validateUri(entityId);
			body = new JsonObject(bodyStr).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		String tenant = HttpUtils.getTenant(request);
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		// try {
		// noConcise(body);
		// } catch (ResponseException e) {
		// return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e,
		// HttpUtils.getTenant(request)));
		// }
		if (!body.containsKey(NGSIConstants.ID)) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
				new ResponseException(ErrorType.BadRequestData, "Id can not be null"), HttpUtils.getTenant(request)));
		}
		if (!entityId.equals(body.get(NGSIConstants.ID))) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
				new ResponseException(ErrorType.BadRequestData, "Id can not be updated"), HttpUtils.getTenant(request)));
		}
		body.put(NGSIConstants.ID, entityId);
		if (!body.containsKey(NGSIConstants.TYPE)) {
			return Uni.createFrom()
					.item(HttpUtils.handleControllerExceptions(
							new ResponseException(ErrorType.BadRequestData, "Type can not be null"),
							HttpUtils.getTenant(request)));
		}
		return HttpUtils.expandBody(request, body, AppConstants.REPLACE_ENTITY_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {

					return entityService.replaceEntity(HttpUtils.getTenant(request), entityId, tuple.getItem2(),
							tuple.getItem1(), request.headers(), viaHeaders).onItem().transform(opResult -> {

								logger.debug("Done replacing entity");
								return HttpUtils.generateUpdateResultResponse(opResult);
							}).onFailure().recoverWithItem(e -> {
								return HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request));
							});
				});
	}

	@PUT
	@Path("/entities/attrs/{attrId}")
	public Uni<RestResponse<Object>> replaceAttributeMissingId(HttpServerRequest req, @PathParam("attrId") String attrId) {
		return Uni.createFrom().item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData, "Entity Id is required"), HttpUtils.getTenant(req)));
	}

	@Path("/entities/{entityId}/attrs/{attrId}")
	@PUT
	@Counted(value = "attrs_replace_total", description = "Total number of attrs replace requests")
	@Timed(value = "attrs_replace_duration", description = "Duration of attrs replace requests")
	@Timed(value = "attrs_replace_concurrent", longTask = true, description = "Number of concurrent attrs replace requests")
	public Uni<RestResponse<Object>> replaceAttribute(@PathParam("attrId") String attrId,
			@PathParam("entityId") String entityId, HttpServerRequest request, String bodyStr,
			@QueryParam("datasetId") String datasetId) {
		logger.debug("replacing Attrs");

		if (NGSIConstants.ENTITY_BASE_PROPS_SHORT.contains(attrId) || NGSIConstants.ENTITY_BASE_PROPS.contains(attrId)) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData, "Cannot replace base property"), HttpUtils.getTenant(request)));
		}
		// attribute names starting with @ are JSON-LD keywords, never valid attribute names
		if (attrId == null || attrId.startsWith("@")) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData, "Invalid attribute name"), HttpUtils.getTenant(request)));
		}

		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}

		String tenant = HttpUtils.getTenant(request);
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return JsonUtils.fromString(bodyStr).onItem().transformToUni(body -> {
			Map<String, Object> bodyMap = new HashMap<>();
			if (body instanceof Map m) {
				bodyMap.put(attrId, List.of(m));
			} else if (body instanceof List l) {
				bodyMap.put(attrId, l);
			} else {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(new ResponseException(ErrorType.BadRequestData), tenant));
			}

			return HttpUtils.expandBody(request, bodyMap, AppConstants.ENTITY_UPDATE_PAYLOAD, ldService)
					.onItem()
					.transformToUni(tuple -> {
						String finalAttrId = tuple.getItem1().expandIri(attrId, false, true, null, null);
						Map<String, Object> finalPayload = tuple.getItem2();

						return entityService.replaceAttribute(tenant, finalPayload,
								tuple.getItem1(), entityId, finalAttrId, datasetId, request.headers(), viaHeaders).onItem()
								.transform(opResult -> {
									logger.debug("Done replacing attribute");
									return HttpUtils.generateUpdateResultResponse(opResult);
								}).onFailure().recoverWithItem(e -> {
									return HttpUtils.handleControllerExceptions(e, tenant);
								});
					});
		});
	}
}
