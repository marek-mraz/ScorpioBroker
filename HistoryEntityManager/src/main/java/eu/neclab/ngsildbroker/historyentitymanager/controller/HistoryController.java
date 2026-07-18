package eu.neclab.ngsildbroker.historyentitymanager.controller;

import java.util.Map;

import jakarta.inject.Inject;
import jakarta.enterprise.context.ApplicationScoped;
import io.quarkus.runtime.Startup;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;
import org.jboss.resteasy.reactive.RestResponse;

import com.github.jsonldjava.core.JsonLDService;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.historyentitymanager.service.HistoryEntityService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonObject;

@ApplicationScoped
@Startup
@Path("/ngsi-ld/v1/temporal/entities")
public class HistoryController {

	@Inject
	HistoryEntityService historyService;

	@ConfigProperty(name = "scorpio.history.default-limit")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.history.max-limit")
	int maxLimit;

	@ConfigProperty(name = "scorpio.ngsild.corecontext")
	String coreContext;

	@Inject
	JsonLDService ldService;

	@POST
	@Counted(value = "temp_entity_create_total", description = "Total number of temp entity create requests")
	@Timed(value = "temp_entity_create_duration", description = "Duration of temp entity create requests")
	@Timed(value = "temp_entity_create_concurrent", longTask = true, description = "Number of concurrent temp entity create requests")
	public Uni<RestResponse<Object>> createTemporalEntity(HttpServerRequest request, String body) {
		Map<String, Object> payload;
		try {
			payload = new JsonObject(body).getMap();
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return HttpUtils.expandBody(request, payload, AppConstants.TEMP_ENTITY_CREATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return historyService.createEntry(HttpUtils.getTenant(request), tuple.getItem2(), tuple.getItem1(),
							request.headers()).onItem().transform(opResult -> {
								if (opResult.isWasUpdated()) {
									return HttpUtils.generateUpdateResultResponse(opResult);
								}
								return HttpUtils.generateCreateResult(opResult, AppConstants.HISTORY_URL);
							});
				}).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
	}

	@Path("/{entityId}")
	@DELETE
	@Counted(value = "temp_entity_delete_total", description = "Total number of temp entity delete requests")
	@Timed(value = "temp_entity_delete_duration", description = "Duration of temp entity delete requests")
	@Timed(value = "temp_entity_delete_concurrent", longTask = true, description = "Number of concurrent temp entity delete requests")
	public Uni<RestResponse<Object>> deleteTemporalEntityById(HttpServerRequest request,
			@PathParam("entityId") String entityId) {
		try {
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(ctx -> {
			return historyService.deleteEntry(HttpUtils.getTenant(request), entityId, ctx, request.headers()).onItem()
					.transform(result -> {
						return HttpUtils.generateDeleteResult(result);
					});
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
	}

	@Path("/{entityId}/attrs")
	@POST
	@Counted(value = "temp_entity_add_attrs_total", description = "Total number of temp entity add attrs requests")
	@Timed(value = "temp_entity_add_attrs_duration", description = "Duration of temp entity add attrs requests")
	@Timed(value = "temp_entity_add_attrs_concurrent", longTask = true, description = "Number of concurrent temp entity add attrs requests")
	public Uni<RestResponse<Object>> addAttrib2TemopralEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, String body) {
		Map<String, Object> payload;

		try {
			payload = new JsonObject(body).getMap();
			HttpUtils.validateUri(entityId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return HttpUtils.expandBody(request, payload, AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return historyService.appendToEntry(HttpUtils.getTenant(request), entityId, tuple.getItem2(),
							tuple.getItem1(), request.headers()).onItem().transform(opResult -> {
								return HttpUtils.generateUpdateResultResponse(opResult);
							});
				}).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
	}

	@Path("/{entityId}/attrs/{attrId}")
	@DELETE
	@Counted(value = "temp_entity_delete_attrs_total", description = "Total number of temp entity delete attrs requests")
	@Timed(value = "temp_entity_delete_attrs_duration", description = "Duration of temp entity delete attrs requests")
	@Timed(value = "temp_entity_delete_attrs_concurrent", longTask = true, description = "Number of concurrent temp entity delete attrs requests")
	public Uni<RestResponse<Object>> deleteAttrib2TemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId,
			@QueryParam("datasetId") String datasetId, @QueryParam("deleteAll") String deleteAllS) {
		// An empty attribute segment (DELETE .../attrs//{instanceId}) is normalized by the router to this
		// 3-segment route with attrId={instanceId}; the original raw URI still carries the empty segment.
		// NGSI-LD requires an Attribute name, so reject it as BadRequestData instead of a misleading 404.
		if (request.uri() != null && request.uri().contains("/attrs//")) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData, "Attribute name is required"),
					HttpUtils.getTenant(request)));
		}
		boolean deleteAll;
		try {
			HttpUtils.validateUri(entityId);
			HttpUtils.validateAttribName(attrId);
			deleteAll = HttpUtils.parseBoolean(deleteAllS);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			return historyService.deleteAttrFromEntry(HttpUtils.getTenant(request), entityId,
					context.expandIri(attrId, false, true, null, null), datasetId, deleteAll, context,
					request.headers()).onItem().transform(opResult -> {
						return HttpUtils.generateDeleteResult(opResult);
					});
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));

	}

	@Path("/{entityId}/attrs/{attrId}/{instanceId}")
	@PATCH
	@Counted(value = "temp_entity_patch_attrs_instance_total", description = "Total number of temp entity attrs instance patch requests")
	@Timed(value = "temp_entity_patch_attrs_instance_duration", description = "Duration of temp entity attrs instance patch requests")
	@Timed(value = "temp_entity_patch_attrs_instance_concurrent", longTask = true, description = "Number of concurrent temp entity attrs instance patch requests")
	public Uni<RestResponse<Object>> modifyAttribInstanceTemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId,
			@PathParam("instanceId") String instanceId, String body) {
		Map<String, Object> payload;
		try {
			payload = new JsonObject(body).getMap();
			HttpUtils.validateUri(entityId);
			HttpUtils.validateUri(instanceId);
			HttpUtils.validateAttribName(attrId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}

		return HttpUtils.expandBody(request, payload, AppConstants.TEMP_ENTITY_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return historyService
							.updateInstanceOfAttr(HttpUtils.getTenant(request), entityId,
									tuple.getItem1().expandIri(attrId, false, true, null, null), instanceId,
									tuple.getItem2(), tuple.getItem1(), request.headers())
							.onItem().transform(opResult -> {
								return HttpUtils.generateUpdateResultResponse(opResult);
							});
				}).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));

	}

	@Path("/{entityId}/attrs/{attrId}/{instanceId}")
	@DELETE
	@Counted(value = "temp_entity_delete_attrs_instance_total", description = "Total number of temp entity attrs instance delete requests")
	@Timed(value = "temp_entity_delete_attrs_instance_duration", description = "Duration of temp entity attrs instance delete requests")
	@Timed(value = "temp_entity_delete_attrs_instance_concurrent", longTask = true, description = "Number of concurrent temp entity attrs instance delete requests")
	public Uni<RestResponse<Object>> deleteAtrribInstanceTemporalEntity(HttpServerRequest request,
			@PathParam("entityId") String entityId, @PathParam("attrId") String attrId,
			@PathParam("instanceId") String instanceId) {
		try {
			HttpUtils.validateUri(entityId);
			HttpUtils.validateUri(instanceId);
			HttpUtils.validateAttribName(attrId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(context -> {
			return historyService
					.deleteInstanceOfAttr(HttpUtils.getTenant(request), entityId,
							context.expandIri(attrId, false, true, null, null), instanceId, context, request.headers())
					.onItem().transform(opResult -> {
						return HttpUtils.generateDeleteResult(opResult);
					});
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
	}

	// Missing entity id (e.g. POST/DELETE/PATCH /temporal/entities//attrs...): the empty
	// path segment collapses to /temporal/entities/attrs..., so add explicit handlers that
	// return 400 instead of the routing default 405. NGSI-LD requires Entity Id.
	private Uni<RestResponse<Object>> missingId(HttpServerRequest request) {
		return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
				new ResponseException(ErrorType.BadRequestData, "Entity Id is required"), HttpUtils.getTenant(request)));
	}

	@POST
	@Path("/attrs")
	public Uni<RestResponse<Object>> addAttribMissingId(HttpServerRequest request, String body) {
		return missingId(request);
	}

	@DELETE
	@Path("/attrs/{attrId}")
	public Uni<RestResponse<Object>> deleteAttribMissingId(HttpServerRequest request, @PathParam("attrId") String attrId) {
		return missingId(request);
	}

	@PATCH
	@Path("/attrs/{attrId}/{instanceId}")
	public Uni<RestResponse<Object>> modifyInstanceMissingId(HttpServerRequest request,
			@PathParam("attrId") String attrId, @PathParam("instanceId") String instanceId, String body) {
		return missingId(request);
	}

	@DELETE
	@Path("/attrs/{attrId}/{instanceId}")
	public Uni<RestResponse<Object>> deleteInstanceMissingId(HttpServerRequest request,
			@PathParam("attrId") String attrId, @PathParam("instanceId") String instanceId) {
		return missingId(request);
	}
}
