package eu.neclab.ngsildbroker.registryhandler.controller;

import java.util.List;
import java.util.Set;

import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import jakarta.inject.Inject;
import jakarta.enterprise.context.ApplicationScoped;
import io.quarkus.runtime.Startup;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import io.micrometer.core.annotation.Counted;
import io.micrometer.core.annotation.Timed;
import org.jboss.resteasy.reactive.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.jsonldjava.core.JsonLDService;
import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.datatypes.terms.AttrsQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.CSFQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.GeoQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.QQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.ScopeQueryTerm;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.QueryParser;
import eu.neclab.ngsildbroker.registryhandler.service.CSourceService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;

/**
 * 
 * @version 1.0
 * @date 20-Jul-2018
 */
@ApplicationScoped
@Startup
@Path("/ngsi-ld/v1/csourceRegistrations")
public class RegistryController {
	private final static Logger logger = LoggerFactory.getLogger(RegistryController.class);

	@Inject
	MicroServiceUtils microServiceUtils;
	@Inject
	CSourceService csourceService;
	@ConfigProperty(name = "scorpio.entity.default-limit")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.entity.max-limit")
	int maxLimit;

	@ConfigProperty(name = "scorpio.ngsild.corecontext")
	String coreContext;

	@Inject
	JsonLDService ldService;

	@GET
	@Counted(value = "registration_query_total", description = "Total number of registration query requests")
	@Timed(value = "registration_query_duration", description = "Duration of registration query requests")
	@Timed(value = "registration_query_concurrent", longTask = true, description = "Number of concurrent registration query requests")
	public Uni<RestResponse<Object>> queryCSource(HttpServerRequest request, @QueryParam("id") String ids,
			@QueryParam("type") String type, @QueryParam("idPattern") String idPattern,
			@QueryParam("attrs") String attrs, @QueryParam("q") String q, @QueryParam("csf") String csf,
			@QueryParam("geometry") String geometry, @QueryParam("georel") String georel,
			@QueryParam("coordinates") String coordinates, @QueryParam("geoproperty") String geoproperty,
			@QueryParam("geometryProperty") String geometryProperty, @QueryParam("timeproperty") String timeProperty,
			@QueryParam("timerel") String timerel, @QueryParam("scopeQ") String scopeQ,
			@QueryParam("timeAt") String timeAt, @QueryParam("endTimeAt") String endTimeAt,
			@QueryParam(value = "limit") Integer limit, @QueryParam(value = "offset") int offset,
			@QueryParam(value = "options") String options, @QueryParam(value = "count") boolean count) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader != 1 && acceptHeader != 2) {
			return HttpUtils.getInvalidHeader();
		}
		int actualLimit;
		if (limit == null) {
			actualLimit = defaultLimit;
		} else {
			actualLimit = limit;
		}
		if (actualLimit > maxLimit) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.TooManyResults), HttpUtils.getTenant(request)));
		}
		if (ids == null && type == null && attrs == null && geometry == null && q == null) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData, "At least one of id, type, attrs, q or geometry must be provided"), HttpUtils.getTenant(request)));
		}
		if (ids != null) {
			try {
				HttpUtils.validateUri(ids);
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
			}
		}
		Set<String> finalOptions;
		try {
			finalOptions = HttpUtils.parseOptionsAndFormat(options, null);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		List<Object> headerContext = HttpUtils.getAtContext(request);
		return ldService.parse(headerContext).onItem().transformToUni(context -> {
			AttrsQueryTerm attrsQuery;
			TypeQueryTerm typeQueryTerm;
			QQueryTerm qQueryTerm;
			CSFQueryTerm csfQueryTerm;
			GeoQueryTerm geoQueryTerm;
			ScopeQueryTerm scopeQueryTerm;

			try {
				attrsQuery = QueryParser.parseAttrs(attrs, context);
				typeQueryTerm = QueryParser.parseTypeQuery(type, context);
				qQueryTerm = QueryParser.parseQuery(q, context);
				csfQueryTerm = QueryParser.parseCSFQuery(csf, context);
				geoQueryTerm = QueryParser.parseGeoQuery(georel, coordinates, geometry, geoproperty, context);
				scopeQueryTerm = QueryParser.parseScopeQuery(scopeQ);
			} catch (Exception e) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
			}
			if (qQueryTerm != null && qQueryTerm.getOperator().isEmpty()) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
						new ResponseException(ErrorType.BadRequestData), HttpUtils.getTenant(request)));
			}
			return csourceService
					.queryRegistrations(HttpUtils.getTenant(request),
							ids == null ? null : Sets.newHashSet(ids.split(",")), typeQueryTerm, idPattern, attrsQuery,
							csfQueryTerm, geoQueryTerm, scopeQueryTerm, qQueryTerm, actualLimit, offset, count)
					.onItem().transformToUni(queryResult -> {
						return HttpUtils.generateQueryResult(request, queryResult, finalOptions, geometryProperty,
								acceptHeader, count, actualLimit, null, context, ldService, false,
								microServiceUtils.getGatewayString(), NGSIConstants.NGSI_LD_REGISTRY_ENDPOINT, -1);
					});
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
	}

	@POST
	@Counted(value = "registration_create_total", description = "Total number of registration create requests")
	@Timed(value = "registration_create_duration", description = "Duration of registration create requests")
	@Timed(value = "registration_create_concurrent", longTask = true, description = "Number of concurrent registration create requests")
	public Uni<RestResponse<Object>> registerCSource(HttpServerRequest request, String payload) {
		JsonObject jsonObject;
		try {
			jsonObject = new JsonObject(payload);
		} catch (DecodeException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}

		if (jsonObject.containsKey(NGSIConstants.CONTEXT_SOURCE_INFO)) {
			for (Object obj : jsonObject.getJsonArray(NGSIConstants.CONTEXT_SOURCE_INFO)) {
				JsonObject jsonObject1 = (JsonObject) obj;
				if (jsonObject1.getString("key").equalsIgnoreCase("Accept") && !List
						.of("application/json", "application/ld+json").contains(jsonObject1.getString("value"))) {
					return Uni.createFrom()
							.item(HttpUtils.handleControllerExceptions(
									new ResponseException(ErrorType.NotAcceptable,
											"Accept should be application/json or application/ld+json"),
									HttpUtils.getTenant(request)));
				}
			}
		}
		return HttpUtils.expandBody(request, payload, AppConstants.CSOURCE_REG_CREATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return csourceService.createRegistration(HttpUtils.getTenant(request), tuple.getItem2()).onItem()
							.transform(opResult -> {
								return HttpUtils.generateCreateResult(opResult, AppConstants.CSOURCE_URL);
							});
				}).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));

	}


	@Path("/{registrationId}")
	@GET
	@Counted(value = "registration_retrieve_total", description = "Total number of registration retrieve requests")
	@Timed(value = "registration_retrieve_duration", description = "Duration of registration retrieve requests")
	@Timed(value = "registration_retrieve_concurrent", longTask = true, description = "Number of concurrent registration retrieve requests")
	public Uni<RestResponse<Object>> getCSourceById(HttpServerRequest request,
			@PathParam("registrationId") String registrationId) {
		logger.debug("get CSource() ::" + registrationId);
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader != 1 && acceptHeader != 2) {
			return HttpUtils.getInvalidHeader();
		}

		try {
			HttpUtils.validateUri(registrationId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		List<Object> headerContext = HttpUtils.getAtContext(request);
		return ldService.parse(headerContext).onItem().transformToUni(context -> {
			return csourceService.retrieveRegistration(HttpUtils.getTenant(request), registrationId).onItem()
					.transformToUni(entity -> {
						return HttpUtils.generateRegistryResult(headerContext, context, acceptHeader, entity, ldService,
								true);
					});
		}).onFailure().recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
	}

	@Path("/{registrationId}")
	@PATCH
	@Counted(value = "registration_patch_total", description = "Total number of registration patch requests")
	@Timed(value = "registration_patch_duration", description = "Duration of registration patch requests")
	@Timed(value = "registration_patch_concurrent", longTask = true, description = "Number of concurrent registration patch requests")
	public Uni<RestResponse<Object>> updateCSource(HttpServerRequest request,
			@PathParam("registrationId") String registrationId, String payload) {
		// NGSI-LD 5.9.x: a member set to null in a registration update means "remove this member".
		// JSON-LD expansion drops nulls, so capture the nulled member names from the raw body here.
		Set<String> nullMembers = Sets.newHashSet();
		try {
			for (var e : new JsonObject(payload).getMap().entrySet()) {
				if (e.getValue() == null && !NGSIConstants.JSON_LD_CONTEXT.equals(e.getKey())) {
					nullMembers.add(e.getKey());
				}
			}
		} catch (DecodeException ignored) {
			// let expandBody surface the proper BadRequest below
		}
		// NGSI-LD 5.9.x: `information` (and `endpoint`) are mandatory — they must not be removed via null.
		if (nullMembers.contains(NGSIConstants.CSOURCE_INFORMATION)
				|| nullMembers.contains(NGSIConstants.CSOURCE_ENDPOINT)) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.BadRequestData, "A mandatory member cannot be removed"),
					HttpUtils.getTenant(request)));
		}
		return HttpUtils.expandBody(request, payload, AppConstants.CSOURCE_REG_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					Set<String> removeMembers = Sets.newHashSet();
					for (String m : nullMembers) {
						removeMembers.add(tuple.getItem1().expandIri(m, false, true, null, null));
					}
					return csourceService
							.updateRegistration(HttpUtils.getTenant(request), registrationId, tuple.getItem2(),
									removeMembers)
							.onItem().transform(opResult -> {
								return HttpUtils.generateUpdateResultResponse(opResult);
							});
				}).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
	}

	@Path("/{registrationId}")
	@DELETE
	@Counted(value = "registration_delete_total", description = "Total number of registration delete requests")
	@Timed(value = "registration_delete_duration", description = "Duration of registration delete requests")
	@Timed(value = "registration_delete_concurrent", longTask = true, description = "Number of concurrent registration delete requests")
	public Uni<RestResponse<Object>> deleteCSource(HttpServerRequest request,
			@PathParam("registrationId") String registrationId) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		if (acceptHeader == -1) {
			return HttpUtils.getInvalidHeader();
		}
		try {
			HttpUtils.validateUri(registrationId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return csourceService.deleteRegistration(HttpUtils.getTenant(request), registrationId).onItem()
				.transform(opResult -> {
					return HttpUtils.generateDeleteResult(opResult);
				}).onFailure()
				.recoverWithItem(e -> HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
	}

}
