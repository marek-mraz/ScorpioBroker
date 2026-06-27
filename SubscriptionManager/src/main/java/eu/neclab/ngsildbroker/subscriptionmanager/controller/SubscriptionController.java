package eu.neclab.ngsildbroker.subscriptionmanager.controller;

import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.google.common.collect.Lists;
import com.google.common.net.HttpHeaders;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.subscriptionmanager.service.SubscriptionService;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.impl.headers.HeadersMultiMap;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import jakarta.inject.Inject;
import jakarta.ws.rs.DELETE;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.PATCH;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.metrics.MetricUnits;
import org.eclipse.microprofile.metrics.annotation.ConcurrentGauge;
import org.eclipse.microprofile.metrics.annotation.Counted;
import org.eclipse.microprofile.metrics.annotation.Timed;
import org.jboss.resteasy.reactive.RestResponse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Path("/ngsi-ld/v1/subscriptions")
public class SubscriptionController {

	// private final static Logger logger =
	// LoggerFactory.getLogger(SubscriptionController.class);

	@Inject
	SubscriptionService subService;

	@Inject
	MicroServiceUtils microServiceUtils;

	@ConfigProperty(name = "scorpio.ngsild.corecontext")
	String coreContext;

	@ConfigProperty(name = "scorpio.subscription.default-limit")
	int defaultLimit;
	@ConfigProperty(name = "scorpio.subscription.max-limit")
	int maxLimit;

	@Inject
	JsonLDService ldService;

	@POST
	public Uni<RestResponse<Object>> subscribe(HttpServerRequest request, String body) {
		Map<String, Object> map;
		String tenant = HttpUtils.getTenant(request);
		try {
			map = new JsonObject(body).getMap();
		} catch (DecodeException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}

		// NGSI-LD 5.8.1: jsonldContext must be a URI. A non-absolute value (e.g. "unknownContext") is
		// invalid input -> 400 BadRequestData, distinct from a resolvable URL that is merely unreachable
		// (-> 503 LdContextNotAvailable). Validate before expansion, which would otherwise try to
		// dereference the value and surface a 503.
		Object jsonldCtx = map.get(NGSIConstants.JSONLD_CONTEXT);
		if (jsonldCtx instanceof String jsonldCtxStr) {
			boolean invalid;
			try {
				invalid = !new java.net.URI(jsonldCtxStr).isAbsolute();
			} catch (java.net.URISyntaxException ex) {
				invalid = true;
			}
			if (invalid) {
				return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
						new ResponseException(ErrorType.BadRequestData, "jsonldContext must be a valid absolute URI"),
						tenant));
			}
		}

		// try {
		// if (!map.containsKey(NGSIConstants.JSONLD_CONTEXT)) {
		// Object contextLink;
		// if (request.getHeader(NGSIConstants.LINK_HEADER) != null) {
		// contextLink =
		// request.getHeader(NGSIConstants.LINK_HEADER).split(";")[0].replace("<", "")
		// .replace(">", "");
		// } else if (map.containsKey(JsonLdConsts.CONTEXT)) {
		// contextLink = map.get(JsonLdConsts.CONTEXT);
		// } else {
		// contextLink = coreContext;
		// }
		// map.put(NGSIConstants.JSONLD_CONTEXT, contextLink);
		// }
		// } catch (Exception e) {
		// return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
		// new ResponseException(ErrorType.BadRequestData),
		// tenant));
		// }
		HeadersMultiMap otherHead = new HeadersMultiMap();
		if (request.headers().contains(NGSIConstants.TENANT_HEADER)) {
			otherHead.add(NGSIConstants.TENANT_HEADER, request.headers().get(NGSIConstants.TENANT_HEADER));
		}

		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}

		return HttpUtils.expandBody(request, map, AppConstants.SUBSCRIPTION_CREATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					Uni<Context> contextLink;
					if (map.containsKey(NGSIConstants.JSONLD_CONTEXT)) {
						contextLink = ldService.parse(map.get(NGSIConstants.JSONLD_CONTEXT));
					} else {
						contextLink = Uni.createFrom().item(tuple.getItem1());
					}
					return contextLink.onItem().transformToUni(ctx -> {
						return subService
								.createSubscription(otherHead, tenant, tuple.getItem2(),
										ctx, viaHeaders)
								.onItem().transform(t -> HttpUtils.generateSubscriptionResult(t, tuple.getItem1()));
					});
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, tenant);
				});
	}

	@GET
	@Counted(name = "retrieve_all_subscriptions_total", description = "Total number of retrieve all subscriptions requests", absolute = true)
	@Timed(name = "retrieve_all_subscriptions_duration", description = "Duration of retrieve all subscriptions requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "retrieve_all_subscriptions_concurrent", description = "Number of concurrent retrieve all subscriptions requests", absolute = true)
	public Uni<RestResponse<Object>> getAllSubscriptions(HttpServerRequest request, @QueryParam("limit") Integer limit,
			@QueryParam("offset") int offset, @QueryParam("options") String options) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		String tenant = HttpUtils.getTenant(request);
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
					new ResponseException(ErrorType.TooManyResults), tenant));
		}
		if (offset < 0) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(
					new ResponseException(ErrorType.InvalidRequest, "invalid offset"), tenant));
		}
		Set<String> finalOptions;
		try {
			finalOptions = HttpUtils.parseOptionsAndFormat(options, null);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return ldService.parse(HttpUtils.getAtContext(request)).onItem().transformToUni(ctx -> {
			return subService.getAllSubscriptions(tenant, actualLimit, offset).onItem()
					.transformToUni(subscriptions -> {
						subscriptions.getData().forEach(sub -> {
							fixSub(sub);
						});
						return HttpUtils.generateQueryResult(request, subscriptions, finalOptions, null, acceptHeader,
								false,
								actualLimit, null, ctx, ldService, false, microServiceUtils.getGatewayString(),
								NGSIConstants.NGSI_LD_SUB_ENDPOINT, -1);
					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, tenant);
		});

	}

	private void fixSub(Map<String, Object> sub) {

		// On create the broker hosts the supplied @context implicitly and stores a synthesized
		// jsonldContext URL pointing at /jsonldContexts/ (SubscriptionService). Per NGSI-LD 5.2.12 and
		// the Subscription Behaviour clause, jsonldContext is a legitimate output member of the
		// Subscription (the @context used when sending notifications). Keep it: it compacts to the
		// "jsonldContext" term carrying the URL string. Representation-comparison tests that don't care
		// about it list it in their ignore_keys.

		// notificationTrigger defaults to attributeCreated + attributeUpdated when not supplied
		// (NGSI-LD 5.2.12). The default is materialized on the Subscription object but not the stored
		// payload, so reflect it in the representation when absent so retrieve/query match the spec.
		if (!sub.containsKey(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER)) {
			sub.put(NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER, List.of(
					Map.of(NGSIConstants.JSON_LD_VALUE,
							NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ATTRIBUTE_CREATED),
					Map.of(NGSIConstants.JSON_LD_VALUE,
							NGSIConstants.NGSI_LD_NOTIFICATION_TRIGGER_ATTRIBUTE_UPDATED)));
		}

		Map<String, Object> notificationParam = ((List<Map<String, Object>>) sub
				.get(NGSIConstants.NGSI_LD_NOTIFICATION)).get(0);
		// timesSent/timesFailed are output-only and restricted to "Greater than 0" (NGSI-LD 5.2.14.2):
		// only surface them once present (i.e. a notification has been attempted), never as 0/null.
		Object timesSent = sub.remove(NGSIConstants.NGSI_LD_TIMES_SENT);
		if (timesSent != null) {
			notificationParam.put(NGSIConstants.NGSI_LD_TIMES_SENT, timesSent);
		}
		Object timesFailed = sub.remove(NGSIConstants.NGSI_LD_TIMES_FAILED);
		if (timesFailed != null) {
			notificationParam.put(NGSIConstants.NGSI_LD_TIMES_FAILED, timesFailed);
		}
		Object lastNotification = sub.remove(NGSIConstants.NGSI_LD_LAST_NOTIFICATION);
		Object lastSuccess = sub.remove(NGSIConstants.NGSI_LD_LAST_SUCCESS);
		Object lastFailure = sub.remove(NGSIConstants.NGSI_LD_LAST_FAILURE);
		if (lastNotification != null) {
			notificationParam.put(NGSIConstants.NGSI_LD_LAST_NOTIFICATION, lastNotification);
			if (lastSuccess != null) {
				notificationParam.put(NGSIConstants.NGSI_LD_LAST_SUCCESS, lastSuccess);
			}
			if (lastFailure != null) {
				notificationParam.put(NGSIConstants.NGSI_LD_LAST_FAILURE, lastFailure);
			}
			// NGSI-LD 5.2.14 NotificationParams.status: "ok" / "failed", derived from the most
			// recent delivery outcome (only meaningful once a notification has been attempted).
			String status;
			if (lastFailure == null) {
				status = "ok";
			} else if (lastSuccess == null) {
				status = "failed";
			} else {
				status = notificationDateValue(lastFailure).compareTo(notificationDateValue(lastSuccess)) >= 0
						? "failed"
						: "ok";
			}
			notificationParam.put(NGSIConstants.NGSI_LD_STATUS,
					List.of(Map.of(NGSIConstants.JSON_LD_VALUE, status)));
		}

	}

	@SuppressWarnings("unchecked")
	private static String notificationDateValue(Object expandedDate) {
		return (String) ((List<Map<String, Object>>) expandedDate).get(0).get(NGSIConstants.JSON_LD_VALUE);
	}

	@Path("/{id}")
	@GET
	@Counted(name = "retrieve_subscription_total", description = "Total number of retrieve subscription requests", absolute = true)
	@Timed(name = "retrieve_subscription_duration", description = "Duration of retrieve subscription requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "retrieve_subscription_concurrent", description = "Number of concurrent retrieve subscription requests", absolute = true)
	public Uni<RestResponse<Object>> getSubscriptionById(HttpServerRequest request,
			@PathParam(value = "id") String subscriptionId, @QueryParam(value = "options") String options) {
		int acceptHeader = HttpUtils.parseAcceptHeader(request.headers().getAll("Accept"));
		String tenant = HttpUtils.getTenant(request);
		if (acceptHeader != 1 && acceptHeader != 2) {
			return HttpUtils.getInvalidHeader();
		}
		try {
			HttpUtils.validateUri(subscriptionId);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}

		List<Object> contextHeader = HttpUtils.getAtContext(request);
		Set<String> finalOptions;
		try {
			finalOptions = HttpUtils.parseOptionsAndFormat(options, null);
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, HttpUtils.getTenant(request)));
		}
		return ldService.parse(contextHeader).onItem().transformToUni(context -> {
			return subService.getSubscription(tenant, subscriptionId).onItem()
					.transformToUni(subscription -> {
						fixSub(subscription);

						return HttpUtils.generateSubscriptionResult(contextHeader, context, acceptHeader, subscription,
								finalOptions, ldService, true);
					});
		}).onFailure().recoverWithItem(e -> {
			return HttpUtils.handleControllerExceptions(e, tenant);
		});
	}

	@Path("/{id}")
	@DELETE
	@Counted(name = "delete_subscription_total", description = "Total number of delete subscription requests", absolute = true)
	@Timed(name = "delete_subscription_duration", description = "Duration of delete subscription requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "delete_subscription_concurrent", description = "Number of concurrent delete subscription requests", absolute = true)
	public Uni<RestResponse<Object>> deleteSubscription(HttpServerRequest request, @PathParam(value = "id") String id) {
		String tenant = HttpUtils.getTenant(request);
		try {
			HttpUtils.validateUri(id);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		return subService.deleteSubscription(tenant, id).onItem()
				.transform(t -> HttpUtils.generateDeleteResult(t)).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, tenant);
				});

	}

	@Path("/{id}")
	@PATCH
	@Counted(name = "patch_subscription_total", description = "Total number of patch subscription requests", absolute = true)
	@Timed(name = "patch_subscription_duration", description = "Duration of patch subscription requests", unit = MetricUnits.MILLISECONDS, absolute = true)
	@ConcurrentGauge(name = "patch_subscription_concurrent", description = "Number of concurrent patch subscription requests", absolute = true)
	public Uni<RestResponse<Object>> updateSubscription(HttpServerRequest request, @PathParam(value = "id") String id,
			String body) {
		Map<String, Object> map;
		String tenant = HttpUtils.getTenant(request);
		ViaHeaders viaHeaders;
		try {
			viaHeaders = new ViaHeaders(request.headers().getAll(HttpHeaders.VIA),
					microServiceUtils.getSourceAlias(tenant));
		} catch (ResponseException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		try {
			map = new JsonObject(body).getMap();
		} catch (DecodeException e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}
		try {
			HttpUtils.validateUri(id);
		} catch (Exception e) {
			return Uni.createFrom().item(HttpUtils.handleControllerExceptions(e, tenant));
		}

		List<String> contexts;
		Object ctxObj = map.get("@context");
		if (ctxObj instanceof List) {
			contexts = (List<String>) ctxObj;
		} else if (ctxObj instanceof String s) {
			contexts = Lists.newArrayList(s);
		} else {
			contexts = null;
		}
		List<String> finalContexts = new ArrayList<>();
		if (contexts != null) {
			for (String url : contexts) {
				url = url + "?type=implicitlyCreated";
				finalContexts.add(url);
			}
			map.put("@context", finalContexts);
		}

		return HttpUtils.expandBody(request, map, AppConstants.SUBSCRIPTION_UPDATE_PAYLOAD, ldService).onItem()
				.transformToUni(tuple -> {
					return subService
							.updateSubscription(tenant, id, tuple.getItem2(), tuple.getItem1(),
									viaHeaders)
							.onItem().transform(t -> HttpUtils.generateSubscriptionResult(t, tuple.getItem1()));
				}).onFailure().recoverWithItem(e -> {
					return HttpUtils.handleControllerExceptions(e, tenant);
				});

	}
}
