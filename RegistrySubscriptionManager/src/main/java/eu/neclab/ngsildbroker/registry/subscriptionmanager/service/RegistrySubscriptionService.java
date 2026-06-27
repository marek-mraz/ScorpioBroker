package eu.neclab.ngsildbroker.registry.subscriptionmanager.service;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.EntityInfo;
import eu.neclab.ngsildbroker.commons.datatypes.NotificationParam;
import eu.neclab.ngsildbroker.commons.datatypes.Subscription;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.DeleteSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.SubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.subscription.UpdateSubscriptionRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.datatypes.results.QueryResult;
import eu.neclab.ngsildbroker.commons.datatypes.terms.TypeQueryTerm;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.CSourceHandler;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import eu.neclab.ngsildbroker.commons.tools.SerializationTools;
import eu.neclab.ngsildbroker.commons.tools.SubscriptionTools;

import eu.neclab.ngsildbroker.registry.subscriptionmanager.messaging.SyncService;
import eu.neclab.ngsildbroker.registry.subscriptionmanager.repository.RegistrySubscriptionInfoDAO;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.quarkus.runtime.Startup;
import io.quarkus.scheduler.Scheduled;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.smallrye.mutiny.tuples.Tuple2;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.WebClient;
import io.vertx.mutiny.mqtt.MqttClient;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowIterator;

@ApplicationScoped
@Startup
public class RegistrySubscriptionService implements CSourceHandler {

	private final static Logger logger = LoggerFactory.getLogger(RegistrySubscriptionService.class);

	@Inject
	RegistrySubscriptionInfoDAO regDAO;

	// @Inject
	// @Channel(AppConstants.INTERNAL_NOTIFICATION_CHANNEL)
	// @Broadcast
	// MutinyEmitter<String> internalNotificationSender;

	@Inject
	ObjectMapper objectMapper;

	@ConfigProperty(name = "scorpio.messaging.maxSize")
	int messageSize;

	@Inject
	Vertx vertx;

	private Table<String, String, SubscriptionRequest> tenant2subscriptionId2Subscription = HashBasedTable.create();

	private Table<String, String, SubscriptionRequest> tenant2subscriptionId2IntervalSubscription = HashBasedTable
			.create();

	private WebClient webClient;

	private Map<String, MqttClient> host2MqttClient = Maps.newHashMap();

	private SyncService subscriptionSyncService;

	@Inject
	JsonLDService ldService;

	@ConfigProperty(name = "scorpio.alltypesub.type")
	private String allTypeSubType;

	@ConfigProperty(name = "scorpio.subscription.notification.worker-thread", defaultValue = "false")
	boolean notificationOnWorkerThread;

	@Inject
	MicroServiceUtils microServiceUtils;
	private String ALL_TYPES_SUB;

	@PostConstruct
	void startup() {
		this.webClient = WebClient.create(vertx);
		ALL_TYPES_SUB = NGSIConstants.NGSI_LD_DEFAULT_PREFIX + allTypeSubType;
		regDAO.loadSubscriptions().onItem().transformToUni(subs -> {
			List<Uni<Tuple2<Tuple2<String, Map<String, Object>>, Context>>> unis = Lists.newArrayList();
			subs.forEach(tuple -> {
				unis.add(ldService.parsePure(tuple.getItem3().get(NGSIConstants.JSON_LD_CONTEXT)).onItem()
						.transform(ctx -> {
							return Tuple2.of(Tuple2.of(tuple.getItem1(), tuple.getItem2()), ctx);
						}));
			});
			if (unis.isEmpty()) {
				return Uni.createFrom().voidItem();
			}
			return Uni.combine().all().unis(unis).with(list -> {
				for (Object obj : list) {
					@SuppressWarnings("unchecked")
					Tuple2<Tuple2<String, Map<String, Object>>, Context> tuple = (Tuple2<Tuple2<String, Map<String, Object>>, Context>) obj;
					SubscriptionRequest request;
					try {
						request = new SubscriptionRequest(tuple.getItem1().getItem1(), tuple.getItem1().getItem2(),
								tuple.getItem2());
						if (isIntervalSub(request)) {
							this.tenant2subscriptionId2IntervalSubscription.put(request.getTenant(), request.getId(),
									request);
						} else {
							this.tenant2subscriptionId2Subscription.put(request.getTenant(), request.getId(), request);
						}
					} catch (Exception e) {
						logger.error("Failed to load stored subscription " + tuple.getItem1());
					}
				}
				return null;
			});
		}).await().indefinitely();
		this.microServiceUtils.registerCSourceReceiver(this);

	}

	private boolean isIntervalSub(SubscriptionRequest request) {
		return request.getSubscription().getTimeInterval() > 0;
	}

	public Uni<NGSILDOperationResult> createSubscription(String tenant, Map<String, Object> subscription,
			Context context) {
		// NGSI-LD 5.11.2: if no id is provided, the broker assigns one (and returns it via Location).
		if (!subscription.containsKey(NGSIConstants.JSON_LD_ID)) {
			subscription.put(NGSIConstants.JSON_LD_ID, "urn:" + java.util.UUID.randomUUID());
		}
		SubscriptionRequest request;
		try {
			request = new SubscriptionRequest(tenant, subscription, context);
		} catch (ResponseException e) {
			return Uni.createFrom().failure(e);
		}

		// NGSI-LD 5.11.2: an expiresAt in the past is invalid input.
		Long expiresAt = request.getSubscription().getExpiresAt();
		if (expiresAt != null && expiresAt < System.currentTimeMillis()) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData,
					"expiresAt must not be in the past"));
		}

		// Do NOT seed timesSent/timesFailed=0 into a fresh csource subscription: per NGSI-LD these
		// notification stats must only appear once a notification has actually been (attempted to be)
		// sent. The increment SQL is COALESCE-safe, so it works without a pre-seeded value.
		return regDAO.createSubscription(request).onItem().transformToUni(t -> {
			if (isIntervalSub(request)) {
				this.tenant2subscriptionId2IntervalSubscription.put(request.getTenant(), request.getId(), request);
			} else {
				tenant2subscriptionId2Subscription.put(tenant, request.getId(), request);
			}
			Uni<Void> syncService;
			if (subscriptionSyncService != null) {
				syncService = subscriptionSyncService.sync(request);
			} else {
				syncService = Uni.createFrom().voidItem();
			}
			return syncService.onItem().transformToUni(v2 -> {
				if (request.getSubscription().getIsActive() != null && !request.getSubscription().getIsActive()) {
					NGSILDOperationResult result = new NGSILDOperationResult(
							AppConstants.CREATE_SUBSCRIPTION_REQUEST, request.getId(), tenant);
					result.addSuccess(
							new CRUDSuccess(null, null, request.getId(), Sets.newHashSet()));
					return Uni.createFrom().item(result);
				}
				return regDAO.getInitialNotificationData(request).onFailure().recoverWithUni(e -> {
					e.printStackTrace();
					return Uni.createFrom().failure(e);
				}).onItem().transformToUni(rows -> {
					List<Map<String, Object>> data = Lists.newArrayList();
					rows.forEach(row -> {
						data.add(row.getJsonObject(0).getMap());
					});
					// NGSI-LD 5.11.x: like entity subscriptions, only fire the initial cSourceNotification
					// when there is matching data. A no-match subscription must not emit an empty
					// notification (which would otherwise seed timesSent/timesFailed on a fresh sub).
					if (data.isEmpty()) {
						NGSILDOperationResult result = new NGSILDOperationResult(
								AppConstants.CREATE_SUBSCRIPTION_REQUEST, request.getId(), tenant);
						result.addSuccess(new CRUDSuccess(null, null, request.getId(), Sets.newHashSet()));
						return Uni.createFrom().item(result);
					}
					// NGSI-LD 5.11.7: the initial-on-subscription cSourceNotification carries
					// triggerReason "newlyMatching" (CREATE_REQUEST), not the internal sentinel
					// (INTERNAL_NOTIFICATION_REQUEST mapped to a null triggerReason → conformance fail).
					return SubscriptionTools.generateCsourceNotification(request, data,
							AppConstants.CREATE_REQUEST, ldService).onFailure().recoverWithUni(e -> {
								e.printStackTrace();
								return Uni.createFrom().failure(e);
							}).onItem().transformToUni(noti -> {
								return deliverNotification(request, noti)
										.onFailure().recoverWithUni(e -> {
											e.printStackTrace();
											return Uni.createFrom().failure(e);
										}).onItem().transform(v -> {
											NGSILDOperationResult result = new NGSILDOperationResult(
													AppConstants.CREATE_SUBSCRIPTION_REQUEST, request.getId(), tenant);
											result.addSuccess(
													new CRUDSuccess(null, null, request.getId(), Sets.newHashSet()));
											return result;
										});
							});

				});
			});

		}).onFailure().recoverWithUni(e -> {
			// TODO sql check
			logger.error("failed to create csource subscription.", e);
			return Uni.createFrom().failure(new ResponseException(ErrorType.AlreadyExists,
					"Subscription with id " + request.getId() + " exists"));
		});
	}

	public Uni<NGSILDOperationResult> updateSubscription(String tenant, String subscriptionId,
			Map<String, Object> update, Context context) {
		UpdateSubscriptionRequest request = new UpdateSubscriptionRequest(tenant, subscriptionId, update, context);
		return regDAO.updateSubscription(request).onItem().transformToUni(tup -> {
			if (tup.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "subscription not found"));
			}
			return ldService.parsePure(tup.getItem2()).onItem().transformToUni(ctx -> {
				SubscriptionRequest updatedRequest;
				try {
					updatedRequest = new SubscriptionRequest(tenant, tup.getItem1(), ctx);
				} catch (Exception e) {
					return Uni.createFrom().failure(e);
				}

				if (isIntervalSub(updatedRequest)) {
					tenant2subscriptionId2IntervalSubscription.put(tenant, updatedRequest.getId(), updatedRequest);
					tenant2subscriptionId2Subscription.remove(tenant, updatedRequest.getId());
				} else {
					tenant2subscriptionId2Subscription.put(tenant, updatedRequest.getId(), updatedRequest);
					tenant2subscriptionId2IntervalSubscription.remove(tenant, updatedRequest.getId());
				}
				Uni<Void> syncService;
				if (subscriptionSyncService != null) {
					syncService = subscriptionSyncService.sync(updatedRequest);
				} else {
					syncService = Uni.createFrom().voidItem();
				}
				return syncService.onItem().transformToUni(v2 -> {
					NGSILDOperationResult result = new NGSILDOperationResult(
							AppConstants.UPDATE_SUBSCRIPTION_REQUEST, subscriptionId, tenant);
					// NGSI-LD 5.11.7: on a (re)matching subscription update, send a cSourceNotification
					// listing all currently matching Context Source Registrations (triggerReason
					// "newlyMatching"). Skip for inactive or interval subscriptions (the latter notify on
					// their own timer) and never emit an empty notification.
					Subscription updatedSub = updatedRequest.getSubscription();
					boolean active = updatedSub.getIsActive() == null || updatedSub.getIsActive();
					if (!active || isIntervalSub(updatedRequest)) {
						return Uni.createFrom().item(result);
					}
					return regDAO.getInitialNotificationData(updatedRequest).onItem().transformToUni(rows -> {
						List<Map<String, Object>> data = Lists.newArrayList();
						rows.forEach(row -> data.add(row.getJsonObject(0).getMap()));
						if (data.isEmpty()) {
							return Uni.createFrom().item(result);
						}
						return SubscriptionTools
								.generateCsourceNotification(updatedRequest, data, AppConstants.CREATE_REQUEST, ldService)
								.onItem().transformToUni(noti -> deliverNotification(updatedRequest, noti)
										.onItem().transform(v -> result));
					}).onFailure().recoverWithItem(e -> {
						logger.debug("failed to send update notification for csource subscription", e);
						return result;
					});
				});
			});
		});
	}

	public Uni<NGSILDOperationResult> deleteSubscription(String tenant, String subscriptionId) {
		DeleteSubscriptionRequest request = new DeleteSubscriptionRequest(tenant, subscriptionId);
		return regDAO.deleteSubscription(request).onItem().transformToUni(t -> {
			tenant2subscriptionId2IntervalSubscription.remove(tenant, subscriptionId);
			tenant2subscriptionId2Subscription.remove(tenant, subscriptionId);
			Uni<Void> syncService;
			if (subscriptionSyncService != null) {
				syncService = subscriptionSyncService.sync(request);
			} else {
				syncService = Uni.createFrom().voidItem();
			}
			return syncService.onItem().transform(v2 -> {
				return new NGSILDOperationResult(AppConstants.DELETE_SUBSCRIPTION_REQUEST, subscriptionId, tenant);
			});
		});
	}

	public Uni<QueryResult> getAllSubscriptions(String tenant, int limit, int offset) {
		return regDAO.getAllSubscriptions(tenant, limit, offset).onItem().transform(rows -> {
			QueryResult result = new QueryResult(tenant);
			Row next = null;
			RowIterator<Row> it = rows.iterator();
			List<Map<String, Object>> resultData = new ArrayList<Map<String, Object>>(rows.size());
			while (it.hasNext()) {
				next = it.next();
				Map<String, Object> subMap = next.getJsonObject(0).getMap();
				// NGSI-LD 5.2.12 / 5.8.2.4: status is read-only and system-computed when querying.
				// The stored payload has no status, so derive it (expired/active/paused) per subscription
				// from the in-memory Subscription — mirrors getSubscription() for the retrieve-by-id path.
				SubscriptionRequest req = tenant2subscriptionId2Subscription.get(tenant,
						(String) subMap.get(NGSIConstants.JSON_LD_ID));
				if (req == null) {
					req = tenant2subscriptionId2IntervalSubscription.get(tenant,
							(String) subMap.get(NGSIConstants.JSON_LD_ID));
				}
				if (req != null) {
					Long expiresAt = req.getSubscription().getExpiresAt();
					subMap.put(NGSIConstants.STATUS, (expiresAt != null && expiresAt < System.currentTimeMillis())
							? "expired"
							: req.getSubscription().getStatus());
				}
				resultData.add(subMap);
			}
			result.setData(resultData);
			if (next == null) {
				return result;
			}
			// Total matching count comes from `count(*) over()` (column 1), NOT rows.size() (the page
			// size) — otherwise leftAfter went negative and the rel="next" pagination Link was dropped.
			long resultCount = next.getLong(1);
			result.setCount(resultCount);
			long leftAfter = resultCount - (offset + limit);
			if (leftAfter < 0) {
				leftAfter = 0;
			}
			long leftBefore = offset;
			result.setResultsLeftAfter(leftAfter);
			result.setResultsLeftBefore(leftBefore);
			result.setLimit(limit);
			result.setOffset(offset);
			return result;
		});
	}

	public Uni<Map<String, Object>> getSubscription(String tenant, String subscriptionId) {
		return regDAO.getSubscription(tenant, subscriptionId).onItem().transformToUni(rows -> {
			if (rows.size() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound, "subscription not found"));
			}
			Map<String, Object> result = rows.iterator().next().getJsonObject(0).getMap();
			SubscriptionRequest req = tenant2subscriptionId2Subscription.get(tenant, subscriptionId);
			if (req == null) {
				req = tenant2subscriptionId2IntervalSubscription.get(tenant, subscriptionId);
			}
			if (req != null) {
				// NGSI-LD 5.2.12 / 5.8.2.4: status is read-only and system-computed when querying.
				// Once expiresAt is in the past the subscription is "expired" (no scheduler flips the
				// stored value, so derive it on read).
				Long expiresAt = req.getSubscription().getExpiresAt();
				String status = (expiresAt != null && expiresAt < System.currentTimeMillis())
						? "expired"
						: req.getSubscription().getStatus();
				result.put(NGSIConstants.STATUS, status);
			}
			return Uni.createFrom().item(result);
		});
	}

	public Uni<Void> handleRegistryChange(CSourceBaseRequest message) {
		// The table is keyed (row=tenant, column=subscriptionId); all subscriptions of a tenant are its
		// row, not its column. Using column(tenant) returned nothing, so registry-change notifications
		// (newlyMatching / updated / noLongerMatching, NGSI-LD 5.11.7) were never sent.
		Collection<SubscriptionRequest> potentialSubs = tenant2subscriptionId2Subscription.row(message.getTenant())
				.values();
		List<Uni<Void>> unis = Lists.newArrayList();
		for (SubscriptionRequest potentialSub : potentialSubs) {
			switch (message.getRequestType()) {
				case AppConstants.UPDATE_REQUEST:
				case AppConstants.APPEND_REQUEST: {
					// NGSI-LD 5.11.7: a registration update fires a cSourceNotification whose triggerReason
					// depends on the match state before vs after the change (updated / newlyMatching /
					// noLongerMatching). Registration updates are emitted as APPEND_REQUEST, so this case
					// MUST cover it (previously only UPDATE_REQUEST was handled -> updates never notified).
					Map<String, Object> newReg = message.getPayload();
					Map<String, Object> oldReg = message.getPrevPayload();
					boolean after = newReg != null && shouldSendOut(potentialSub, newReg);
					boolean before = oldReg != null && shouldSendOut(potentialSub, oldReg);
					if (after) {
						// "updated" if it was (or is assumed) already matching, else "newlyMatching"
						int reason = (before || oldReg == null) ? AppConstants.UPDATE_REQUEST
								: AppConstants.CREATE_REQUEST;
						unis.add(sendNotification(potentialSub, newReg, reason));
					} else if (before) {
						// no longer matches after the update -> noLongerMatching (DELETE_REQUEST maps to it)
						unis.add(sendNotification(potentialSub, oldReg, AppConstants.DELETE_REQUEST));
					}
					break;
				}
				case AppConstants.CREATE_REQUEST:
				case AppConstants.DELETE_REQUEST:
					unis.add(sendNotification(potentialSub, message.getPayload(), message.getRequestType()));
				default:
					break;
			}

		}
		if (unis.isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		return Uni.combine().all().unis(unis).discardItems();
	}

	private Uni<Void> sendNotification(SubscriptionRequest potentialSub, Map<String, Object> reg, int triggerReason) {
		if (shouldSendOut(potentialSub, reg)) {
			long notificationStartTime = System.currentTimeMillis();
			Map<String, Object> regToSend = filterRelevantInformation(potentialSub, reg);
			Uni<Map<String, Object>> generated = SubscriptionTools.generateCsourceNotification(potentialSub, regToSend, triggerReason, ldService);
			if (notificationOnWorkerThread) {
				generated = generated.emitOn(Infrastructure.getDefaultWorkerPool());
			}
			return generated.onItem().transformToUni(notification -> deliverNotification(potentialSub, notification));
		}
		return Uni.createFrom().voidItem();
	}

	// Deliver an already-generated cSourceNotification to the subscription endpoint. Kept separate so
	// the initial-on-subscription notification (built in createSubscription with all matching
	// registrations) is sent as-is instead of being re-wrapped by a second generateCsourceNotification.
	private Uni<Void> deliverNotification(SubscriptionRequest potentialSub, Map<String, Object> notification) {
						NotificationParam notificationParam = potentialSub.getSubscription().getNotification();
						Uni<Void> toSend;
						switch (notificationParam.getEndPoint().getUri().getScheme()) {
							case "internal":
								// try {
								// MicroServiceUtils
								// .serializeAndSplitObjectAndEmit(
								// new InternalNotification(potentialSub.getTenant(), potentialSub.getId(),
								// notification),
								// messageSize, internalNotificationSender, objectMapper);
								// } catch (ResponseException e) {
								// logger.error("Failed to send internal notification", e);
								// }
								toSend = Uni.createFrom().voidItem();
								break;
							case "mqtt":
							case "mqtts":
								try {
									toSend = getMqttClient(notificationParam).onItem().transformToUni(client -> {
										int qos = 1;

										String qosString = notificationParam.getEndPoint().getNotifierInfo()
												.get(NGSIConstants.MQTT_QOS);
										if (qosString != null) {
											qos = Integer.parseInt(qosString);
										}
										try {
											return client
													.publish(
															notificationParam.getEndPoint().getUri().getPath()
																	.substring(1),
															Buffer.buffer(SubscriptionTools
																	.getMqttPayload(notificationParam, notification,
																			potentialSub.getSubscription().getOtherHead())),
															MqttQoS.valueOf(qos), false, false)
													.onItem().transformToUni(t -> {
														if (t == 0) {
															// TODO what the fuck is the result here
														}
														long now = System.currentTimeMillis();
														potentialSub.getSubscription().getNotification()
																.setLastSuccessfulNotification(now);
														potentialSub.getSubscription().getNotification()
																.setLastNotification(now);
														return regDAO.updateNotificationSuccess(
																potentialSub.getTenant(),
																potentialSub.getId(),
																SerializationTools.notifiedAt_formatter.format(
																		LocalDateTime.ofInstant(
																				Instant.ofEpochMilli(now),
																				ZoneId.of("Z"))));
													}).onFailure().recoverWithUni(e -> {
														logger.error("failed to send notification for subscription "
																+ potentialSub, e);
														long now = System.currentTimeMillis();
														potentialSub.getSubscription().getNotification()
																.setLastFailedNotification(now);
														potentialSub.getSubscription().getNotification()
																.setLastNotification(now);
														return regDAO.updateNotificationFailure(
																potentialSub.getTenant(),
																potentialSub.getId(),
																SerializationTools.notifiedAt_formatter.format(
																		LocalDateTime.ofInstant(
																				Instant.ofEpochMilli(now),
																				ZoneId.of("Z"))));
													});
										} catch (Exception e) {
											logger.error("failed to send notification for subscription " + potentialSub,
													e);
											return Uni.createFrom().voidItem();
										}
									});
								} catch (Exception e) {
									logger.error("failed to send notification for subscription " + potentialSub, e);
									return Uni.createFrom().voidItem();
								}
								break;
							case "http":
							case "https":
								try {
									toSend = ldService
											.compact(notification, null, potentialSub.getContext(), HttpUtils.opts, -1)
											.onItem().transformToUni(noti -> {
												// JSON-LD compaction unwraps a single-element array to the element; the
												// ContextSourceNotification "data" member must always be a JSON array.
												Object dataMember = noti.get(NGSIConstants.NGSI_LD_DATA_SHORT);
												if (dataMember != null && !(dataMember instanceof List)) {
													noti.put(NGSIConstants.NGSI_LD_DATA_SHORT, List.of(dataMember));
												}
												// Compaction also collapses each RegistrationInfo's single-element
												// "entities" array and each EntityInfo's "type" array; restore them so
												// the registration data keeps its NGSI-LD array structure.
												restoreCsourceDataArrays(noti.get(NGSIConstants.NGSI_LD_DATA_SHORT));
												// postAbs: the endpoint URI is absolute (host+port+path); webClient.post(String)
											// treats it as a request URI on the default host:80 -> Connection refused.
											return webClient
														.postAbs(notificationParam.getEndPoint().getUri().toString())
														.putHeaders(SubscriptionTools.getHeaders(notificationParam,
																potentialSub.getSubscription().getOtherHead()))
														.sendJsonObject(new JsonObject(noti)).onFailure().retry()
														.atMost(3)
														.onItem().transformToUni(result -> {
															int statusCode = result.statusCode();
															long now = System.currentTimeMillis();
															if (statusCode >= 200 && statusCode < 300) {
																potentialSub.getSubscription().getNotification()
																		.setLastSuccessfulNotification(now);
																potentialSub.getSubscription().getNotification()
																		.setLastNotification(now);
																return regDAO.updateNotificationSuccess(
																		potentialSub.getTenant(), potentialSub.getId(),
																		SerializationTools.notifiedAt_formatter
																				.format(LocalDateTime.ofInstant(
																						Instant.ofEpochMilli(now),
																						ZoneId.of("Z"))));
															} else {
																logger.error(
																		"failed to send notification for subscription "
																				+ potentialSub + " with status code "
																				+ statusCode
																				+ ". Remember there is no redirect following for post due to security considerations");
																potentialSub.getSubscription().getNotification()
																		.setLastFailedNotification(now);
																potentialSub.getSubscription().getNotification()
																		.setLastNotification(now);
																return regDAO.updateNotificationFailure(
																		potentialSub.getTenant(), potentialSub.getId(),
																		SerializationTools.notifiedAt_formatter
																				.format(LocalDateTime.ofInstant(
																						Instant.ofEpochMilli(now),
																						ZoneId.of("Z"))));
															}
														}).onFailure().recoverWithUni(e -> {
															logger.error("failed to send notification for subscription "
																	+ potentialSub, e);
															long now = System.currentTimeMillis();
															potentialSub.getSubscription().getNotification()
																	.setLastFailedNotification(now);
															potentialSub.getSubscription().getNotification()
																	.setLastNotification(now);
															return regDAO.updateNotificationFailure(
																	potentialSub.getTenant(), potentialSub.getId(),
																	SerializationTools.notifiedAt_formatter
																			.format(LocalDateTime.ofInstant(
																					Instant.ofEpochMilli(now),
																					ZoneId.of("Z"))));
														});
											});
								} catch (Exception e) {
									logger.error("failed to send notification for subscription " + potentialSub, e);
									return Uni.createFrom().voidItem();
								}
								break;
							default:
								logger.error("unsuported endpoint in subscription " + potentialSub.getId());
								return Uni.createFrom().voidItem();
						}
						if (potentialSub.getSubscription().getThrottling() > 0) {
							long delay = potentialSub.getSubscription().getThrottling() - (System.currentTimeMillis()
									- potentialSub.getSubscription().getNotification().getLastNotification());
							if (delay > 0) {
								return Uni.createFrom().voidItem().onItem().delayIt().by(Duration.ofMillis(delay))
										.onItem().transformToUni(v -> toSend);
							} else {
								return toSend;
							}
						}
						return toSend;
	}

	private Uni<MqttClient> getMqttClient(NotificationParam notificationParam) {
		URI host = notificationParam.getEndPoint().getUri();
		String hostString = host.getHost() + host.getPort();
		MqttClient client;
		if (!host2MqttClient.containsKey(hostString)) {
			client = MqttClient.create(vertx);
			return client.connect(host.getPort(), host.getHost()).onItem().transform(t -> {
				host2MqttClient.put(hostString, client);
				return client;
			});
		} else {
			client = host2MqttClient.get(hostString);
			if (client.isConnected()) {
				return Uni.createFrom().item(client);
			} else {
				return client.connect(host.getPort(), host.getHost()).onItem().transform(t -> {
					return client;
				});
			}
		}
	}

	@SuppressWarnings("unchecked")
	private boolean shouldSendOut(SubscriptionRequest potentialSub, Map<String, Object> reg) {
		Subscription sub = potentialSub.getSubscription();
		if (sub.getIsActive() == null || !sub.getIsActive() || sub.getExpiresAt() < System.currentTimeMillis()) {
			return false;
		}

		List<Map<String, Object>> information = (List<Map<String, Object>>) reg.get(NGSIConstants.NGSI_LD_INFORMATION);
		boolean matched = matchesEntitySelectors(sub, information);
		if (!matched) {
			return false;
		}
		if (!SubscriptionTools.evaluateGeoQuery(sub.getLdGeoQuery(),
				(List<Map<String, Object>>) reg.get(NGSIConstants.NGSI_LD_LOCATION))) {
			return false;
		}
		if (sub.getScopeQuery() != null) {
			if (!sub.getScopeQuery().calculate(EntityTools.getScopes(reg))) {
				return false;
			}
		}
		if (sub.getCsf() != null) {
			if (!sub.getCsf().calculate(EntityTools.getBaseProperties(reg))) {
				return false;
			}
		}
		return true;
	}

	// Whether the given RegistrationInfo list matches at least one of the subscription's EntitySelectors
	// (NGSI-LD 5.12). Extracted from shouldSendOut so it can also be applied per-RegistrationInfo block
	// to return only the relevant information in a cSourceNotification (5.11.7).
	@SuppressWarnings("unchecked")
	private boolean matchesEntitySelectors(Subscription sub, List<Map<String, Object>> information) {
		boolean matched = false;
		if (sub.getEntities() == null || sub.getEntities().isEmpty()) {
			matched = true;
		} else {
			for (EntityInfo entityInfo : sub.getEntities()) {
				if (entityInfo.getTypeTerm() != null && entityInfo.getTypeTerm().getAllTypes().contains(ALL_TYPES_SUB)) {
					return true;
				}
				if (entityInfo.getId() != null && entityInfo.getTypeTerm() != null && sub.getAttributeNames() != null) {
					if (checkRegForIdTypeAttrs(entityInfo.getId(), entityInfo.getTypeTerm(), sub.getAttributeNames(),
							information)) {
						matched = true;
						break;
					}
				} else if (entityInfo.getIdPattern() != null && entityInfo.getTypeTerm() != null
						&& sub.getAttributeNames() != null) {
					if (checkRegForIdPatternTypeAttrs(entityInfo.getIdPattern(), entityInfo.getTypeTerm(),
							sub.getAttributeNames(),
							information)) {
						matched = true;
						break;
					}
				} else if (entityInfo.getId() != null && entityInfo.getTypeTerm() != null) {
					if (checkRegForIdType(entityInfo.getId(), entityInfo.getTypeTerm(),
							information)) {
						matched = true;
						break;
					}
				} else if (entityInfo.getIdPattern() != null && entityInfo.getTypeTerm() != null) {
					if (checkRegForIdPatternType(entityInfo.getIdPattern(), entityInfo.getTypeTerm(),
							information)) {
						matched = true;
						break;
					}
				} else if (entityInfo.getId() != null && sub.getAttributeNames() != null) {
					if (checkRegForIdAttrs(entityInfo.getId(), sub.getAttributeNames(),
							information)) {
						matched = true;
						break;
					}
				} else if (entityInfo.getIdPattern() != null && sub.getAttributeNames() != null) {
					if (checkRegForIdPatternAttrs(entityInfo.getIdPattern(), sub.getAttributeNames(),
							information)) {
						matched = true;
						break;
					}
				} else if (entityInfo.getTypeTerm() != null && sub.getAttributeNames() != null) {
					if (checkRegForTypeAttrs(entityInfo.getTypeTerm(), sub.getAttributeNames(),
							information)) {
						matched = true;
						break;
					}
				} else if (entityInfo.getTypeTerm() != null) {
					if (checkRegForType(entityInfo.getTypeTerm(),
							information)) {
						matched = true;
						break;
					}
				} else if (entityInfo.getIdPattern() != null) {
					if (checkRegForIdPattern(entityInfo.getIdPattern(),
							information)) {
						matched = true;
						break;
					}
				} else if (entityInfo.getId() != null) {
					if (checkRegForId(entityInfo.getId(),
							information)) {
						matched = true;
						break;
					}
				} else if (sub.getAttributeNames() != null) {
					if (checkRegForAttribs(sub.getAttributeNames(),
							information)) {
						matched = true;
						break;
					}
				}
			}
		}
		return matched;
	}

	// Returns a copy of the registration whose "information" array keeps only the RegistrationInfo
	// blocks relevant to the subscription (NGSI-LD 5.11.7: a cSourceNotification should carry only the
	// matching RegistrationInfo elements, not the whole registration). If nothing would remain (or there
	// is no information array) the registration is returned unchanged.
	@SuppressWarnings("unchecked")
	private Map<String, Object> filterRelevantInformation(SubscriptionRequest potentialSub, Map<String, Object> reg) {
		Object infoObj = reg.get(NGSIConstants.NGSI_LD_INFORMATION);
		if (!(infoObj instanceof List)) {
			return reg;
		}
		List<Map<String, Object>> information = (List<Map<String, Object>>) infoObj;
		List<Map<String, Object>> relevant = new ArrayList<>();
		for (Map<String, Object> block : information) {
			if (matchesEntitySelectors(potentialSub.getSubscription(), List.of(block))) {
				relevant.add(block);
			}
		}
		if (relevant.isEmpty() || relevant.size() == information.size()) {
			return reg;
		}
		Map<String, Object> filtered = new HashMap<>(reg);
		filtered.put(NGSIConstants.NGSI_LD_INFORMATION, relevant);
		return filtered;
	}

	// Re-wraps the single-element arrays that JSON-LD compaction collapsed in a cSourceNotification's
	// compacted "data" registrations: information[].entities and each entity's type must stay arrays
	// per the NGSI-LD ContextSourceRegistration structure.
	@SuppressWarnings("unchecked")
	private void restoreCsourceDataArrays(Object dataMember) {
		if (!(dataMember instanceof List)) {
			return;
		}
		for (Object regObj : (List<Object>) dataMember) {
			if (!(regObj instanceof Map)) {
				continue;
			}
			Map<String, Object> reg = (Map<String, Object>) regObj;
			Object info = reg.get("information");
			if (info != null && !(info instanceof List)) {
				info = new ArrayList<>(List.of(info));
				reg.put("information", info);
			}
			if (!(info instanceof List)) {
				continue;
			}
			for (Object blockObj : (List<Object>) info) {
				if (!(blockObj instanceof Map)) {
					continue;
				}
				Map<String, Object> block = (Map<String, Object>) blockObj;
				Object entities = block.get(NGSIConstants.NGSI_LD_ENTITIES_SHORT);
				if (entities != null && !(entities instanceof List)) {
					entities = new ArrayList<>(List.of(entities));
					block.put(NGSIConstants.NGSI_LD_ENTITIES_SHORT, entities);
				}
				if (!(entities instanceof List)) {
					continue;
				}
				for (Object entObj : (List<Object>) entities) {
					if (!(entObj instanceof Map)) {
						continue;
					}
					Map<String, Object> ent = (Map<String, Object>) entObj;
					Object type = ent.get(NGSIConstants.QUERY_PARAMETER_TYPE);
					if (type != null && !(type instanceof List)) {
						ent.put(NGSIConstants.QUERY_PARAMETER_TYPE, new ArrayList<>(List.of(type)));
					}
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForId(String[] id, List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				return true;
			}
			List<Map<String, Object>> entities = (List<Map<String, Object>>) entry.get(NGSIConstants.NGSI_LD_ENTITIES);
			for (Map<String, Object> entity : entities) {
				if (!entity.containsKey(NGSIConstants.JSON_LD_ID)) {
					return true;
				}

				if (ArrayUtils.contains(id, entity.get(NGSIConstants.JSON_LD_ID))) {
					return true;
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForIdPattern(String idPattern, List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				return true;
			}
			List<Map<String, Object>> entities = (List<Map<String, Object>>) entry.get(NGSIConstants.NGSI_LD_ENTITIES);
			for (Map<String, Object> entity : entities) {
				if ((!entity.containsKey(NGSIConstants.JSON_LD_ID)
						&& !entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN))
						|| (entity.containsKey(NGSIConstants.JSON_LD_ID)
								&& ((String) entity.get(NGSIConstants.JSON_LD_ID)).matches(idPattern))
						|| (entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)
								&& ((List<Map<String, String>>) entity.get(NGSIConstants.NGSI_LD_ID_PATTERN)).get(0)
										.get(NGSIConstants.JSON_LD_VALUE).equals(idPattern))) {
					return true;
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForAttribs(Set<String> attributeNames, List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)
					&& !entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
				return true;
			}
			if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
				List<Map<String, String>> relationships = (List<Map<String, String>>) entry
						.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
				for (Map<String, String> relationship : relationships) {
					if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_ID))) {
						return true;
					}
				}
			}
			if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
				List<Map<String, String>> properties = (List<Map<String, String>>) entry
						.get(NGSIConstants.NGSI_LD_PROPERTIES);
				for (Map<String, String> property : properties) {
					if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_ID))) {
						return true;
					}
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForType(TypeQueryTerm typeQueryTerm, List<Map<String, Object>> information) {
		if (information != null) {
			for (Map<String, Object> entry : information) {
				if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
					return true;
				}
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				for (Map<String, Object> entity : entities) {
					if (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
							|| typeQueryTerm.calculate((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE))) {
						return true;
					}
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForTypeAttrs(TypeQueryTerm typeQueryTerm, Set<String> attributeNames,
			List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
			} else if (!entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)
					&& !entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				for (Map<String, Object> entity : entities) {
					if (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
							|| typeQueryTerm.calculate((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE))) {
						return true;
					}
				}
			} else {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				boolean typeFound = false;
				for (Map<String, Object> entity : entities) {
					if (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
							|| typeQueryTerm.calculate((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE))) {
						typeFound = true;
						break;
					}
				}
				if (!typeFound) {
					return false;
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForIdPatternAttrs(String idPattern, Set<String> attributeNames,
			List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
			} else if (!entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)
					&& !entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				for (Map<String, Object> entity : entities) {
					if (!entity.containsKey(NGSIConstants.JSON_LD_ID)
							&& !entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)) {
						return true;
					}
					if (entity.containsKey(NGSIConstants.JSON_LD_ID)
							&& ((String) entity.get(NGSIConstants.JSON_LD_ID)).matches(idPattern)) {
						return true;
					}
					if (entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)
							&& ((List<Map<String, String>>) entity.get(NGSIConstants.NGSI_LD_ID_PATTERN)).get(0)
									.get(NGSIConstants.JSON_LD_VALUE).equals(idPattern)) {
						return true;
					}
				}
			} else {

				boolean idPatternFound = false;
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				for (Map<String, Object> entity : entities) {
					if (!entity.containsKey(NGSIConstants.JSON_LD_ID)
							&& !entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)) {
						idPatternFound = true;
						break;
					}
					if (entity.containsKey(NGSIConstants.JSON_LD_ID)
							&& ((String) entity.get(NGSIConstants.JSON_LD_ID)).matches(idPattern)) {
						idPatternFound = true;
						break;
					}
					if (entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)
							&& ((List<Map<String, String>>) entity.get(NGSIConstants.NGSI_LD_ID_PATTERN)).get(0)
									.get(NGSIConstants.JSON_LD_VALUE).equals(idPattern)) {
						idPatternFound = true;
						break;
					}
				}
				if (!idPatternFound) {
					return false;
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForIdAttrs(String[] id, Set<String> attributeNames, List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
			} else if (!entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)
					&& !entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				for (Map<String, Object> entity : entities) {
					if (!entity.containsKey(NGSIConstants.JSON_LD_ID)) {
						return true;
					}
					if (ArrayUtils.contains(id, entity.get(NGSIConstants.JSON_LD_ID))) {
						return true;
					}
				}
			} else {

				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				boolean idFound = false;

				for (Map<String, Object> entity : entities) {
					if (!entity.containsKey(NGSIConstants.JSON_LD_ID)) {
						idFound = true;
						break;
					}
					if (entity.get(NGSIConstants.JSON_LD_ID).equals(id.toString())) {
						idFound = true;
						break;
					}
				}
				if (!idFound) {
					return false;
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForIdPatternType(String idPattern, TypeQueryTerm typeQueryTerm,
			List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				return true;
			}
			List<Map<String, Object>> entities = (List<Map<String, Object>>) entry.get(NGSIConstants.NGSI_LD_ENTITIES);
			for (Map<String, Object> entity : entities) {
				if (((!entity.containsKey(NGSIConstants.JSON_LD_ID)
						&& !entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN))
						|| (entity.containsKey(NGSIConstants.JSON_LD_ID)
								&& ((String) entity.get(NGSIConstants.JSON_LD_ID)).matches(idPattern))
						|| (entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)
								&& ((List<Map<String, String>>) entity.get(NGSIConstants.NGSI_LD_ID_PATTERN)).get(0)
										.get(NGSIConstants.JSON_LD_VALUE).equals(idPattern)))
						&& (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
								|| typeQueryTerm.calculate((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)))) {
					return true;
				}
			}
		}
		return false;

	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForIdType(String[] id, TypeQueryTerm typeQueryTerm, List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				return true;
			}
			List<Map<String, Object>> entities = (List<Map<String, Object>>) entry.get(NGSIConstants.NGSI_LD_ENTITIES);
			for (Map<String, Object> entity : entities) {
				if ((!entity.containsKey(NGSIConstants.JSON_LD_ID)
						|| ArrayUtils.contains(id, entity.get(NGSIConstants.JSON_LD_ID)))
						&& (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
								|| typeQueryTerm.calculate((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)))) {
					return true;
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForIdPatternTypeAttrs(String idPattern, TypeQueryTerm typeQueryTerm,
			Set<String> attributeNames, List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
			} else if (!entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)
					&& !entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				for (Map<String, Object> entity : entities) {
					if (((!entity.containsKey(NGSIConstants.JSON_LD_ID)
							&& !entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN))
							|| (entity.containsKey(NGSIConstants.JSON_LD_ID)
									&& ((String) entity.get(NGSIConstants.JSON_LD_ID)).matches(idPattern))
							|| (entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)
									&& ((List<Map<String, String>>) entity.get(NGSIConstants.NGSI_LD_ID_PATTERN)).get(0)
											.get(NGSIConstants.JSON_LD_VALUE).equals(idPattern)))
							&& (!entity.containsKey(NGSIConstants.JSON_LD_TYPE) || typeQueryTerm
									.calculate((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)))) {
						return true;
					}
				}
			} else {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				boolean idPatternAndTypeFound = false;
				for (Map<String, Object> entity : entities) {
					if (((!entity.containsKey(NGSIConstants.JSON_LD_ID)
							&& !entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN))
							|| (entity.containsKey(NGSIConstants.JSON_LD_ID)
									&& ((String) entity.get(NGSIConstants.JSON_LD_ID)).matches(idPattern))
							|| (entity.containsKey(NGSIConstants.NGSI_LD_ID_PATTERN)
									&& ((List<Map<String, String>>) entity.get(NGSIConstants.NGSI_LD_ID_PATTERN)).get(0)
											.get(NGSIConstants.JSON_LD_VALUE).equals(idPattern)))
							&& (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
									|| ((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)).stream()
											.anyMatch(typeEntry -> typeQueryTerm.getAllTypes().contains(typeEntry)))) {
						idPatternAndTypeFound = true;
						break;
					}
				}

				if (!idPatternAndTypeFound) {
					return false;
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	@SuppressWarnings("unchecked")
	private boolean checkRegForIdTypeAttrs(String[] id, TypeQueryTerm type, Set<String> attributeNames,
			List<Map<String, Object>> information) {
		for (Map<String, Object> entry : information) {
			if (!entry.containsKey(NGSIConstants.NGSI_LD_ENTITIES)) {
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
			} else if (!entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)
					&& !entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				for (Map<String, Object> entity : entities) {
					if ((!entity.containsKey(NGSIConstants.JSON_LD_ID)
							|| ArrayUtils.contains(id, entity.get(NGSIConstants.JSON_LD_ID)))
							&& (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
									|| type.calculate((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)))) {

						return true;
					}
				}
			} else {
				List<Map<String, Object>> entities = (List<Map<String, Object>>) entry
						.get(NGSIConstants.NGSI_LD_ENTITIES);
				boolean idAndTypeFound = false;
				for (Map<String, Object> entity : entities) {
					if ((!entity.containsKey(NGSIConstants.JSON_LD_ID)
							|| entity.get(NGSIConstants.JSON_LD_ID).equals(id.toString()))
							&& (!entity.containsKey(NGSIConstants.JSON_LD_TYPE)
									|| ((List<String>) entity.get(NGSIConstants.JSON_LD_TYPE)).stream()
											.anyMatch(typeEntry -> type.getAllTypes().contains(typeEntry)))) {
						idAndTypeFound = true;
						break;
					}
				}

				if (!idAndTypeFound) {
					return false;
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_RELATIONSHIPS)) {
					List<Map<String, String>> relationships = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
					for (Map<String, String> relationship : relationships) {
						if (attributeNames.contains(relationship.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
				if (entry.containsKey(NGSIConstants.NGSI_LD_PROPERTIES)) {
					List<Map<String, String>> properties = (List<Map<String, String>>) entry
							.get(NGSIConstants.NGSI_LD_PROPERTIES);
					for (Map<String, String> property : properties) {
						if (attributeNames.contains(property.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
			}
		}
		return false;
	}

	public Uni<Void> handleInternalSubscription(SubscriptionRequest message) {
		if (message.getRequestType() == AppConstants.DELETE_SUBSCRIPTION_REQUEST || message.getPayload() == null) {
			tenant2subscriptionId2Subscription.remove(message.getTenant(), message.getId());
			return Uni.createFrom().voidItem();
		}
		try {
			if (message.getRequestType() == AppConstants.UPDATE_SUBSCRIPTION_REQUEST) {
				message.setSubscription(Subscription.expandSubscription(message.getPayload(), message.getId(),
						message.getContext(), true));
			} else {
				message.setSubscription(Subscription.expandSubscription(message.getPayload(), message.getId(),
						message.getContext(), false));
			}
		} catch (ResponseException e) {
			logger.error("Failed to load internal subscription", e);
			return Uni.createFrom().voidItem();
		}
		boolean sendNotification = !tenant2subscriptionId2Subscription.contains(message.getTenant(), message.getId());
		try {
			message.getSubscription().getNotification().getEndPoint().setUri(new URI("internal:kafka"));
			// if there is attribs and there is q take attribs from q as well into attrs
			if (message.getSubscription().getAttributeNames() != null
					&& !message.getSubscription().getAttributeNames().isEmpty()
					&& message.getSubscription().getLdQuery() != null) {
				message.getSubscription().getAttributeNames()
						.addAll(message.getSubscription().getLdQuery().getAllAttibs());
			}
			message.getSubscription().setThrottling(0);
			message.getSubscription().setTimeInterval(0);
			tenant2subscriptionId2Subscription.put(message.getTenant(), message.getId(), message);
		} catch (URISyntaxException e) {
			// left empty intentionally this will never throw because it's a constant string
			// we control
		} catch (Exception e1) {
			e1.printStackTrace();
		}
		if (sendNotification) {
			return regDAO.getInitialNotificationData(message).onItem().transformToUni(rows -> {
				List<Map<String, Object>> data = Lists.newArrayList();
				rows.forEach(row -> {
					data.add(row.getJsonObject(0).getMap());
				});
				return SubscriptionTools.generateCsourceNotification(message, data,
						AppConstants.INTERNAL_NOTIFICATION_REQUEST, ldService).onItem().transformToUni(noti -> {
							// try {
							// MicroServiceUtils.serializeAndSplitObjectAndEmit(
							// new InternalNotification(message.getTenant(), message.getId(), noti),
							// messageSize, internalNotificationSender, objectMapper);
							// } catch (ResponseException e) {
							// logger.error("Failed to serialize notification", e);
							// }
							return Uni.createFrom().voidItem();
						});

			});
		} else {
			return Uni.createFrom().voidItem();
		}
	}

	protected boolean shouldFire(Map<String, Object> entry, SubscriptionRequest subscription) {
		Set<String> attribs = subscription.getSubscription().getAttributeNames();

		if (attribs == null || attribs.isEmpty()) {
			return true;
		}
		if (entry.containsKey(NGSIConstants.NGSI_LD_INFORMATION)) {
			@SuppressWarnings("unchecked")
			List<Map<String, Object>> information = (List<Map<String, Object>>) entry
					.get(NGSIConstants.NGSI_LD_INFORMATION);
			for (Map<String, Object> informationEntry : information) {
				Object propertyNames = informationEntry.get(NGSIConstants.NGSI_LD_PROPERTIES);
				Object relationshipNames = informationEntry.get(NGSIConstants.NGSI_LD_RELATIONSHIPS);
				if (relationshipNames == null && relationshipNames == null) {
					return true;
				}
				if (relationshipNames != null) {
					@SuppressWarnings("unchecked")
					List<Map<String, String>> list = (List<Map<String, String>>) relationshipNames;
					for (Map<String, String> relationshipEntry : list) {
						if (attribs.contains(relationshipEntry.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
				if (propertyNames != null) {
					@SuppressWarnings("unchecked")
					List<Map<String, String>> list = (List<Map<String, String>>) propertyNames;
					for (Map<String, String> propertyEntry : list) {
						if (attribs.contains(propertyEntry.get(NGSIConstants.JSON_LD_ID))) {
							return true;
						}
					}
				}
			}
		}
		// TODO add aditional changes on what could fire in a csource reg
		return false;
	}

	@Scheduled(every = "${scorpio.registry.subscription.checkinterval}", delayed = "${scorpio.startupdelay}")
	Uni<Void> checkIntervalSubs() {
		List<Uni<Void>> unis = Lists.newArrayList();
		for (Cell<String, String, SubscriptionRequest> cell : tenant2subscriptionId2IntervalSubscription.cellSet()) {
			SubscriptionRequest request = cell.getValue();
			Subscription sub = request.getSubscription();
			long now = System.currentTimeMillis();
			if (sub.getNotification().getLastNotification() + sub.getTimeInterval() < now) {
				unis.add(regDAO.getInitialNotificationData(request).onItem().transformToUni(rows -> {
					if (rows.size() == 0) {
						return Uni.createFrom().voidItem();
					}
					List<Map<String, Object>> data = Lists.newArrayList();
					rows.forEach(row -> {
						data.add(row.getJsonObject(0).getMap());
					});
					return SubscriptionTools.generateCsourceNotification(request, data,
							AppConstants.INTERVAL_NOTIFICATION_REQUEST, ldService).onItem().transformToUni(noti -> {
								return deliverNotification(request, noti);
							});
				}));
			}
		}
		if (unis.isEmpty()) {
			return Uni.createFrom().voidItem();
		}
		return Uni.combine().all().unis(unis).discardItems();
	}

	public Uni<Void> syncDeleteSubscription(String tenant, String subId) {
		tenant2subscriptionId2IntervalSubscription.remove(tenant, subId);
		tenant2subscriptionId2Subscription.remove(tenant, subId);
		return Uni.createFrom().voidItem();
	}

	public Uni<Void> syncUpdateSubscription(String tenant, String subId) {
		return regDAO.getSubscription(tenant, subId).onFailure().recoverWithItem(e -> {
			tenant2subscriptionId2IntervalSubscription.remove(tenant, subId);
			tenant2subscriptionId2Subscription.remove(tenant, subId);
			return null;
		}).onItem().transformToUni(rows -> {
			if (rows == null || rows.size() == 0) {
				return Uni.createFrom().voidItem();
			}
			Row first = rows.iterator().next();
			return ldService.parsePure(first.getJsonObject(1).getMap()).onItem().transformToUni(ctx -> {
				SubscriptionRequest request;
				try {
					request = new SubscriptionRequest(tenant, first.getJsonObject(0).getMap(), ctx);
					request.setContextId(first.getString(2));
					request.getSubscription().addOtherHead(NGSIConstants.LINK_HEADER,
							"<%s>; rel=\"http://www.w3.org/ns/json-ld#context\"; type=\"application/ld+json\""
									.formatted(request.getSubscription().getJsonldContext()));
					request.getSubscription().addOtherHead(NGSIConstants.TENANT_HEADER, request.getTenant());
					request.setSendTimestamp(-1);
					if (isIntervalSub(request)) {
						tenant2subscriptionId2IntervalSubscription.put(request.getTenant(), request.getId(), request);
						tenant2subscriptionId2Subscription.remove(tenant, request.getId());
					} else {
						tenant2subscriptionId2Subscription.put(request.getTenant(), request.getId(), request);
						tenant2subscriptionId2IntervalSubscription.remove(tenant, request.getId());
					}
				} catch (Exception e) {
					logger.error("Failed to load stored subscription " + subId);
				}
				return Uni.createFrom().voidItem();
			});
		});
	}

	public List<String> getAllSubscriptionIds() {
		Set<String> tmp = Sets.newHashSet(tenant2subscriptionId2Subscription.columnKeySet());
		tmp.addAll(tenant2subscriptionId2IntervalSubscription.columnKeySet());
		return tmp.stream().sorted().collect(Collectors.toList());
	}

	public void activateSubs(List<String> mySubs) {
		tenant2subscriptionId2Subscription.values().forEach(t -> {
			if (mySubs.contains(t.getId())) {
				t.getSubscription().setActive(true);
			} else {
				t.getSubscription().setActive(false);
			}
		});
		tenant2subscriptionId2IntervalSubscription.values().forEach(t -> {
			if (mySubs.contains(t.getId())) {
				t.getSubscription().setActive(true);
			} else {
				t.getSubscription().setActive(false);
			}
		});

	}

	public void addSyncService(SyncService registrySubscriptionSyncService) {
		this.subscriptionSyncService = registrySubscriptionSyncService;

	}

	public void reloadSubscription(String tenant, String id, boolean internal) {
		if (internal) {
			regDAO.loadSubscription(tenant, id).onItem().transformToUni(t -> {
				return ldService.parsePure(t.getItem2().get(NGSIConstants.JSON_LD_CONTEXT)).onItem()
						.transformToUni(ctx -> {
							SubscriptionRequest request;
							try {
								request = new SubscriptionRequest(tenant, t.getItem1(), ctx);
							} catch (ResponseException e) {
								logger.error("Failed to reload subscription " + id);
								return Uni.createFrom().voidItem();
							}

							return handleInternalSubscription(request);
						});
			}).subscribe().with(i -> {
				logger.debug("Reloaded subscription: " + id);
			});
		} else {
			regDAO.loadRegSubscription(tenant, id).onItem().transformToUni(t -> {
				return ldService.parsePure(t.getItem2().get(NGSIConstants.JSON_LD_CONTEXT)).onItem()
						.transformToUni(ctx -> {
							SubscriptionRequest request;
							try {
								request = new SubscriptionRequest(tenant, t.getItem1(), ctx);
							} catch (ResponseException e) {
								logger.error("Failed to reload subscription " + id);
								return Uni.createFrom().voidItem();
							}
							request.setSendTimestamp(-1);
							if (isIntervalSub(request)) {
								this.tenant2subscriptionId2IntervalSubscription.put(request.getTenant(),
										request.getId(), request);
							} else {
								this.tenant2subscriptionId2Subscription.put(request.getTenant(), request.getId(),
										request);
							}
							return Uni.createFrom().voidItem();
						});
			}).subscribe().with(i -> {
				logger.debug("Reloaded subscription: " + id);
			});
		}
	}

}
