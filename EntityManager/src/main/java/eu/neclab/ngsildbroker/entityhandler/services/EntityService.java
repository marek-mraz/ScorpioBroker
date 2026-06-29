package eu.neclab.ngsildbroker.entityhandler.services;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import io.vertx.mutiny.core.MultiMap;
import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.OnOverflow;
import org.eclipse.microprofile.reactive.messaging.OnOverflow.Strategy;
import org.locationtech.spatial4j.shape.Shape;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jsonldjava.core.Context;
import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.utils.JsonUtils;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.RemoteHost;
import eu.neclab.ngsildbroker.commons.datatypes.ViaHeaders;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CSourceBaseRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;

import eu.neclab.ngsildbroker.commons.datatypes.requests.MergePatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.ReplaceAttribRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.ReplaceEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpsertEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.results.Attrib;
import eu.neclab.ngsildbroker.commons.datatypes.results.CRUDSuccess;
import eu.neclab.ngsildbroker.commons.datatypes.results.NGSILDOperationResult;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.interfaces.CSourceHandler;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.HttpUtils;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.runtime.Startup;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.reactive.messaging.MutinyEmitter;
import io.smallrye.reactive.messaging.annotations.Broadcast;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.core.Vertx;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
@Startup
@SuppressWarnings("unchecked")
public class EntityService implements CSourceHandler {

	private final static Logger logger = LoggerFactory.getLogger(EntityService.class);
	public static boolean checkEntity = false;
	@ConfigProperty(name = "scorpio.topics.entity.zip")
	boolean zip;
	@Inject
	EntityInfoDAO entityDAO;

	@Inject
	@Channel(AppConstants.ENTITY_CHANNEL)
	@Broadcast
	@OnOverflow(value = Strategy.UNBOUNDED_BUFFER)
	MutinyEmitter<String> entityEmitter;

	// @Inject
	// @Channel(AppConstants.ENTITY_BATCH_CHANNEL)
	// @Broadcast
	// @OnOverflow(value = Strategy.UNBOUNDED_BUFFER)
	// MutinyEmitter<String> batchEmitter;

	@Inject
	Vertx vertx;

	WebClient webClient;

	private Table<String, String, List<RegistrationEntry>> tenant2CId2RegEntries = HashBasedTable.create();
	private Table<String, String, List<RegistrationEntry>> tenant2CId2QueryRegEntries = HashBasedTable.create();

	@Inject
	JsonLDService jsonLdService;

	@Inject
	MicroServiceUtils microServiceUtils;

	@Inject
	ObjectMapper objectMapper;

	@ConfigProperty(name = "scorpio.messaging.maxSize")
	int messageSize;

	@PostConstruct
	void startup() {
		webClient = WebClient.create(vertx);
		entityDAO.getAllRegistries().onItem().transform(t -> {
			tenant2CId2RegEntries = t;
			return null;
		}).await().indefinitely();
		entityDAO.getAllQueryRegistries().onItem().transform(t -> {
			tenant2CId2QueryRegEntries = t;
			return null;
		}).await().indefinitely();
		this.microServiceUtils.registerCSourceReceiver(this);
	}

	private List<NGSILDOperationResult> handleBatchResponse(HttpResponse<Buffer> response, Throwable failure,
			RemoteHost host, List<Map<String, Object>> remoteEntities, Integer[] successCodes) {
		List<NGSILDOperationResult> result = Lists.newArrayList();
		if (failure != null) {

			for (Map<String, Object> entity : remoteEntities) {
				NGSILDOperationResult tmp = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
						entity.get("id") == null ? "no entityId" : (String) entity.get("id"), host.tenant());
				tmp.addFailure(new ResponseException(ErrorType.InternalError, failure.getMessage(), host,
						HttpUtils.getAttribsFromCompactedPayload(entity)));
				result.add(tmp);
			}
		} else {
			int statusCode = response.statusCode();
			// Any 2xx except 207 means the forwarded op succeeded on the Context Source — a distributed
			// Update/Replace/Merge/Append/Delete legitimately answers 204 where the local op's successCodes
			// expected 201 (and vice-versa). Counting that as a failure wrongly drove a 207 on a fully
			// successful distributed op (ETSI D018_02_02/04, batch D012/D015/D016). 207 keeps its own branch.
			if (ArrayUtils.contains(successCodes, statusCode)
					|| (statusCode >= 200 && statusCode < 300 && statusCode != 207)) {
				for (Map<String, Object> entity : remoteEntities) {
					NGSILDOperationResult tmp = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
							entity.get("id") == null ? "no entityId" : (String) entity.get("id"), host.tenant());
					tmp.addSuccess(new CRUDSuccess(host, HttpUtils.getAttribsFromCompactedPayload(entity)));
					result.add(tmp);
				}
			} else if (statusCode == 207) {
				JsonArray jsonArray = response.bodyAsJsonArray();
				if (jsonArray != null) {
					jsonArray.forEach(i -> {
						JsonObject jsonObj = (JsonObject) i;
						NGSILDOperationResult remoteResult;
						try {
							remoteResult = NGSILDOperationResult.getFromPayload(jsonObj.getMap(), host.tenant());
						} catch (ResponseException e) {
							remoteResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
									jsonObj.getMap().get("id") == null ? "no entityId"
											: (String) jsonObj.getMap().get("id"),
									host.tenant());
							remoteResult.addFailure(e);
						}
						result.add(remoteResult);
					});
				}

			} else {
				for (Map<String, Object> entity : remoteEntities) {
					NGSILDOperationResult tmp = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
							entity.get("id") == null ? "no entityId" : (String) entity.get("id"), host.tenant());

					JsonObject responseBody = response.bodyAsJsonObject();

					if (responseBody == null) {
						tmp.addFailure(new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
								NGSIConstants.ERROR_UNEXPECTED_RESULT_NULL_TITLE, statusCode, host,
								HttpUtils.getAttribsFromCompactedPayload(entity)));

					} else {
						if (!responseBody.containsKey(NGSIConstants.ERROR_TYPE)
								|| !responseBody.containsKey(NGSIConstants.ERROR_TITLE)
								|| !responseBody.containsKey(NGSIConstants.ERROR_DETAIL)) {
							tmp.addFailure(
									new ResponseException(statusCode, responseBody.getString(NGSIConstants.ERROR_TYPE),
											responseBody.getString(NGSIConstants.ERROR_TITLE),
											responseBody.getMap().get(NGSIConstants.ERROR_DETAIL), host,
											HttpUtils.getAttribsFromCompactedPayload(entity)));
						} else {
							tmp.addFailure(new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
									NGSIConstants.ERROR_UNEXPECTED_RESULT_NOT_EXPECTED_BODY_TITLE,
									responseBody.getMap(), host, HttpUtils.getAttribsFromCompactedPayload(entity)));
						}
					}
					result.add(tmp);
				}
			}
		}
		return result;
	}

	@SuppressWarnings("unused")
	private Uni<Void> handleWebResponse(NGSILDOperationResult result, HttpResponse<Buffer> response, Throwable failure,
			int successCode, RemoteHost host, Set<Attrib> attribs) {
		if (failure != null) {
			result.addFailure(new ResponseException(ErrorType.InternalError, failure.getMessage(), host, attribs));
		} else {
			int statusCode = response.statusCode();
			if (successCode == statusCode || (statusCode >= 200 && statusCode < 300 && statusCode != 207)) {
				result.addSuccess(new CRUDSuccess(host, attribs));
			} else if (statusCode == 207) {
				JsonObject jsonObj = response.bodyAsJsonObject();
				if (jsonObj != null) {
					NGSILDOperationResult remoteResult;
					try {
						remoteResult = NGSILDOperationResult.getFromPayload(jsonObj.getMap(), host.tenant());
					} catch (ResponseException e) {
						result.addFailure(e);
						return Uni.createFrom().voidItem();
					}
					result.getFailures().addAll(remoteResult.getFailures());
					result.getSuccesses().addAll(remoteResult.getSuccesses());
				}

			} else {

				JsonObject responseBody = response.bodyAsJsonObject();
				if (responseBody == null) {
					// could be from a batch response
					JsonArray tmp = response.bodyAsJsonArray();
					if (tmp != null) {
						try {
							responseBody = tmp.getJsonObject(0);
						} catch (ClassCastException e) {
							responseBody = null;
						}
					}
				}
				if (responseBody == null) {
					result.addFailure(new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
							NGSIConstants.ERROR_UNEXPECTED_RESULT_NULL_TITLE, statusCode, host, attribs));

				} else {
					if (!responseBody.containsKey(NGSIConstants.ERROR_TYPE)
							|| !responseBody.containsKey(NGSIConstants.ERROR_TITLE)
							|| !responseBody.containsKey(NGSIConstants.ERROR_DETAIL)) {
						result.addFailure(
								new ResponseException(statusCode, responseBody.getString(NGSIConstants.ERROR_TYPE),
										responseBody.getString(NGSIConstants.ERROR_TITLE),
										responseBody.getMap().get(NGSIConstants.ERROR_DETAIL), host, attribs));
					} else {
						result.addFailure(new ResponseException(500, NGSIConstants.ERROR_UNEXPECTED_RESULT,
								NGSIConstants.ERROR_UNEXPECTED_RESULT_NOT_EXPECTED_BODY_TITLE, responseBody.getMap(),
								host, attribs));
					}
				}
			}
		}
		return Uni.createFrom().voidItem();
	}

	public Uni<NGSILDOperationResult> partialUpdateAttribute(String tenant, String entityId, String attribName,
			Map<String, Object> payload, Context context, io.vertx.core.MultiMap headersFromReq,
			ViaHeaders viaHeaders) {
		logger.trace("updateMessage() :: started");

		UpdateEntityRequest request = new UpdateEntityRequest(tenant, entityId, payload, attribName, zip);
		request.setRequestType(AppConstants.PARTIAL_UPDATE_REQUEST);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> splitted = splitEntity(request,
				entityId);
		Map<String, Object> localEntity = splitted.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = splitted.getItem2();
		// if (remoteEntitiesAndHosts.isEmpty()) {
		// request.setPayload(localEntity);
		// return partialUpdateLocalEntity(request, context);
		// }
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();

			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);
			if (remoteHost.canDoSingleOp()) {
				unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
					String body;
					try {
						body = JsonUtils.toString(compacted);
					} catch (IOException e) {
						return Uni.createFrom().item(new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
								entityId, remoteHost.tenant()));
					}

					return HttpUtils
							.connect(webClient,
									remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId
											+ "/attrs/" + context.compactIri(request.getAttribName()),
									tenant, AppConstants.PATCH_OP, AppConstants.NGB_APPLICATION_JSON, null,
									toFrwd, body, viaHeaders,
									remoteHost.cSourceAlias(), -1)
							.onItemOrFailure()
							.transform((response, failure) -> {
								return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(204),
										remoteHost, AppConstants.PARTIAL_UPDATE_REQUEST, entityId,
										HttpUtils.getAttribsFromCompactedPayload(compacted));
							});
				}));
			}

		}
		if (localEntity != null && !localEntity.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteEntitiesAndHosts, entityId)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}

			request.setPayloadFromSingle(entityId, localEntity);
			unis.add(partialUpdateLocalEntity(request, entityId, context).onFailure().recoverWithItem(e -> {
				NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.PARTIAL_UPDATE_REQUEST,
						entityId, tenant);
				if (e instanceof ResponseException) {
					localResult.addFailure((ResponseException) e);
				} else {
					localResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

				return localResult;

			}));
		}
		return Uni.combine().all().unis(unis).with(list -> {
			return getResult(list);
		});
	}

	public Uni<Boolean> patchToEndPoint(String entityId, HttpServerRequest request, Map<String, Object> inputBody,
			String attrId, ViaHeaders viaHeaders) {
		String tenantId = HttpUtils.getTenant(request);
		return entityDAO.getEndpoint(entityId, tenantId).onItem().transformToUni(t -> {
			String endPoint = t.getItem1();
			String csourceAlias = t.getItem2();
			if (endPoint != null && !endPoint.equals("")) {
				String body;
				try {
					body = JsonUtils.toString(inputBody);
				} catch (IOException e) {
					return Uni.createFrom().item(false);
				}

				return HttpUtils
						.connect(webClient,
								endPoint + "/ngsi-ld/v1/entities/" + entityId + "/attrs/" + attrId,
								tenantId, AppConstants.PATCH_OP, AppConstants.NGB_APPLICATION_JSON, null,
								null, body, viaHeaders,
								csourceAlias, -1)
						.onItem().transform(ar -> {
							logger.trace("patchToEndPoint() :: completed");
							return true;
						});
			}
			return Uni.createFrom().item(false);
		});
	}

	public Uni<NGSILDOperationResult> deleteAttribute(String tenant, String entityId, String attribName,
			String datasetId, boolean deleteAll, Context context, io.vertx.core.MultiMap headersFromReq,
			ViaHeaders viaHeaders) {
		DeleteAttributeRequest request = new DeleteAttributeRequest(tenant, entityId, attribName, datasetId, deleteAll,
				zip);
		Set<RemoteHost> remoteHosts = getRemoteHostsForDeleteAttrib(request, entityId);
		// if (remoteHosts.isEmpty()) {
		// return localDeleteAttrib(request, context);
		// }
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteHosts.size());
		for (RemoteHost remoteHost : remoteHosts) {
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);
			String url = remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId + "/attrs/"
					+ context.compactIri(attribName);
			// ponytail: only forward non-default query params; deleteAll=false / absent datasetId are
			// the defaults, and a spurious "?deleteAll=false" breaks exact-match Context Sources.
			Map<String, String> queryParams = null;
			if (deleteAll || datasetId != null) {
				queryParams = new HashMap<>(2);
				if (deleteAll) {
					queryParams.put(NGSIConstants.QUERY_PARAMETER_DELETE_ALL, "true");
				}
				if (datasetId != null) {
					queryParams.put(NGSIConstants.QUERY_PARAMETER_DATA_SET_ID, datasetId);
				}
			}

			unis.add(HttpUtils
					.connect(webClient,
							url,
							tenant, AppConstants.DELETE_OP, null, queryParams,
							toFrwd, null, viaHeaders,
							remoteHost.cSourceAlias(), -1)
					.onItemOrFailure().transform((response, failure) -> {
						Set<Attrib> attribs = new HashSet<>();
						attribs.add(new Attrib(attribName, entityId));
						return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(204), remoteHost,
								AppConstants.DELETE_REQUEST, entityId, attribs);

					}));
		}
		if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteHosts, entityId)) {
			request.setDistributed(true);
		} else {
			request.setDistributed(false);
		}
		unis.add(localDeleteAttrib(request, entityId));
		return Uni.combine().all().unis(unis).with(list -> getResult(list));

	}

	private boolean isDifferentRemoteQueryAvailable(BaseRequest request, Set<RemoteHost> remoteHosts, String entityId) {
		if (!remoteHosts.isEmpty()) {
			return true;
		}
		return isRemoteQueryPossible(request.getPayload().get(entityId).get(0), request.getTenant(), entityId);
	}

	private Uni<NGSILDOperationResult> localDeleteAttrib(DeleteAttributeRequest request, String entityId) {
		return entityDAO.deleteAttribute(request).onItem().transformToUni(resultEntity -> {
			request.setPrevPayloadFromSingle(entityId, resultEntity);
			try {
				microServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.DELETE_ATTRIBUTE_REQUEST, entityId,
					request.getTenant());
			result.addSuccess(new CRUDSuccess(null, null, null,
					Set.of(new Attrib(request.getAttribName(), request.getDatasetId()))));
			return Uni.createFrom().item(result);
		}).onFailure().recoverWithUni(err -> {
			// The current representation may not exist (e.g. the entity was created only via the
			// temporal API). The temporal evolution can still record the soft-delete deletedAt
			// tombstone, so emit the event regardless; the original 404 is preserved for callers.
			try {
				microServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			} catch (ResponseException ignore) {
			}
			return Uni.createFrom().failure(err);
		});
	}

	private Set<RemoteHost> getRemoteHostsForDeleteAttrib(DeleteAttributeRequest request, String entityId) {
		Set<RemoteHost> result = Sets.newHashSet();
		for (List<RegistrationEntry> regEntries : tenant2CId2RegEntries.row(request.getTenant()).values()) {
			for (RegistrationEntry regEntry : regEntries) {
				boolean matches = ((regEntry.eId() == null && regEntry.eIdp() == null)
						|| (regEntry.eId() != null && regEntry.eId().equals(entityId))
						|| (regEntry.eIdp() != null && entityId.matches(regEntry.eIdp())))
						&& ((regEntry.eProp() == null && regEntry.eRel() == null)
								|| (regEntry.eProp() != null && regEntry.eProp().equals(request.getAttribName()))
								|| (regEntry.eRel() != null && regEntry.eRel().equals(request.getAttribName())));
				if (!matches) {
					continue;
				}
				if (!regEntry.deleteEntity() && !regEntry.deleteBatch()) {
					if (regEntry.regMode() > 1) {
						throw new RuntimeException(new ResponseException(ErrorType.OperationNotSupported, "Operation not supported by exclusive/redirect registration"));
					}
					continue;
				}
				result.add(new RemoteHost(regEntry.host().host(), regEntry.host().tenant(),
							regEntry.host().headers(), regEntry.host().cSourceId(), regEntry.deleteEntity(),
							regEntry.deleteBatch(), regEntry.regMode(), false, regEntry.queryEntityMap(),
							regEntry.host().cSourceAlias()));
			}
		}
		return result;
	}

	public Uni<NGSILDOperationResult> deleteEntity(String tenant, String entityId, Context context,
			io.vertx.core.MultiMap headersFromReq, ViaHeaders viaHeaders, boolean localOnly) {
		DeleteEntityRequest request = new DeleteEntityRequest(tenant, entityId, zip);
		// local=true -> do not forward to Context Sources (NGSI-LD 6.3.18).
		Set<RemoteHost> remoteHosts = localOnly ? Set.of() : getRemoteHostsForDelete(request, entityId);

		// if (remoteHosts.isEmpty()) {
		// return localDeleteEntity(request, context);
		// }
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteHosts.size());
		for (RemoteHost remoteHost : remoteHosts) {
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);
			if (remoteHost.canDoSingleOp()) {
				String url = remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId;

				unis.add(HttpUtils
						.connect(webClient,
								url,
								tenant, AppConstants.DELETE_OP, null, null,
								toFrwd, null, viaHeaders,
								remoteHost.cSourceAlias(), -1)
						.onItemOrFailure().transform((response, failure) -> {
							Set<Attrib> attribs = new HashSet<>();
							attribs.add(new Attrib(null, entityId));
							return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(204), remoteHost,
									AppConstants.DELETE_REQUEST, entityId, attribs);

						}));
			} else {
				String body;
				try {
					body = JsonUtils.toString(List.of(entityId));
				} catch (IOException e) {
					continue;
				}

				unis.add(HttpUtils
						.connect(webClient,
								remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_DELETE,
								tenant, AppConstants.POST_OP, AppConstants.NGB_APPLICATION_JSON, null,
								toFrwd, body, viaHeaders,
								remoteHost.cSourceAlias(), -1)
						.onItemOrFailure()
						.transform((response, failure) -> {
							return handleBatchDeleteResponse(response, failure, remoteHost, List.of(entityId),
									ArrayUtils.toArray(204)).get(0);
						}));
			}
		}
		if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteHosts, entityId)) {
			request.setDistributed(true);
		} else {
			request.setDistributed(false);
		}

		Uni<NGSILDOperationResult> localUni = localDeleteEntity(request, entityId, context);
		if (!remoteHosts.isEmpty()) {
			// Distributed delete: a missing local entity must not abort the whole operation. In
			// inclusive/exclusive mode the local miss is reported as a per-source failure (-> 207);
			// in redirect mode the entity is never stored locally, so the miss is ignored (-> 204
			// when the Context Source(s) succeed). NGSI-LD 5.6.6.
			boolean anyRedirect = false;
			for (RemoteHost rh : remoteHosts) {
				if (rh.regMode() == 2) {
					anyRedirect = true;
					break;
				}
			}
			final boolean ignoreLocalMiss = anyRedirect;
			localUni = localUni.onFailure().recoverWithItem(e -> {
				NGSILDOperationResult r = new NGSILDOperationResult(AppConstants.DELETE_REQUEST, entityId, tenant);
				if (!ignoreLocalMiss && e instanceof ResponseException re) {
					r.addFailure(re);
				}
				return r;
			});
		}
		unis.add(localUni);
		return Uni.combine().all().unis(unis).with(list -> getResult(list));

	}

	// NGSI-LD 5.6.21 / 6.4.3.3 Purge Entities. Resolve the matching set, then delete each match (no
	// keep/drop), strip the listed attributes (drop), or strip all-but-listed attributes (keep).
	// useListQuery -> reuse the broker's GET /entities (full type/q/geo/local semantics); otherwise the
	// selector is id-only, which the list-query endpoint rejects, so retrieve each id directly.
	public Uni<Void> purgeEntities(String tenant, String cleanedQuery, String idParam, boolean useListQuery,
			String keep, String drop, boolean localOnly, Context context, io.vertx.core.MultiMap headersFromReq,
			ViaHeaders viaHeaders) {
		final boolean dropMode = drop != null && !drop.isEmpty();
		final Set<String> dropSet = dropMode ? new HashSet<>(Arrays.asList(drop.split(","))) : Set.of();
		final boolean keepMode = keep != null && !keep.isEmpty();
		final Set<String> keepSet = keepMode ? new HashSet<>(Arrays.asList(keep.split(","))) : Set.of();
		String link = headersFromReq.get("Link");
		String tenantH = headersFromReq.get(NGSIConstants.TENANT_HEADER);

		Uni<List<JsonObject>> matchesUni;
		if (useListQuery) {
			String url = microServiceUtils.getGatewayString() + "ngsi-ld/v1/entities"
					+ (cleanedQuery == null || cleanedQuery.isEmpty() ? "" : "?" + cleanedQuery);
			matchesUni = sendForJson(url, tenantH, link).onItem().transform(resp -> {
				List<JsonObject> out = new ArrayList<>();
				try {
					JsonArray arr = resp.bodyAsJsonArray();
					for (int i = 0; i < arr.size(); i++) {
						out.add(arr.getJsonObject(i));
					}
				} catch (Exception ignored) {
				}
				return out;
			});
		} else {
			// id-only selector: retrieve each id directly (GET /entities/{id}); skip 404s.
			List<Uni<JsonObject>> idUnis = new ArrayList<>();
			for (String id : idParam.split(",")) {
				String url = microServiceUtils.getGatewayString() + "ngsi-ld/v1/entities/" + id.trim();
				idUnis.add(sendForJson(url, tenantH, link).onItem().transform(resp -> {
					try {
						return resp.statusCode() == 200 ? resp.bodyAsJsonObject() : null;
					} catch (Exception e) {
						return null;
					}
				}));
			}
			matchesUni = Uni.combine().all().unis(idUnis).with(list -> {
				List<JsonObject> out = new ArrayList<>();
				for (Object o : list) {
					if (o != null) {
						out.add((JsonObject) o);
					}
				}
				return out;
			});
		}

		return matchesUni.onItem().transformToUni(entities -> {
			if (entities.isEmpty()) {
				return Uni.createFrom().voidItem();
			}
			List<Uni<NGSILDOperationResult>> unis = new ArrayList<>();
			for (JsonObject entity : entities) {
				String id = entity.getString("id");
				if (id == null) {
					continue;
				}
				if (!dropMode && !keepMode) {
					unis.add(deleteEntity(tenant, id, context, headersFromReq, viaHeaders, localOnly));
				} else if (dropMode) {
					for (String attr : dropSet) {
						unis.add(deleteAttribute(tenant, id, context.expandIri(attr.trim(), false, true, null, null),
								null, true, context, headersFromReq, viaHeaders));
					}
				} else {
					for (String key : entity.fieldNames()) {
						if (key.equals("id") || key.equals("type") || key.equals("@context")
								|| key.equals(NGSIConstants.SCOPE) || keepSet.contains(key)) {
							continue;
						}
						unis.add(deleteAttribute(tenant, id, context.expandIri(key, false, true, null, null), null,
								true, context, headersFromReq, viaHeaders));
					}
				}
			}
			if (unis.isEmpty()) {
				return Uni.createFrom().voidItem();
			}
			return Uni.combine().all().unis(unis).with(list -> (Void) null);
		});
	}

	// Self-call GET as compact application/json, forwarding tenant + @context Link so matching and
	// short-name compaction match the original request.
	private Uni<io.vertx.mutiny.ext.web.client.HttpResponse<Buffer>> sendForJson(String url, String tenantH,
			String link) {
		var req = webClient.getAbs(url);
		if (tenantH != null) {
			req.putHeader(NGSIConstants.TENANT_HEADER, tenantH);
		}
		if (link != null) {
			req.putHeader("Link", link);
		}
		req.putHeader("Accept", AppConstants.NGB_APPLICATION_JSON);
		return req.send();
	}

	private List<NGSILDOperationResult> handleBatchDeleteResponse(HttpResponse<Buffer> response, Throwable failure,
			RemoteHost remoteHost, List<String> of, Integer[] array) {
		// TODO Auto-generated method stub
		return null;
	}

	private Uni<NGSILDOperationResult> localDeleteEntity(DeleteEntityRequest request, String entityId,
			Context context) {
		return entityDAO.deleteEntity(request).onItem().transformToUni(deleted -> {
			// request.setPayload(deleted);
			request.setPrevPayloadFromSingle(entityId, deleted);
			try {
				microServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			NGSILDOperationResult result = new NGSILDOperationResult(AppConstants.DELETE_REQUEST, entityId,
					request.getTenant());
			result.addSuccess(new CRUDSuccess(null, null, null, deleted, context));
			return Uni.createFrom().item(result);
		});
	}

	private Set<RemoteHost> getRemoteHostsForDelete(DeleteEntityRequest request, String entityId) {
		Set<RemoteHost> result = Sets.newHashSet();
		for (List<RegistrationEntry> regEntries : tenant2CId2RegEntries.row(request.getTenant()).values()) {
			for (RegistrationEntry regEntry : regEntries) {
				boolean matches = (regEntry.eId() == null && regEntry.eIdp() == null)
						|| (regEntry.eId() != null && regEntry.eId().equals(entityId))
						|| (regEntry.eIdp() != null && entityId.matches(regEntry.eIdp()));
				if (!matches) {
					continue;
				}
				if (!regEntry.deleteEntity() && !regEntry.deleteBatch()) {
					if (regEntry.regMode() > 1) {
						throw new RuntimeException(new ResponseException(ErrorType.OperationNotSupported, "Operation not supported by exclusive/redirect registration"));
					}
					continue;
				}
				result.add(new RemoteHost(regEntry.host().host(), regEntry.host().tenant(),
							regEntry.host().headers(), regEntry.host().cSourceId(), regEntry.deleteEntity(),
							regEntry.deleteBatch(), regEntry.regMode(), false, regEntry.queryEntityMap(),
							regEntry.host().cSourceAlias()));
			}
		}
		return result;
	}

	public Uni<NGSILDOperationResult> appendToEntity(String tenant, String entityId, Map<String, Object> payload,
			boolean noOverwrite, Context context, io.vertx.core.MultiMap headersFromReq, ViaHeaders viaHeaders) {
		AppendEntityRequest request = new AppendEntityRequest(tenant, entityId, payload, zip);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> localAndRemote = splitEntity(
				request, entityId);
		Map<String, Object> localEntity = localAndRemote.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = localAndRemote.getItem2();
		// if (remoteEntitiesAndHosts.isEmpty()) {
		// request.setPayload(localEntity);
		// return appendLocal(request, noOverwrite, context);
		// }
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);
			if (remoteHost.canDoSingleOp()) {
				unis.add(prepareSplitUpEntityForSending(remoteEntityAndHost.getItem2(), context).onItem()
						.transformToUni(compacted -> {

							String body;
							try {
								body = JsonUtils.toString(compacted);
							} catch (IOException e) {
								return Uni.createFrom().item(new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
										entityId, remoteHost.tenant()));
							}
							return HttpUtils
									.connect(webClient,
											remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/"
													+ entityId + "/attrs/",
											tenant, AppConstants.POST_OP, AppConstants.NGB_APPLICATION_JSON, null,
											toFrwd, body, viaHeaders,
											remoteHost.cSourceAlias(), -1)
									.onItemOrFailure()
									.transform((response, failure) -> {
										return HttpUtils.handleWebResponse(response, failure,
												ArrayUtils.toArray(204),
												remoteHost, AppConstants.APPEND_REQUEST, entityId,
												HttpUtils.getAttribsFromCompactedPayload(compacted));
									});

						}));
			} else {
				unis.add(prepareSplitUpEntityForSending(remoteEntityAndHost.getItem2(), context).onItem()
						.transformToUni(compacted -> {
							compacted.put(NGSIConstants.QUERY_PARAMETER_ID, entityId);
							String body;
							try {
								body = JsonUtils.toString(List.of(compacted));
							} catch (IOException e) {
								return Uni.createFrom().item(new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
										entityId, remoteHost.tenant()));
							}

							return HttpUtils
									.connect(webClient,
											remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_UPDATE,
											tenant, AppConstants.POST_OP, AppConstants.NGB_APPLICATION_JSON, null,
											toFrwd, body, viaHeaders,
											remoteHost.cSourceAlias(), -1)
									.onItemOrFailure().transform((response, failure) -> {
										return handleBatchResponse(response, failure, remoteHost,
												Lists.newArrayList(compacted), ArrayUtils.toArray(201)).get(0);
									});
						}));
			}

		}
		if (localEntity != null && !localEntity.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteEntitiesAndHosts, entityId)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			request.setPayloadFromSingle(entityId, localEntity);
			unis.add(appendLocal(request, entityId, noOverwrite, context).onFailure().recoverWithItem(e -> {
				NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, entityId,
						tenant);
				if (e instanceof ResponseException) {
					localResult.addFailure((ResponseException) e);
				} else {
					localResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

				return localResult;

			}));
		}
		return Uni.combine().all().unis(unis).with(list -> {
			return getResult(list);
		});
	}

	public Uni<NGSILDOperationResult> updateEntity(String tenant, String entityId, Map<String, Object> payload,
			Context context, io.vertx.core.MultiMap headersFromReq, ViaHeaders viaHeaders) {
		UpdateEntityRequest request = new UpdateEntityRequest(tenant, entityId, payload, null, zip);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> localAndRemote = splitEntity(
				request, entityId);
		Map<String, Object> localEntity = localAndRemote.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = localAndRemote.getItem2();
		// if (remoteEntitiesAndHosts.isEmpty()) {
		// request.setPayload(localEntity);
		// return updateLocalEntity(request, context);
		// }
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);

			unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
				String body;
				try {
					body = JsonUtils.toString(compacted);
				} catch (IOException e) {
					return Uni.createFrom().item(new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
							entityId, remoteHost.tenant()));
				}

				return HttpUtils
						.connect(webClient,
								remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId + "/attrs",
								tenant, AppConstants.PATCH_OP, AppConstants.NGB_APPLICATION_JSON, null,
								toFrwd, body, viaHeaders,
								remoteHost.cSourceAlias(), -1)
						.onItemOrFailure()
						.transform((response, failure) -> {
							return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201), remoteHost,
									AppConstants.UPDATE_REQUEST, entityId,
									HttpUtils.getAttribsFromCompactedPayload(compacted));
						});
			}));

		}
		if (localEntity != null && !localEntity.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteEntitiesAndHosts, entityId)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			request.setPayloadFromSingle(entityId, localEntity);
			unis.add(updateLocalEntity(request, entityId, context).onFailure().recoverWithItem(e -> {
				NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.UPDATE_REQUEST, entityId,
						tenant);
				if (e instanceof ResponseException) {
					localResult.addFailure((ResponseException) e);
				} else {
					localResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

				return localResult;

			}));
		}
		return Uni.combine().all().unis(unis).with(list -> {
			return getResult(list);
		});
	}

	private NGSILDOperationResult getResult(List<?> list) {
		Iterator<?> it = list.iterator();
		NGSILDOperationResult operationResult = (NGSILDOperationResult) it.next();
		while (it.hasNext()) {
			NGSILDOperationResult tmp = (NGSILDOperationResult) it.next();
			operationResult.getSuccesses().addAll(tmp.getSuccesses());
			operationResult.getFailures().addAll(tmp.getFailures());
		}
		return operationResult;
	}

	private Uni<NGSILDOperationResult> updateLocalEntity(UpdateEntityRequest request, String entityId,
			Context context) {
		return entityDAO.updateExistingAttribs(request).onItem().transformToUni(previousEntity -> {

			NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.UPDATE_REQUEST, entityId,
					request.getTenant());
			Map<String, Object> payload = request.getFirstPayload();
			// Update Attributes: regular attributes are appended/updated, but scope is only
			// replaced when already present -> if absent it is not added and is reported as
			// notUpdated (drives a 207). Drop it from the emitted payload too.
			if (payload.containsKey(NGSIConstants.NGSI_LD_SCOPE)
					&& !previousEntity.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
				payload.remove(NGSIConstants.NGSI_LD_SCOPE);
				localResult.addNotUpdated(NGSIConstants.NGSI_LD_SCOPE);
			}

			request.setPrevPayloadFromSingle(entityId, previousEntity);
			try {
				emitUpdateWithNullDeletes(request, entityId, previousEntity);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}

			localResult.addSuccess(new CRUDSuccess(null, null, null, payload, context));
			return Uni.createFrom().item(localResult);
		});
	}

	public Uni<NGSILDOperationResult> createEntity(String tenant, Map<String, Object> resolved, Context context,
			io.vertx.core.MultiMap headersFromReq, ViaHeaders viaHeaders) {
		return createEntity(tenant, resolved, context, headersFromReq, viaHeaders, false);
	}

	public Uni<NGSILDOperationResult> createEntity(String tenant, Map<String, Object> resolved, Context context,
			io.vertx.core.MultiMap headersFromReq, ViaHeaders viaHeaders, boolean localOnly) {
		logger.debug("createMessage() :: started");
		String entityId = (String) resolved.get(NGSIConstants.JSON_LD_ID);
		CreateEntityRequest request = new CreateEntityRequest(tenant, resolved, zip);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> localAndRemote = splitEntity(
				request, entityId);
		Map<String, Object> localEntity = localAndRemote.getItem1();
		// local=true (NGSI-LD 6.3.18): handle only on this broker, do not forward to Context Sources.
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = localOnly ? List.of()
				: localAndRemote.getItem2();
		// if (remoteEntitiesAndHosts.isEmpty()) {
		// request.setPayload(localEntity);
		// return createLocalEntity(request, context);
		// }
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);

			if (remoteHost.canDoSingleOp()) {
				unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
					String body;
					try {
						body = JsonUtils.toString(compacted);
					} catch (IOException e) {
						return Uni.createFrom().item(new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
								entityId, remoteHost.tenant()));
					}

					return HttpUtils
							.connect(webClient,
									remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT,
									tenant, AppConstants.POST_OP, AppConstants.NGB_APPLICATION_JSON, null,
									toFrwd, body, viaHeaders,
									remoteHost.cSourceAlias(), -1)
							.onItemOrFailure()
							.transform((response, failure) -> {
								return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
										remoteHost, AppConstants.CREATE_REQUEST, entityId,
										HttpUtils.getAttribsFromCompactedPayload(compacted));
							});
				}));
			} else {
				unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
					String body;
					try {
						body = JsonUtils.toString(List.of(compacted));
					} catch (IOException e) {
						return Uni.createFrom().item(new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
								entityId, remoteHost.tenant()));
					}

					return HttpUtils
							.connect(webClient,
									remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_CREATE,
									tenant, AppConstants.POST_OP, AppConstants.NGB_APPLICATION_JSON, null,
									toFrwd, body, viaHeaders,
									remoteHost.cSourceAlias(), -1)
							.onItemOrFailure()
							.transform((response, failure) -> {
								return handleBatchResponse(response, failure, remoteHost, Lists.newArrayList(compacted),
										ArrayUtils.toArray(201)).get(0);
							});
				}));
			}
		}
		if (localEntity != null && !localEntity.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteEntitiesAndHosts, entityId)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			request.setPayloadFromSingle(entityId, localEntity);
			unis.add(0, createLocalEntity(request, entityId, context).onFailure().recoverWithItem(e -> {
				NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, entityId,
						tenant);
				if (e instanceof ResponseException) {
					localResult.addFailure((ResponseException) e);
				} else {
					localResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

				return localResult;

			}));
		}
		return Uni.combine().all().unis(unis).with(list -> {
			return getResult(list);
		});
	}

	private boolean isDifferentRemoteQueryAvailable(BaseRequest request,
			Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts, String entityId) {
		if (!remoteEntitiesAndHosts.isEmpty()) {
			return true;
		}
		return isRemoteQueryPossible(request.getPayload().get(entityId).get(0), request.getTenant(), entityId);
	}

	private Uni<NGSILDOperationResult> createLocalEntity(CreateEntityRequest request, String entityId,
			Context context) {
		return entityDAO.createEntity(request).onItem().transformToUni(v -> {
			try {
				microServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, entityId,
					request.getTenant());
			localResult
					.addSuccess(new CRUDSuccess(null, null, null, request.getPayload().get(entityId).get(0), context));
			return Uni.createFrom().item(localResult);
		});
	}

	private Uni<NGSILDOperationResult> partialUpdateLocalEntity(UpdateEntityRequest request, String entityId,
			Context context) {
		// An NGSI-LD Null fragment means "delete the instance" and is handled by the generic
		// update path; a genuine partial update needs ngsild_partialupdate (correct replace
		// semantics + 404 when the attribute is absent). ponytail: route on the sentinel,
		// which only appears as a delete marker, never as a real stored value.
		Object attrVal = request.getFirstPayload().get(request.getAttribName());
		boolean nullDelete = attrVal != null && attrVal.toString().contains(NGSIConstants.NGSI_LD_NULL);
		Uni<Map<String, Object>> daoCall = nullDelete ? entityDAO.updateEntity(request)
				: entityDAO.partialUpdateAttribute(request);
		return daoCall.onItem().transformToUni(v -> {
			NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.PARTIAL_UPDATE_REQUEST,
					entityId, request.getTenant());
			request.setPrevPayloadFromSingle(entityId, v);

			try {
				emitUpdateWithNullDeletes(request, entityId, v);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			localResult
					.addSuccess(new CRUDSuccess(null, null, null, request.getPayload().get(entityId).get(0), context));
			return Uni.createFrom().item(localResult);
		});
	}

	// NGSI-LD 1.6 null in an Update-Attributes / Partial-Update operation deletes the attribute. To make
	// attributeDeleted subscriptions fire (and the notification carry the urn:ngsi-ld:null tombstone with
	// the full entity), emit a DeleteAttributeRequest for each null'd attribute - exactly like Merge Patch
	// does - and emit the original update only for the remaining (genuinely updated) attributes.
	@SuppressWarnings("unchecked")
	private void emitUpdateWithNullDeletes(BaseRequest request, String entityId, Map<String, Object> prevEntity)
			throws ResponseException {
		Map<String, Object> payload = request.getPayload().get(entityId).get(0);
		List<String> nullAttrs = Lists.newArrayList();
		for (Entry<String, Object> e : payload.entrySet()) {
			if (e.getKey().startsWith("@")) {
				continue;
			}
			if (e.getValue() != null && e.getValue().toString().contains(NGSIConstants.NGSI_LD_NULL)) {
				nullAttrs.add(e.getKey());
			}
		}
		if (nullAttrs.isEmpty()) {
			microServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			return;
		}
		for (String attr : nullAttrs) {
			String datasetId = null;
			Object val = payload.get(attr);
			if (val instanceof List<?> l && !l.isEmpty() && l.get(0) instanceof Map) {
				Object ds = ((Map<String, Object>) l.get(0)).get(NGSIConstants.NGSI_LD_DATA_SET_ID);
				if (ds instanceof List<?> dl && !dl.isEmpty() && dl.get(0) instanceof Map) {
					datasetId = (String) ((Map<String, Object>) dl.get(0)).get(NGSIConstants.JSON_LD_ID);
				}
			}
			DeleteAttributeRequest delReq = new DeleteAttributeRequest(request.getTenant(), entityId, attr, datasetId,
					false, zip);
			delReq.setPrevPayloadFromSingle(entityId, prevEntity);
			microServiceUtils.serializeAndSplitObjectAndEmit(delReq, messageSize, entityEmitter, objectMapper);
		}
		boolean hasNonNull = payload.keySet().stream().anyMatch(k -> !k.startsWith("@") && !nullAttrs.contains(k));
		if (hasNonNull) {
			microServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
		}
	}

	private Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> splitEntity(
			BaseRequest request, String entityId) {
		Map<String, Object> originalEntity = request.getPayload().get(entityId).get(0);
		Collection<List<RegistrationEntry>> tenantRegs = tenant2CId2RegEntries.row(request.getTenant()).values();

		Object originalScopes = originalEntity.remove(NGSIConstants.NGSI_LD_SCOPE);
		originalEntity.remove(NGSIConstants.JSON_LD_ID);
		List<String> originalTypes = (List<String>) originalEntity.remove(NGSIConstants.JSON_LD_TYPE);
		Map<String, Tuple2<RemoteHost, Map<String, Object>>> cId2RemoteHostEntity = Maps.newHashMap();
		Shape location = null;
		Set<String> toBeRemoved = Sets.newHashSet();
		for (Entry<String, Object> entry : originalEntity.entrySet()) {
			for (List<RegistrationEntry> regs : tenantRegs) {
				Iterator<RegistrationEntry> it = regs.iterator();
				while (it.hasNext()) {
					RegistrationEntry regEntry = it.next();
					if (regEntry.expiresAt() > System.currentTimeMillis()) {
						it.remove();
						continue;
					}
					boolean opSupported = true;
					switch (request.getRequestType()) {
						case AppConstants.CREATE_REQUEST:
							if (!regEntry.createEntity() && !regEntry.createBatch()) {
								opSupported = false;
							}
							break;
						case AppConstants.UPDATE_REQUEST:
						case AppConstants.MERGE_PATCH_REQUEST:
						case AppConstants.REPLACE_ENTITY_REQUEST:
							if (!regEntry.updateEntity()) {
								opSupported = false;
							}
							break;
						case AppConstants.PARTIAL_UPDATE_REQUEST:
						case AppConstants.REPLACE_ATTRIBUTE_REQUEST:
							if (!regEntry.updateAttrs()) {
								opSupported = false;
							}
							break;
						case AppConstants.APPEND_REQUEST:
							if (!regEntry.appendAttrs() && !regEntry.updateBatch()) {
								opSupported = false;
							}
							break;
						case AppConstants.UPSERT_REQUEST:
							if (!regEntry.upsertBatch() && !regEntry.appendAttrs() && !regEntry.createEntity()) {
								opSupported = false;
							}
							break;
						default:
							opSupported = false;
					}
					if (!opSupported) {
						if (regEntry.regMode() > 1) {
							throw new RuntimeException(new ResponseException(ErrorType.OperationNotSupported, "Operation not supported by exclusive/redirect registration"));
						}
						continue;
					}

					List<Map<String, Object>> attrInstances = (List<Map<String, Object>>) entry.getValue();
					Object typeVal = attrInstances.isEmpty() ? null
							: attrInstances.get(0).get(NGSIConstants.JSON_LD_TYPE);
					// ponytail: value-only fragments (partial update) carry no @type -> treat as property
					String propType = (typeVal instanceof List && !((List<?>) typeVal).isEmpty())
							? (String) ((List<?>) typeVal).get(0)
							: null;
					Tuple2<Set<String>, Set<String>> matches;
					if (NGSIConstants.NGSI_LD_RELATIONSHIP.equals(propType)) {
						matches = regEntry.matches(entityId, originalTypes, null, entry.getKey(), originalScopes,
								location);
					} else {
						matches = regEntry.matches(entityId, originalTypes, entry.getKey(), null, originalScopes,
								location);
					}
					if (matches != null) {
						Map<String, Object> tmp;
						if (cId2RemoteHostEntity.containsKey(regEntry.cId())) {
							tmp = cId2RemoteHostEntity.get(regEntry.cId()).getItem2();
							if (matches.getItem1() != null) {
								((Set<String>) tmp.get(NGSIConstants.JSON_LD_TYPE)).addAll(matches.getItem1());
							}
							if (matches.getItem2() != null && !matches.getItem2().isEmpty()) {
								if (!tmp.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
									tmp.put(NGSIConstants.NGSI_LD_SCOPE, matches.getItem2());
								} else {
									((Set<String>) tmp.get(NGSIConstants.NGSI_LD_SCOPE)).addAll(matches.getItem2());
								}

							}
						} else {
							RemoteHost regHost = regEntry.host();
							RemoteHost host;
							switch (request.getRequestType()) {
								case AppConstants.CREATE_REQUEST:
									host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
											regHost.cSourceId(), regEntry.createEntity(), regEntry.createBatch(),
											regEntry.regMode(), false, regEntry.queryEntityMap(),
											regEntry.host().cSourceAlias());
									break;
								case AppConstants.UPDATE_REQUEST:
									host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
											regHost.cSourceId(), regEntry.updateAttrs(), regEntry.updateBatch(),
											regEntry.regMode(), false, regEntry.queryEntityMap(),
											regEntry.host().cSourceAlias());
									break;
								case AppConstants.MERGE_PATCH_REQUEST:
									host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
											regHost.cSourceId(), regEntry.mergeEntity(), regEntry.mergeBatch(),
											regEntry.regMode(), false, regEntry.queryEntityMap(),
											regEntry.host().cSourceAlias());
									break;
								case AppConstants.REPLACE_ENTITY_REQUEST:
									host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
											regHost.cSourceId(), regEntry.replaceEntity(), false, regEntry.regMode(),
											false,
											regEntry.queryEntityMap(), regEntry.host().cSourceAlias());
									break;
								case AppConstants.REPLACE_ATTRIBUTE_REQUEST:
									host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
											regHost.cSourceId(), regEntry.replaceAttrs(), false, regEntry.regMode(),
											false,
											regEntry.queryEntityMap(), regEntry.host().cSourceAlias());
								case AppConstants.PARTIAL_UPDATE_REQUEST:
									host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
											regHost.cSourceId(), regEntry.updateAttrs(), false, regEntry.regMode(),
											false,
											regEntry.queryEntityMap(), regEntry.host().cSourceAlias());
									break;
								case AppConstants.APPEND_REQUEST:
									host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
											regHost.cSourceId(), regEntry.appendAttrs(), regEntry.updateBatch(),
											regEntry.regMode(), false, regEntry.queryEntityMap(),
											regEntry.host().cSourceAlias());
									break;
								case AppConstants.UPSERT_REQUEST:
									host = new RemoteHost(regHost.host(), regHost.tenant(), regHost.headers(),
											regHost.cSourceId(), (regEntry.appendAttrs() && regEntry.createEntity()),
											regEntry.upsertBatch(), regEntry.regMode(), false,
											regEntry.queryEntityMap(), regEntry.host().cSourceAlias());
									break;
								default:
									return null;
							}

							tmp = Maps.newHashMap();
							tmp.put(NGSIConstants.JSON_LD_ID, entityId);
							if (matches.getItem1() != null) {
								tmp.put(NGSIConstants.JSON_LD_TYPE, matches.getItem1());
							}
							if (matches.getItem2() != null) {
								tmp.put(NGSIConstants.NGSI_LD_SCOPE, matches.getItem2());
							}
							cId2RemoteHostEntity.put(regEntry.cId(), Tuple2.of(host, tmp));
						}
						tmp.put(entry.getKey(), entry.getValue());
						if (regEntry.regMode() > 1) {
							toBeRemoved.add(entry.getKey());
							if (regEntry.regMode() == 3) {
								break;
							}
						}
					}
				}
			}
		}
		for (String s : toBeRemoved) {
			originalEntity.remove(s);
		}
		Map<String, Object> toStore;
		// Even when no attributes remain locally (minimal entity, or type/scope-only
		// operation), still store the local entity shell (id + type? + scope?). Otherwise
		// splitEntity returns a null local entity and createEntity/createBatch build an
		// empty Uni set -> "The Uni set is empty" 500. (Restores pre-refactor behavior.)
		if (originalEntity.isEmpty()) {
			toStore = new HashMap<>();
		} else if (cId2RemoteHostEntity.isEmpty()) {
			toStore = originalEntity;
		} else {
			toStore = MicroServiceUtils.deepCopyMap(originalEntity);
		}
		toStore.put(NGSIConstants.JSON_LD_ID, entityId);
		if (originalTypes != null && !originalTypes.isEmpty()) {
			toStore.put(NGSIConstants.JSON_LD_TYPE, originalTypes);
		}
		if (originalScopes != null) {
			toStore.put(NGSIConstants.NGSI_LD_SCOPE, originalScopes);
		}
		EntityTools.addSysAttrs(toStore, request.getSendTimestamp());
		return Tuple2.of(toStore, cId2RemoteHostEntity.values());
	}

	public Uni<Void> handleRegistryChange(CSourceBaseRequest req) {
		return RegistrationEntry.fromRegPayload(req.getPayload(), jsonLdService).onItem().transformToUni(regs -> {
			tenant2CId2RegEntries.remove(req.getTenant(), req.getId());
			tenant2CId2QueryRegEntries.remove(req.getTenant(), req.getId());
			if (req.getRequestType() != AppConstants.DELETE_REQUEST) {
				List<RegistrationEntry> newRegs = Lists.newArrayList();
				List<RegistrationEntry> newQueryRegs = Lists.newArrayList();
				for (RegistrationEntry regEntry : regs) {
					if ((regEntry.createEntity() || regEntry.appendAttrs() || regEntry.createBatch()
							|| regEntry.deleteAttrs() || regEntry.deleteBatch() || regEntry.deleteEntity()
							|| regEntry.mergeBatch() || regEntry.mergeEntity() || regEntry.replaceAttrs()
							|| regEntry.replaceEntity() || regEntry.updateAttrs() || regEntry.updateBatch()
							|| regEntry.updateEntity() || regEntry.upsertBatch()) && regEntry.regMode() != 0) {
						newRegs.add(regEntry);
					}
					if (regEntry.queryBatch() || regEntry.queryEntity() || regEntry.retrieveEntity()) {
						newQueryRegs.add(regEntry);
					}
				}
				tenant2CId2RegEntries.put(req.getTenant(), req.getId(), newRegs);
				tenant2CId2QueryRegEntries.put(req.getTenant(), req.getId(), newQueryRegs);
			}
			return Uni.createFrom().voidItem();
		});
	}

	private Uni<NGSILDOperationResult> appendLocal(AppendEntityRequest request, String entityId, boolean noOverwrite,
			Context context) {
		return entityDAO.appendToEntity2(request, noOverwrite).onItem().transformToUni(resultAndNotAppended -> {
			NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.APPEND_REQUEST, entityId,
					request.getTenant());
			Set<String> notAppended = resultAndNotAppended.getItem3();
			Map<String, Object> payload = request.getPayload().get(entityId).get(0);
			for (String entry : notAppended) {
				payload.remove(entry);
				// noOverwrite-skipped attribute -> notUpdated (expanded name, per the UpdateResult)
				localResult.addNotUpdated(entry);
			}
			if (!notAppended.isEmpty()) {
				// record the actually-appended attributes (expanded names) for the 207 UpdateResult body
				for (String attr : payload.keySet()) {
					if (!attr.startsWith("@") && !NGSIConstants.ENTITY_BASE_PROPS.contains(attr)) {
						localResult.addUpdated(attr);
					}
				}
			}
			request.setPrevPayloadFromSingle(entityId, resultAndNotAppended.getItem1());
			localResult.addSuccess(new CRUDSuccess(null, null, null, payload, context));
			try {
				microServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			return Uni.createFrom().item(localResult);

		});
	}

	private Uni<Map<String, Object>> prepareSplitUpEntityForSending(Map<String, Object> expanded, Context context) {
		if (expanded.containsKey(NGSIConstants.JSON_LD_TYPE)) {
			expanded.put(NGSIConstants.JSON_LD_TYPE,
					Lists.newArrayList((Set<String>) expanded.get(NGSIConstants.JSON_LD_TYPE)));
		}
		if (expanded.containsKey(NGSIConstants.NGSI_LD_SCOPE)) {
			Set<String> collectedScopes = (Set<String>) expanded.get(NGSIConstants.NGSI_LD_SCOPE);
			List<Map<String, String>> finalScopes = Lists.newArrayList();
			for (String scope : collectedScopes) {
				finalScopes.add(Map.of(NGSIConstants.JSON_LD_VALUE, scope));
			}
			expanded.put(NGSIConstants.NGSI_LD_SCOPE, finalScopes);
		}
		return jsonLdService.compact(expanded, null, context, HttpUtils.opts, -1);

	}

	public Uni<List<NGSILDOperationResult>> createBatch(String tenant, List<Map<String, Object>> expandedEntities,
			List<Context> contexts, boolean localOnly, io.vertx.core.MultiMap headersFromReq, ViaHeaders viaHeaders) {
		Iterator<Map<String, Object>> itEntities = expandedEntities.iterator();
		Iterator<Context> itContext = contexts.iterator();
		Map<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> remoteHost2Batch = Maps.newHashMap();
		Map<String, List<Map<String, Object>>> localEntities = Maps.newHashMap();
		while (itEntities.hasNext() && itContext.hasNext()) {
			Map<String, Object> entity = itEntities.next();
			String entityId = (String) entity.get(NGSIConstants.JSON_LD_ID);
			Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> split = splitEntity(
					new CreateEntityRequest(tenant, entity, zip), entityId);
			Map<String, Object> local = split.getItem1();
			Context context = itContext.next();
			if (local != null) {
				MicroServiceUtils.putIntoIdMap(localEntities, (String) local.get(NGSIConstants.JSON_LD_ID), local);

			} else {
				itContext.remove();
			}
			Collection<Tuple2<RemoteHost, Map<String, Object>>> remotes = split.getItem2();
			for (Tuple2<RemoteHost, Map<String, Object>> remote : remotes) {
				List<Tuple2<Context, Map<String, Object>>> entities2Context;
				if (remoteHost2Batch.containsKey(remote.getItem1())) {
					entities2Context = remoteHost2Batch.get(remote.getItem1());
				} else {
					entities2Context = Lists.newArrayList();
					remoteHost2Batch.put(remote.getItem1(), entities2Context);
				}
				entities2Context.add(Tuple2.of(context, remote.getItem2()));
			}
		}

		List<Uni<List<NGSILDOperationResult>>> unis = new ArrayList<>();
		if (!localOnly) {
			for (Entry<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> entry : remoteHost2Batch.entrySet()) {
				RemoteHost remoteHost = entry.getKey();
				MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);
				List<Tuple2<Context, Map<String, Object>>> tuples = entry.getValue();
				List<Uni<Map<String, Object>>> compactedUnis = Lists.newArrayList();
				for (Tuple2<Context, Map<String, Object>> tuple : tuples) {
					Map<String, Object> expanded = tuple.getItem2();
					Context context = tuple.getItem1();
					compactedUnis.add(jsonLdService.compact(expanded, null, context, AppConstants.opts, -1));
				}

				if (remoteHost.canDoBatchOp()) {
					unis.add(Uni.combine().all().unis(compactedUnis).with(list -> {
						List<Map<String, Object>> toSend = Lists.newArrayList();
						for (Object obj : list) {
							toSend.add((Map<String, Object>) obj);
						}
						return toSend;
					}).onItem().transformToUni(toSend -> {
						String body;
						try {
							body = JsonUtils.toString(toSend);
						} catch (IOException e) {
							return Uni.createFrom().item(Lists.newArrayList());
						}

						return HttpUtils
								.connect(webClient,
										remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_CREATE,
										tenant, AppConstants.POST_OP, AppConstants.NGB_APPLICATION_JSON, null,
										toFrwd, body, viaHeaders,
										remoteHost.cSourceAlias(), -1)
								.onItemOrFailure()
								.transform((response, failure) -> {
									return handleBatchResponse(response, failure, remoteHost, toSend,
											ArrayUtils.toArray(201));
								});
					}));
				} else {
					List<Uni<NGSILDOperationResult>> singleUnis = new ArrayList<>();
					for (Uni<Map<String, Object>> compactedUni : compactedUnis) {
						singleUnis.add(compactedUni.onItem().transformToUni(entity -> {
							String body;
							try {
								body = JsonUtils.toString(entity);
							} catch (IOException e) {
								return Uni.createFrom().item(new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
										"unknown", remoteHost.tenant()));
							}

							return HttpUtils
									.connect(webClient,
											remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT,
											tenant, AppConstants.POST_OP, AppConstants.NGB_APPLICATION_JSON, null,
											toFrwd, body, viaHeaders,
											remoteHost.cSourceAlias(), -1)
									.onItemOrFailure()
									.transform((response, failure) -> {
										return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
												remoteHost, AppConstants.CREATE_REQUEST,
												(String) entity.get(NGSIConstants.JSON_LD_ID),
												HttpUtils.getAttribsFromCompactedPayload(entity));
									});
						}));
					}
					unis.add(Uni.combine().all().unis(singleUnis).with(list -> {
						List<NGSILDOperationResult> result = Lists.newArrayList();
						list.forEach(obj -> result.add((NGSILDOperationResult) obj));
						return result;
					}));
				}
			}

		}
		BatchRequest request = new BatchRequest(tenant, localEntities.keySet(), localEntities,
				AppConstants.BATCH_CREATE_REQUEST, zip);
		if (!localEntities.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteHost2Batch)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			unis.add(0, entityDAO.batchCreateEntity(request).onItem().transformToUni(dbResult -> {
				List<NGSILDOperationResult> result = Lists.newArrayList();
				List<String> successes = (List<String>) dbResult.get("success");
				List<Map<String, String>> fails = (List<Map<String, String>>) dbResult.get("failure");

				for (String entityId : successes) {
					NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, entityId,
							tenant);
					opResult.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
					result.add(opResult);
				}
				Map<String, List<Map<String, Object>>> reqPayload = request.getPayload();
				for (Map<String, String> fail : fails) {
					fail.entrySet().forEach(entry -> {
						String entityId = entry.getKey();
						String sqlstate = entry.getValue();
						reqPayload.remove(entityId);
						NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST,
								entityId, tenant);
						if (sqlstate.equals(AppConstants.SQL_ALREADY_EXISTS)) {
							opResult.addFailure(new ResponseException(ErrorType.AlreadyExists, entityId));
						} else {
							opResult.addFailure(new ResponseException(ErrorType.InvalidRequest, sqlstate));
						}
						result.add(opResult);
					});

				}

				if (!reqPayload.isEmpty()) {
					logger.debug("Create batch request sending to kafka " + request.getIds());
					try {
						microServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter,
								objectMapper);
					} catch (ResponseException e) {
						return Uni.createFrom().failure(e);
					}
				}
				return Uni.createFrom().item(result);
			}));
		}

		return Uni.combine().all().unis(unis).with(resultLists -> {
			List<NGSILDOperationResult> result = Lists.newArrayList();
			resultLists.forEach(resultList -> {
				// A combined uni result may be a List, any other Collection (e.g. a HashSet from a
				// forwarded op), a single NGSILDOperationResult, or null (a forward that yielded nothing).
				// The old blind (List) cast + addAll threw ClassCastException / NPE once distributed
				// forwarding was in play, turning successful distributed batch ops into 500s.
				if (resultList == null) {
					return;
				}
				if (resultList instanceof java.util.Collection) {
					result.addAll((java.util.Collection<NGSILDOperationResult>) resultList);
				} else if (resultList instanceof NGSILDOperationResult) {
					result.add((NGSILDOperationResult) resultList);
				}
			});
			return result;
		});
	}

	public Uni<List<NGSILDOperationResult>> appendBatch(String tenant, List<Map<String, Object>> expandedEntities,
			List<Context> contexts, boolean localOnly, boolean noOverWrite, io.vertx.core.MultiMap headersFromReq,
			ViaHeaders viaHeaders) {
		Iterator<Map<String, Object>> itEntities = expandedEntities.iterator();
		Iterator<Context> itContext = contexts.iterator();
		Map<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> remoteHost2Batch = Maps.newHashMap();
		Map<String, List<Map<String, Object>>> localEntities = Maps.newHashMap();
		while (itEntities.hasNext() && itContext.hasNext()) {
			Map<String, Object> entity = itEntities.next();
			String entityId = (String) entity.get(NGSIConstants.JSON_LD_ID);
			Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> split = splitEntity(
					new AppendEntityRequest(tenant, entityId, entity, zip), entityId);
			Map<String, Object> local = split.getItem1();
			Context context = itContext.next();
			if (local != null) {
				local.remove(NGSIConstants.NGSI_LD_CREATED_AT);
				MicroServiceUtils.putIntoIdMap(localEntities, entityId, local);
			} else {
				itContext.remove();
			}
			Collection<Tuple2<RemoteHost, Map<String, Object>>> remotes = split.getItem2();
			for (Tuple2<RemoteHost, Map<String, Object>> remote : remotes) {
				List<Tuple2<Context, Map<String, Object>>> entities2Context;
				if (remoteHost2Batch.containsKey(remote.getItem1())) {
					entities2Context = remoteHost2Batch.get(remote.getItem1());
				} else {
					entities2Context = Lists.newArrayList();
					remoteHost2Batch.put(remote.getItem1(), entities2Context);
				}
				entities2Context.add(Tuple2.of(context, remote.getItem2()));
			}
		}

		List<Uni<List<NGSILDOperationResult>>> unis = new ArrayList<>();
		if (!localOnly) {
			for (Entry<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> entry : remoteHost2Batch.entrySet()) {
				RemoteHost remoteHost = entry.getKey();
				MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);

				List<Tuple2<Context, Map<String, Object>>> tuples = entry.getValue();
				List<Uni<Map<String, Object>>> compactedUnis = Lists.newArrayList();
				for (Tuple2<Context, Map<String, Object>> tuple : tuples) {
					Map<String, Object> expanded = tuple.getItem2();
					Context context = tuple.getItem1();
					compactedUnis.add(jsonLdService.compact(expanded, null, context, AppConstants.opts, -1));
				}
				if (remoteHost.canDoBatchOp()) {
					unis.add(Uni.combine().all().unis(compactedUnis).with(list -> {
						List<Map<String, Object>> toSend = Lists.newArrayList();
						for (Object obj : list) {
							toSend.add((Map<String, Object>) obj);
						}
						return toSend;
					}).onItem().transformToUni(toSend -> {
						String body;
						try {
							body = JsonUtils.toString(toSend);
						} catch (IOException e) {
							return Uni.createFrom().item(Lists.newArrayList());
						}

						return HttpUtils
								.connect(webClient,
										remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_UPDATE,
										tenant, AppConstants.POST_OP, AppConstants.NGB_APPLICATION_JSON, null,
										toFrwd, body, viaHeaders,
										remoteHost.cSourceAlias(), -1)
								.onItemOrFailure()
								.transform((response, failure) -> {
									return handleBatchResponse(response, failure, remoteHost, toSend,
											ArrayUtils.toArray(204));
								});
					}));
				} else {
					List<Uni<NGSILDOperationResult>> singleUnis = new ArrayList<>();
					for (Uni<Map<String, Object>> compactedUni : compactedUnis) {
						singleUnis.add(compactedUni.onItem().transformToUni(entity -> {
							String body;
							try {
								body = JsonUtils.toString(entity);
							} catch (IOException e) {
								return Uni.createFrom().item(new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
										"unknown", remoteHost.tenant()));
							}

							return HttpUtils
									.connect(webClient,
											remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/"
													+ entity.get(NGSIConstants.JSON_LD_ID) + "/"
													+ NGSIConstants.QUERY_PARAMETER_ATTRS,
											tenant, AppConstants.POST_OP, AppConstants.NGB_APPLICATION_JSON, null,
											toFrwd, body, viaHeaders,
											remoteHost.cSourceAlias(), -1)
									.onItemOrFailure()
									.transform((response, failure) -> {
										return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
												remoteHost, AppConstants.APPEND_REQUEST,
												(String) entity.get(NGSIConstants.JSON_LD_ID),
												HttpUtils.getAttribsFromCompactedPayload(entity));
									});
						}));
					}
					unis.add(Uni.combine().all().unis(singleUnis).with(list -> {
						List<NGSILDOperationResult> result = Lists.newArrayList();
						list.forEach(obj -> result.add((NGSILDOperationResult) obj));
						return result;
					}));
				}
			}
		}
		if (!localEntities.isEmpty()) {
			BatchRequest request = new BatchRequest(tenant, localEntities.keySet(), localEntities,
					AppConstants.BATCH_UPDATE_REQUEST, zip);
			request.setNoOverwrite(noOverWrite);
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteHost2Batch)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			Uni<List<NGSILDOperationResult>> local = entityDAO.batchAppendEntity(request).onItem()
					.transformToUni(dbResult -> {
						List<NGSILDOperationResult> result = Lists.newArrayList();
						List<Map<String, Object>> successes = (List<Map<String, Object>>) dbResult.get("success");
						List<Map<String, String>> fails = (List<Map<String, String>>) dbResult.get("failure");
						Map<String, List<Map<String, Object>>> oldEntities = Maps.newHashMap();
						for (Map<String, Object> success : successes) {
							String entityId = (String) success.get("id");
							NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
									entityId, tenant);
							opResult.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
							result.add(opResult);
							Map<String, Object> old = (Map<String, Object>) success.get("old");
							MicroServiceUtils.putIntoIdMap(oldEntities, entityId, old);
							// noOverwrite (NGSI-LD 5.6.9): attributes already present in the old entity
							// were left untouched -> report them as notUpdated so the batch returns 207.
							if (noOverWrite && old != null) {
								List<Map<String, Object>> submitted = request.getPayload().get(entityId);
								if (submitted != null) {
									for (Map<String, Object> subEntity : submitted) {
										for (String attr : subEntity.keySet()) {
											if (attr.startsWith("@")
													|| NGSIConstants.ENTITY_BASE_PROPS.contains(attr)) {
												continue;
											}
											if (old.containsKey(attr)) {
												opResult.addNotUpdated(attr);
											}
										}
									}
								}
							}

						}
						request.setPrevPayload(oldEntities);
						for (Map<String, String> fail : fails) {
							fail.entrySet().forEach(entry -> {
								String entityId = entry.getKey();
								String sqlstate = entry.getValue();
								request.getPayload().remove(entityId);
								NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
										entityId, tenant);
								if (sqlstate.equals(AppConstants.SQL_NOT_FOUND)) {
									opResult.addFailure(new ResponseException(ErrorType.NotFound, entityId));
								} else {
									opResult.addFailure(new ResponseException(ErrorType.InvalidRequest, sqlstate));
								}
								result.add(opResult);
							});

						}
						if (!request.getPayload().isEmpty()) {
							logger.debug("Append batch request sending to kafka " + request.getIds());
							try {
								microServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter,
										objectMapper);
							} catch (ResponseException e) {
								return Uni.createFrom().failure(e);
							}
						}
						return Uni.createFrom().item(result);
					});

			unis.add(0, local);
		}
		if (unis.isEmpty()) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
		}
		return Uni.combine().all().unis(unis).with(resultLists -> {
			List<NGSILDOperationResult> result = Lists.newArrayList();
			resultLists.forEach(resultList -> {
				// A combined uni result may be a List, any other Collection (e.g. a HashSet from a
				// forwarded op), a single NGSILDOperationResult, or null (a forward that yielded nothing).
				// The old blind (List) cast + addAll threw ClassCastException / NPE once distributed
				// forwarding was in play, turning successful distributed batch ops into 500s.
				if (resultList == null) {
					return;
				}
				if (resultList instanceof java.util.Collection) {
					result.addAll((java.util.Collection<NGSILDOperationResult>) resultList);
				} else if (resultList instanceof NGSILDOperationResult) {
					result.add((NGSILDOperationResult) resultList);
				}
			});
			return result;
		});
	}

	private boolean isDifferentRemoteQueryAvailable(BatchRequest request,
			Map<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> remoteHost2Batch) {
		if (!remoteHost2Batch.isEmpty()) {
			return true;
		}

		for (Entry<String, List<Map<String, Object>>> entry : request.getPayload().entrySet()) {
			for (Map<String, Object> map : entry.getValue()) {
				if (isRemoteQueryPossible(map, request.getTenant(), (String) map.get(NGSIConstants.JSON_LD_ID))) {
					return true;
				}
			}
		}
		return false;
	}

	public Uni<List<NGSILDOperationResult>> upsertBatch(String tenant, List<Map<String, Object>> expandedEntities,
			List<Context> contexts, boolean localOnly, boolean doReplace, io.vertx.core.MultiMap headersFromReq,
			ViaHeaders viaHeaders) {
		Iterator<Map<String, Object>> itEntities = expandedEntities.iterator();
		Iterator<Context> itContext = contexts.iterator();
		Map<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> remoteHost2Batch = Maps.newHashMap();
		Map<String, List<Map<String, Object>>> localEntities = Maps.newHashMap();
		while (itEntities.hasNext() && itContext.hasNext()) {
			Map<String, Object> entity = itEntities.next();
			String entityId = (String) entity.get(NGSIConstants.JSON_LD_ID);
			Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> split = splitEntity(
					new UpsertEntityRequest(tenant, entity, zip), entityId);
			Map<String, Object> local = split.getItem1();
			Context context = itContext.next();
			if (local != null) {
				MicroServiceUtils.putIntoIdMap(localEntities, entityId, local);
			} else {
				itContext.remove();
			}
			Collection<Tuple2<RemoteHost, Map<String, Object>>> remotes = split.getItem2();
			for (Tuple2<RemoteHost, Map<String, Object>> remote : remotes) {
				List<Tuple2<Context, Map<String, Object>>> entities2Context;
				if (remoteHost2Batch.containsKey(remote.getItem1())) {
					entities2Context = remoteHost2Batch.get(remote.getItem1());
				} else {
					entities2Context = Lists.newArrayList();
					remoteHost2Batch.put(remote.getItem1(), entities2Context);
				}
				entities2Context.add(Tuple2.of(context, remote.getItem2()));
			}
		}

		BatchRequest request = new BatchRequest(tenant, localEntities.keySet(), localEntities,
				AppConstants.BATCH_UPSERT_REQUEST, zip);
		Uni<List<NGSILDOperationResult>> local = entityDAO.batchUpsertEntity(request, doReplace).onItem()
				.transformToUni(dbResult -> {
					List<NGSILDOperationResult> result = Lists.newArrayList();
					List<Map<String, Object>> successes = (List<Map<String, Object>>) dbResult.get("success");
					List<Map<String, String>> fails = (List<Map<String, String>>) dbResult.get("failure");

					Map<String, List<Map<String, Object>>> olds = Maps.newHashMap();

					for (Map<String, Object> entityResult : successes) {
						String entityId = (String) entityResult.get("id");
						boolean updated = (boolean) entityResult.get("updated");
						Map<String, Object> old = (Map<String, Object>) entityResult.get("old");

						MicroServiceUtils.putIntoIdMap(olds, entityId, old);

						NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.UPSERT_REQUEST,
								entityId, tenant);
						opResult.setWasUpdated(updated);
						opResult.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
						result.add(opResult);
					}

					request.setPrevPayload(olds);
					for (Map<String, String> fail : fails) {
						fail.entrySet().forEach(entry -> {
							String entityId = entry.getKey();
							String sqlstate = entry.getValue();
							request.getPayload().remove(entityId);
							request.getPrevPayload().remove(entityId);
							NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.UPSERT_REQUEST,
									entityId, tenant);
							opResult.addFailure(new ResponseException(ErrorType.InvalidRequest, sqlstate));
							result.add(opResult);
						});

					}
					if (!request.getPayload().isEmpty()) {
						// logger.debug("Upsert batch request sending to kafka " + request.getIds());
						try {
							microServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter,
									objectMapper);
						} catch (ResponseException e) {
							return Uni.createFrom().failure(e);
						}
					}
					return Uni.createFrom().item(result);
				});
		if (localOnly) {
			return local;
		}
		List<Uni<List<NGSILDOperationResult>>> unis = new ArrayList<>();

		for (Entry<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> entry : remoteHost2Batch.entrySet()) {
			RemoteHost remoteHost = entry.getKey();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);

			List<Tuple2<Context, Map<String, Object>>> tuples = entry.getValue();
			List<Uni<Map<String, Object>>> compactedUnis = Lists.newArrayList();
			for (Tuple2<Context, Map<String, Object>> tuple : tuples) {
				Map<String, Object> expanded = tuple.getItem2();
				Context context = tuple.getItem1();
				compactedUnis.add(jsonLdService.compact(expanded, null, context, AppConstants.opts, -1));
			}
			if (remoteHost.canDoBatchOp()) {
				unis.add(Uni.combine().all().unis(compactedUnis).with(list -> {
					List<Map<String, Object>> toSend = Lists.newArrayList();
					for (Object obj : list) {
						toSend.add((Map<String, Object>) obj);
					}
					return toSend;
				}).onItem().transformToUni(toSend -> {
					String body;
					try {
						body = JsonUtils.toString(toSend);
					} catch (IOException e) {
						return Uni.createFrom().item(Lists.newArrayList());
					}

					return HttpUtils
							.connect(webClient,
									remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_UPSERT,
									tenant, AppConstants.POST_OP, AppConstants.NGB_APPLICATION_JSON, null,
									toFrwd, body, viaHeaders,
									remoteHost.cSourceAlias(), -1)
							.onItemOrFailure().transform((response, failure) -> {
								return handleBatchResponse(response, failure, remoteHost, toSend,
										ArrayUtils.toArray(204));
							});
				}));
			} else {
				List<Uni<NGSILDOperationResult>> singleUnis = new ArrayList<>();
				for (Uni<Map<String, Object>> compactedUni : compactedUnis) {
					singleUnis.add(compactedUni.onItem().transformToUni(entity -> {
						String body;
						try {
							body = JsonUtils.toString(entity);
						} catch (IOException e) {
							return Uni.createFrom().item(new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
									"unknown", remoteHost.tenant()));
						}

						return HttpUtils
								.connect(webClient,
										remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/"
												+ entity.get(NGSIConstants.JSON_LD_ID) + "/"
												+ NGSIConstants.QUERY_PARAMETER_ATTRS,
										tenant, AppConstants.POST_OP, AppConstants.NGB_APPLICATION_JSON, null,
										toFrwd, body, viaHeaders,
										remoteHost.cSourceAlias(), -1)
								.onItemOrFailure()
								.transformToUni((response, failure) -> {
									if (response.statusCode() == 404) {
										return HttpUtils
												.connect(webClient,
														remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT,
														tenant, AppConstants.POST_OP, AppConstants.NGB_APPLICATION_JSON,
														null,
														toFrwd, body, viaHeaders,
														remoteHost.cSourceAlias(), -1)
												.onItemOrFailure().transform((response1, failure1) -> {
													return HttpUtils.handleWebResponse(response1, failure1,
															ArrayUtils.toArray(201), remoteHost,
															AppConstants.CREATE_REQUEST,
															(String) entity.get(NGSIConstants.JSON_LD_ID),
															HttpUtils.getAttribsFromCompactedPayload(entity));

												});
									}
									return Uni.createFrom()
											.item(HttpUtils.handleWebResponse(response, failure,
													ArrayUtils.toArray(201), remoteHost, AppConstants.APPEND_REQUEST,
													(String) entity.get(NGSIConstants.JSON_LD_ID),
													HttpUtils.getAttribsFromCompactedPayload(entity)));
								});
					}));
				}
				unis.add(Uni.combine().all().unis(singleUnis).with(list -> {
					List<NGSILDOperationResult> result = Lists.newArrayList();
					list.forEach(obj -> result.add((NGSILDOperationResult) obj));
					return result;
				}));
			}
		}
		if (unis.isEmpty()) {
			return local;
		}
		unis.add(0, local);
		return Uni.combine().all().unis(unis).with(resultLists -> {
			List<NGSILDOperationResult> result = Lists.newArrayList();
			resultLists.forEach(resultList -> {
				// A combined uni result may be a List, any other Collection (e.g. a HashSet from a
				// forwarded op), a single NGSILDOperationResult, or null (a forward that yielded nothing).
				// The old blind (List) cast + addAll threw ClassCastException / NPE once distributed
				// forwarding was in play, turning successful distributed batch ops into 500s.
				if (resultList == null) {
					return;
				}
				if (resultList instanceof java.util.Collection) {
					result.addAll((java.util.Collection<NGSILDOperationResult>) resultList);
				} else if (resultList instanceof NGSILDOperationResult) {
					result.add((NGSILDOperationResult) resultList);
				}
			});
			return result;
		});
	}

	public Uni<List<NGSILDOperationResult>> deleteBatch(String tenant, List<String> entityIds, boolean localOnly,
			io.vertx.core.MultiMap headersFromReq, ViaHeaders viaHeaders) {
		Map<RemoteHost, List<String>> host2Ids = Maps.newHashMap();
		for (String entityId : entityIds) {
			DeleteEntityRequest request = new DeleteEntityRequest(tenant, entityId, zip);
			Set<RemoteHost> remoteHosts = getRemoteHostsForDelete(request, entityId);
			for (RemoteHost remoteHost : remoteHosts) {
				if (host2Ids.containsKey(remoteHost)) {
					host2Ids.get(remoteHost).add(entityId);
				} else {
					host2Ids.put(remoteHost, Lists.newArrayList(entityId));
				}
			}
		}
		List<Uni<List<NGSILDOperationResult>>> unis = new ArrayList<>(host2Ids.keySet().size());
		for (Entry<RemoteHost, List<String>> entry : host2Ids.entrySet()) {
			RemoteHost remoteHost = entry.getKey();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);
			List<String> toSend = entry.getValue();
			if (remoteHost.canDoBatchOp()) {
				String body;
				try {
					body = JsonUtils.toString(toSend);
				} catch (IOException e) {
					return Uni.createFrom().item(Lists.newArrayList());
				}

				unis.add(HttpUtils
						.connect(webClient,
								remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_DELETE,
								tenant, AppConstants.POST_OP, AppConstants.NGB_APPLICATION_JSON, null,
								toFrwd, body, viaHeaders,
								remoteHost.cSourceAlias(), -1)
						.onItemOrFailure().transform((response, failure) -> {
							return handleBatchDeleteResponse(response, failure, remoteHost, toSend,
									ArrayUtils.toArray(204));
						}));
			} else {
				List<Uni<NGSILDOperationResult>> singleUnis = new ArrayList<>();
				for (String entityId : toSend) {
					String url = remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId;

					singleUnis.add(HttpUtils
							.connect(webClient,
									url,
									tenant, AppConstants.DELETE_OP, null, null,
									toFrwd, null, viaHeaders,
									remoteHost.cSourceAlias(), -1)
							.onItemOrFailure().transform((response, failure) -> {
								return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
										remoteHost, AppConstants.CREATE_REQUEST, entityId, Sets.newHashSet());

							}));
				}
				unis.add(Uni.combine().all().unis(singleUnis).with(list -> {
					List<NGSILDOperationResult> result = Lists.newArrayList();
					list.forEach(obj -> result.add((NGSILDOperationResult) obj));
					return result;
				}));
			}
		}

		Uni<List<NGSILDOperationResult>> local = entityDAO.batchDeleteEntity(tenant, entityIds).onItem()
				.transformToUni(dbResult -> {
					List<NGSILDOperationResult> result = Lists.newArrayList();
					List<Map<String, Object>> successes = (List<Map<String, Object>>) dbResult.get("success");
					List<Map<String, String>> fails = (List<Map<String, String>>) dbResult.get("failure");
					Set<String> successEntityIds = Sets.newHashSet();
					Map<String, List<Map<String, Object>>> deleted = Maps.newHashMap();
					for (Map<String, Object> entry : successes) {
						String entityId = (String) entry.get("id");
						successEntityIds.add(entityId);
						MicroServiceUtils.putIntoIdMap(deleted, entityId, (Map<String, Object>) entry.get("old"));
						NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.DELETE_REQUEST,
								entityId, tenant);
						opResult.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
						result.add(opResult);
					}
					for (Map<String, String> fail : fails) {
						fail.entrySet().forEach(entry -> {
							String entityId = entry.getKey();
							String sqlstate = entry.getValue();
							NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.DELETE_REQUEST,
									entityId, tenant);
							opResult.addFailure(new ResponseException(ErrorType.NotFound, sqlstate));
							result.add(opResult);
						});

					}
					BatchRequest request = new BatchRequest(tenant, successEntityIds, null,
							AppConstants.BATCH_DELETE_REQUEST, zip);
					request.setPrevPayload(deleted);
					if (!request.getIds().isEmpty()) {
						logger.debug("Delete batch request sending to kafka " + request.getIds());
						try {
							microServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter,
									objectMapper);
						} catch (ResponseException e) {
							return Uni.createFrom().failure(e);
						}
					}
					return Uni.createFrom().item(result);
				});

		unis.add(0, local);
		return Uni.combine().all().unis(unis).with(resultLists -> {
			List<NGSILDOperationResult> result = Lists.newArrayList();
			resultLists.forEach(resultList -> {
				// A combined uni result may be a List, any other Collection (e.g. a HashSet from a
				// forwarded op), a single NGSILDOperationResult, or null (a forward that yielded nothing).
				// The old blind (List) cast + addAll threw ClassCastException / NPE once distributed
				// forwarding was in play, turning successful distributed batch ops into 500s.
				if (resultList == null) {
					return;
				}
				if (resultList instanceof java.util.Collection) {
					result.addAll((java.util.Collection<NGSILDOperationResult>) resultList);
				} else if (resultList instanceof NGSILDOperationResult) {
					result.add((NGSILDOperationResult) resultList);
				}
			});
			return result;
		});
	}

	public Uni<NGSILDOperationResult> mergePatch(String tenant, String entityId, Map<String, Object> resolved,
			Context context, io.vertx.core.MultiMap headersFromReq, ViaHeaders viaHeaders) {
		logger.debug("createMessage() :: started");
		MergePatchRequest request = new MergePatchRequest(tenant, entityId, resolved, zip);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> localAndRemote = splitEntity(
				request, entityId);
		Map<String, Object> localEntity = localAndRemote.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = localAndRemote.getItem2();
		// if (remoteEntitiesAndHosts.isEmpty()) {
		// request.setPayload(localEntity);
		// return localMergePatch(request, context);
		// }
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);

			if (remoteHost.canDoSingleOp()) {
				unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
					String body;
					try {
						body = JsonUtils.toString(compacted);
					} catch (IOException e) {
						return Uni.createFrom().item(new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
								entityId, remoteHost.tenant()));
					}

					return HttpUtils
							.connect(webClient,
									remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId,
									tenant, AppConstants.PATCH_OP, AppConstants.NGB_APPLICATION_JSON, null,
									toFrwd, body, viaHeaders,
									remoteHost.cSourceAlias(), -1)
							.onItemOrFailure()
							.transform((response, failure) -> {
								return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
										remoteHost, AppConstants.CREATE_REQUEST, entityId,
										HttpUtils.getAttribsFromCompactedPayload(compacted));
							});
				}));
			} else {
				unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
					String body;
					try {
						body = JsonUtils.toString(List.of(compacted));
					} catch (IOException e) {
						return Uni.createFrom().item(new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
								entityId, remoteHost.tenant()));
					}

					return HttpUtils
							.connect(webClient,
									remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_CREATE,
									tenant, AppConstants.PATCH_OP, AppConstants.NGB_APPLICATION_JSON, null,
									toFrwd, body, viaHeaders,
									remoteHost.cSourceAlias(), -1)
							.onItemOrFailure()
							.transform((response, failure) -> {
								return handleBatchResponse(response, failure, remoteHost, Lists.newArrayList(compacted),
										ArrayUtils.toArray(201)).get(0);
							});
				}));
			}

		}
		if (localEntity != null && !localEntity.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteEntitiesAndHosts, entityId)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			request.setPayloadFromSingle(entityId, localEntity);
			unis.add(localMergePatch(request, entityId, context).onFailure().recoverWithItem(e -> {
				NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, entityId,
						tenant);
				if (e instanceof ResponseException) {
					localResult.addFailure((ResponseException) e);
				} else {
					localResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

				return localResult;

			}));
		}
		return Uni.combine().all().unis(unis).with(list -> {
			return getResult(list);
		});
	}

	private Uni<NGSILDOperationResult> localMergePatch(MergePatchRequest request, String entityId, Context context) {
		return entityDAO.mergePatch(request).onItem().transformToUni(result -> {
			List<ResponseException> collectedFails = handleMergePatchDBResult(result, request.getTenant(), entityId);
			if (!collectedFails.isEmpty()) {
				collectedFails.forEach(e -> {
					logger.error("Failed to send on of the messages", e);
				});
				return Uni.createFrom().failure(collectedFails.get(0));
			}
			NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.MERGE_PATCH_REQUEST, entityId,
					request.getTenant());
			localResult
					.addSuccess(new CRUDSuccess(null, null, null, request.getPayload().get(entityId).get(0), context));
			return Uni.createFrom().item(localResult);
		});
	}

	private List<ResponseException> handleMergePatchDBResult(Map<String, Object> result, String tenant,
			String entityId) {
		Map<String, List<String>> updated = (Map<String, List<String>>) result.get("updated");
		Map<String, List<String>> deleted = (Map<String, List<String>>) result.get("deleted");
		Map<String, Object> prev = (Map<String, Object>) result.get("old");
		Map<String, Object> updatedEntity = (Map<String, Object>) result.get("new");

		List<ResponseException> collectedFails = Lists.newArrayList();
		if (!deleted.isEmpty()) {
			for (Entry<String, List<String>> attrEntry : deleted.entrySet()) {
				String attr = attrEntry.getKey();
				DeleteAttributeRequest deleteReq;
				if (attrEntry.getValue().contains("@all")) {
					deleteReq = new DeleteAttributeRequest(tenant, entityId, attr, null, true, zip);
					deleteReq.setPrevPayloadFromSingle(entityId, prev);
					try {
						microServiceUtils.serializeAndSplitObjectAndEmit(deleteReq, messageSize, entityEmitter,
								objectMapper);
					} catch (ResponseException e) {
						collectedFails.add(e);
					}
				} else {
					for (String datasetId : attrEntry.getValue()) {
						deleteReq = new DeleteAttributeRequest(tenant, entityId, attr, datasetId, false, zip);
						deleteReq.setPrevPayloadFromSingle(entityId, prev);
						try {
							microServiceUtils.serializeAndSplitObjectAndEmit(deleteReq, messageSize, entityEmitter,
									objectMapper);
						} catch (ResponseException e) {
							collectedFails.add(e);
						}
					}
				}
			}
		}
		if (!updated.isEmpty()) {
			Object type = updatedEntity.get(NGSIConstants.JSON_LD_TYPE);
			Object createdAt = updatedEntity.get(NGSIConstants.NGSI_LD_CREATED_AT);
			Object modifiedAt = updatedEntity.get(NGSIConstants.NGSI_LD_MODIFIED_AT);
			Map<String, Object> tmp = Maps.newHashMap();

			tmp.put(NGSIConstants.JSON_LD_TYPE, type);
			tmp.put(NGSIConstants.NGSI_LD_CREATED_AT, createdAt);
			tmp.put(NGSIConstants.NGSI_LD_MODIFIED_AT, modifiedAt);
			for (Entry<String, List<String>> attrEntry : updated.entrySet()) {
				String attr = attrEntry.getKey();

				List<Map<String, Object>> attrib = (List<Map<String, Object>>) updatedEntity.get(attr);
				for (String datasetId : attrEntry.getValue()) {
					Map<String, Object> searchedInstance = null;
					for (Map<String, Object> attrInstance : attrib) {
						Object datasetIdObj = attrInstance.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
						if ((datasetId == null && datasetIdObj == null) || (datasetId != null && datasetIdObj != null
								&& datasetId.equals(((List<Map<String, String>>) datasetIdObj).get(0)
										.get(NGSIConstants.JSON_LD_ID)))) {
							searchedInstance = attrInstance;
							break;
						}
					}
					if (searchedInstance != null) {
						Object attrList = tmp.get(attr);
						if (attrList == null) {
							attrList = Lists.newArrayList();
							tmp.put(attr, attrList);
						}
						((List) attrList).add(searchedInstance);
					}
				}
			}
			AppendEntityRequest append = new AppendEntityRequest(tenant, entityId, tmp, zip);
			append.setPrevPayloadFromSingle(entityId, prev);
			try {
				microServiceUtils.serializeAndSplitObjectAndEmit(append, messageSize, entityEmitter, objectMapper);
			} catch (ResponseException e) {
				collectedFails.add(e);
			}
		}
		return collectedFails;
	}

	public Uni<NGSILDOperationResult> replaceEntity(String tenant, String entityId, Map<String, Object> resolved,
			Context context, io.vertx.core.MultiMap headersFromReq, ViaHeaders viaHeaders) {
		logger.debug("ReplaceMessage() :: started");

		ReplaceEntityRequest request = new ReplaceEntityRequest(tenant, resolved, zip);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> localAndRemote = splitEntity(
				request, entityId);
		Map<String, Object> localEntity = localAndRemote.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = localAndRemote.getItem2();
		// if (remoteEntitiesAndHosts.isEmpty()) {
		// request.setPayload(localEntity);
		// return replaceLocalEntity(request, context);
		// }
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);
			if (remoteHost.canDoSingleOp()) {
				unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
					String body;
					try {
						body = JsonUtils.toString(compacted);
					} catch (IOException e) {
						return Uni.createFrom().item(new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
								entityId, remoteHost.tenant()));
					}

					return HttpUtils
							.connect(webClient,
									remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId,
									tenant, AppConstants.PATCH_OP, AppConstants.NGB_APPLICATION_JSON, null,
									toFrwd, body, viaHeaders,
									remoteHost.cSourceAlias(), -1)
							.onItemOrFailure()
							.transform((response, failure) -> {
								return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
										remoteHost, AppConstants.CREATE_REQUEST, entityId,
										HttpUtils.getAttribsFromCompactedPayload(compacted));
							});
				}));
			}

		}
		if (localEntity != null && !localEntity.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteEntitiesAndHosts, entityId)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}

			request.setPayloadFromSingle(entityId, localEntity);
			unis.add(replaceLocalEntity(request, entityId, context).onFailure().recoverWithItem(e -> {
				NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, entityId,
						tenant);
				if (e instanceof ResponseException) {
					localResult.addFailure((ResponseException) e);
				} else {
					localResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

				return localResult;

			}));
		}
		return Uni.combine().all().unis(unis).with(list -> {
			return getResult(list);
		});
	}

	private Uni<NGSILDOperationResult> replaceLocalEntity(ReplaceEntityRequest request, String entityId,
			Context context) {
		return entityDAO.replaceEntity(request).onItem().transformToUni(v -> {
			request.setPrevPayloadFromSingle(entityId, v);
			try {
				microServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.REPLACE_ENTITY_REQUEST,
					entityId, request.getTenant());
			localResult
					.addSuccess(new CRUDSuccess(null, null, null, request.getPayload().get(entityId).get(0), context));
			return Uni.createFrom().item(localResult);
		});
	}

	public Uni<NGSILDOperationResult> replaceAttribute(String tenant, Map<String, Object> resolved, Context context,
			String entityId, String attrId, String datasetId, io.vertx.core.MultiMap headersFromReq, ViaHeaders viaHeaders) {
		logger.debug("ReplaceMessage() :: started");
		// NGSI-LD 5.6.19: when no datasetId query param is given but the replacement fragment is a
		// single instance carrying a datasetId, replace that specific instance (preserving the other
		// instances), rather than the default one.
		if (datasetId == null) {
			Object attrVal = resolved.get(attrId);
			Map<String, Object> instance = null;
			if (attrVal instanceof List<?> l && l.size() == 1 && l.get(0) instanceof Map<?, ?> m0) {
				instance = (Map<String, Object>) m0;
			} else if (attrVal instanceof Map<?, ?> m) {
				instance = (Map<String, Object>) m;
			}
			if (instance != null) {
				Object dsId = instance.get(NGSIConstants.NGSI_LD_DATA_SET_ID);
				Map<String, Object> dsMap = null;
				if (dsId instanceof List<?> dsList && !dsList.isEmpty() && dsList.get(0) instanceof Map) {
					dsMap = (Map<String, Object>) dsList.get(0);
				} else if (dsId instanceof Map) {
					dsMap = (Map<String, Object>) dsId;
				}
				if (dsMap != null) {
					Object id = dsMap.get(NGSIConstants.JSON_LD_ID);
					if (id != null) {
						datasetId = id.toString();
					}
				}
			}
		}
		ReplaceAttribRequest request = new ReplaceAttribRequest(tenant, resolved, entityId, attrId, datasetId, zip);
		Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> localAndRemote = splitEntity(
				request, entityId);
		Map<String, Object> localEntity = localAndRemote.getItem1();
		Collection<Tuple2<RemoteHost, Map<String, Object>>> remoteEntitiesAndHosts = localAndRemote.getItem2();
		if (localEntity != null) {
			localEntity.remove(NGSIConstants.JSON_LD_TYPE);
		}
		List<Uni<NGSILDOperationResult>> unis = new ArrayList<>(remoteEntitiesAndHosts.size());
		// if (remoteEntitiesAndHosts.isEmpty()) {
		// request.setPayload(localEntity);
		// return replaceLocalAttrib(request, context);
		// }
		for (Tuple2<RemoteHost, Map<String, Object>> remoteEntityAndHost : remoteEntitiesAndHosts) {
			Map<String, Object> expanded = remoteEntityAndHost.getItem2();
			RemoteHost remoteHost = remoteEntityAndHost.getItem1();
			MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);
			if (remoteHost.canDoSingleOp()) {
				unis.add(prepareSplitUpEntityForSending(expanded, context).onItem().transformToUni(compacted -> {
					String body;
					try {
						body = JsonUtils.toString(compacted);
					} catch (IOException e) {
						return Uni.createFrom().item(new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
								entityId, remoteHost.tenant()));
					}

					return HttpUtils
							.connect(webClient,
									remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/" + entityId + "/"
											+ "attrs" + "/" + context.compactIri(attrId),
									tenant, AppConstants.PUT_OP, AppConstants.NGB_APPLICATION_JSON, null,
									toFrwd, body, viaHeaders,
									remoteHost.cSourceAlias(), -1)
							.onItemOrFailure().transform((response, failure) -> {
								return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
										remoteHost, AppConstants.CREATE_REQUEST, entityId,
										HttpUtils.getAttribsFromCompactedPayload(compacted));
							});
				}));

			}
		}
		if (localEntity != null && !localEntity.isEmpty()) {
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteEntitiesAndHosts, entityId)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			request.setPayloadFromSingle(entityId, localEntity);
			unis.add(replaceLocalAttrib(request, entityId, context).onFailure().recoverWithItem(e -> {
				NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.CREATE_REQUEST, entityId,
						tenant);
				if (e instanceof ResponseException) {
					localResult.addFailure((ResponseException) e);
				} else {
					localResult.addFailure(new ResponseException(ErrorType.InternalError, e.getMessage()));
				}

				return localResult;

			}));
		}
		return Uni.combine().all().unis(unis).with(list -> {
			return getResult(list);
		});
	}

	private Uni<NGSILDOperationResult> replaceLocalAttrib(ReplaceAttribRequest request, String entityId,
			Context context) {
		return entityDAO.replaceAttrib(request).onItem().transformToUni(v -> {
			request.setPrevPayloadFromSingle(entityId, v);
			try {
				microServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize, entityEmitter, objectMapper);
			} catch (ResponseException e) {
				return Uni.createFrom().failure(e);
			}
			NGSILDOperationResult localResult = new NGSILDOperationResult(AppConstants.REPLACE_ENTITY_REQUEST,
					entityId, request.getTenant());
			localResult
					.addSuccess(new CRUDSuccess(null, null, null, request.getPayload().get(entityId).get(0), context));
			return Uni.createFrom().item(localResult);
		});
	}

	private boolean isRemoteQueryPossible(Map<String, Object> payload, String tenant, String id) {

		Iterator<List<RegistrationEntry>> it = tenant2CId2QueryRegEntries.row(tenant).values().iterator();
		// ids, types, attrs, geo, scope

		List<String> types = null;
		if (payload.containsKey(NGSIConstants.JSON_LD_TYPE)) {
			types = (List<String>) payload.get(NGSIConstants.JSON_LD_TYPE);
		}

		while (it.hasNext()) {
			Iterator<RegistrationEntry> tenantRegs = it.next().iterator();
			while (tenantRegs.hasNext()) {

				RegistrationEntry regEntry = tenantRegs.next();
				if (regEntry.expiresAt() > 0 && regEntry.expiresAt() <= System.currentTimeMillis()) {
					it.remove();
					continue;
				}
				if ((((regEntry.eId() != null && regEntry.eId().equals(id))
						|| (regEntry.eIdp() != null && regEntry.eIdp().matches(id))
						|| (regEntry.eIdp() == null && regEntry.eId() == null)))
						&& (types == null || types.contains(regEntry.type()))) {
					return true;
				}
			}
		}
		return false;
	}

	public Uni<List<NGSILDOperationResult>> mergeBatch(String tenant, List<Map<String, Object>> expandedEntities,
			List<Context> contexts, boolean localOnly, boolean noOverWrite, io.vertx.core.MultiMap headersFromReq,
			ViaHeaders viaHeaders) {

		Iterator<Map<String, Object>> itEntities = expandedEntities.iterator();
		Iterator<Context> itContext = contexts.iterator();
		Map<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> remoteHost2Batch = Maps.newHashMap();
		Map<String, List<Map<String, Object>>> localEntities = Maps.newHashMap();
		while (itEntities.hasNext() && itContext.hasNext()) {
			Map<String, Object> entity = itEntities.next();
			String entityId = (String) entity.get(NGSIConstants.JSON_LD_ID);
			Tuple2<Map<String, Object>, Collection<Tuple2<RemoteHost, Map<String, Object>>>> split = splitEntity(
					new AppendEntityRequest(tenant, entityId, entity, zip), entityId);
			Map<String, Object> local = split.getItem1();
			Context context = itContext.next();
			if (local != null) {
				local.remove(NGSIConstants.NGSI_LD_CREATED_AT);
				MicroServiceUtils.putIntoIdMap(localEntities, entityId, local);
			} else {
				itContext.remove();
			}
			Collection<Tuple2<RemoteHost, Map<String, Object>>> remotes = split.getItem2();
			for (Tuple2<RemoteHost, Map<String, Object>> remote : remotes) {
				List<Tuple2<Context, Map<String, Object>>> entities2Context;
				if (remoteHost2Batch.containsKey(remote.getItem1())) {
					entities2Context = remoteHost2Batch.get(remote.getItem1());
				} else {
					entities2Context = Lists.newArrayList();
					remoteHost2Batch.put(remote.getItem1(), entities2Context);
				}
				entities2Context.add(Tuple2.of(context, remote.getItem2()));
			}
		}

		List<Uni<List<NGSILDOperationResult>>> unis = new ArrayList<>();
		if (!localOnly) {
			for (Entry<RemoteHost, List<Tuple2<Context, Map<String, Object>>>> entry : remoteHost2Batch.entrySet()) {
				RemoteHost remoteHost = entry.getKey();
				MultiMap toFrwd = HttpUtils.getHeadToFrwd(remoteHost.headers(), headersFromReq);
				List<Tuple2<Context, Map<String, Object>>> tuples = entry.getValue();
				List<Uni<Map<String, Object>>> compactedUnis = Lists.newArrayList();
				for (Tuple2<Context, Map<String, Object>> tuple : tuples) {
					Map<String, Object> expanded = tuple.getItem2();
					Context context = tuple.getItem1();
					compactedUnis.add(jsonLdService.compact(expanded, null, context, AppConstants.opts, -1));
				}
				if (remoteHost.canDoBatchOp()) {
					unis.add(Uni.combine().all().unis(compactedUnis).with(list -> {
						List<Map<String, Object>> toSend = Lists.newArrayList();
						for (Object obj : list) {
							toSend.add((Map<String, Object>) obj);
						}
						return toSend;
					}).onItem().transformToUni(toSend -> {
						String body;
						try {
							body = JsonUtils.toString(toSend);
						} catch (IOException e) {
							return Uni.createFrom().item(Lists.newArrayList());
						}

						return HttpUtils
								.connect(webClient,
										remoteHost.host() + NGSIConstants.ENDPOINT_BATCH_MERGE,
										tenant, AppConstants.PATCH_OP, AppConstants.NGB_APPLICATION_JSON, null,
										toFrwd, body, viaHeaders,
										remoteHost.cSourceAlias(), -1)
								.onItemOrFailure()
								.transform((response, failure) -> {
									return handleBatchResponse(response, failure, remoteHost, toSend,
											ArrayUtils.toArray(204));
								});
					}));
				} else {
					List<Uni<NGSILDOperationResult>> singleUnis = new ArrayList<>();
					for (Uni<Map<String, Object>> compactedUni : compactedUnis) {
						singleUnis.add(compactedUni.onItem().transformToUni(entity -> {
							String body;
							try {
								body = JsonUtils.toString(entity);
							} catch (IOException e) {
								return Uni.createFrom().item(new NGSILDOperationResult(AppConstants.APPEND_REQUEST,
										"unknown", remoteHost.tenant()));
							}

							return HttpUtils
									.connect(webClient,
											remoteHost.host() + NGSIConstants.NGSI_LD_ENTITIES_ENDPOINT + "/"
													+ entity.get(NGSIConstants.JSON_LD_ID) + "/"
													+ NGSIConstants.QUERY_PARAMETER_ATTRS,
											tenant, AppConstants.PATCH_OP, AppConstants.NGB_APPLICATION_JSON, null,
											toFrwd, body, viaHeaders,
											remoteHost.cSourceAlias(), -1)
									.onItemOrFailure()
									.transform((response, failure) -> {
										return HttpUtils.handleWebResponse(response, failure, ArrayUtils.toArray(201),
												remoteHost, AppConstants.MERGE_PATCH_REQUEST,
												(String) entity.get(NGSIConstants.JSON_LD_ID),
												HttpUtils.getAttribsFromCompactedPayload(entity));
									});
						}));
					}
					unis.add(Uni.combine().all().unis(singleUnis).with(list -> {
						List<NGSILDOperationResult> result = Lists.newArrayList();
						list.forEach(obj -> result.add((NGSILDOperationResult) obj));
						return result;
					}));
				}
			}
		}
		if (!localEntities.isEmpty()) {
			BatchRequest request = new BatchRequest(tenant, localEntities.keySet(), localEntities,
					AppConstants.BATCH_MERGE_REQUEST, zip);
			request.setNoOverwrite(noOverWrite);
			if (!unis.isEmpty() && isDifferentRemoteQueryAvailable(request, remoteHost2Batch)) {
				request.setDistributed(true);
			} else {
				request.setDistributed(false);
			}
			Uni<List<NGSILDOperationResult>> local = entityDAO.mergeBatchEntity(request).onItem()
					.transformToUni(dbResult -> {
						List<NGSILDOperationResult> result = Lists.newArrayList();
						List<Map<String, Object>> successes = (List<Map<String, Object>>) dbResult.get("success");
						List<Map<String, String>> fails = (List<Map<String, String>>) dbResult.get("failure");
						Map<String, List<Map<String, Object>>> oldEntities = Maps.newHashMap();

						for (Map<String, Object> success : successes) {
							String entityId = (String) success.get("id");
							Map<String, Object> old = (Map<String, Object>) success.get("old");
							handleMergePatchDBResult(Map.of("old", old, "new", success.get("new"), "deleted",
									success.get("deleted"), "updated", success.get("updated")), tenant, entityId);
							MicroServiceUtils.putIntoIdMap(oldEntities, entityId, old);
							NGSILDOperationResult opResult = new NGSILDOperationResult(AppConstants.MERGE_PATCH_REQUEST,
									entityId, tenant);
							opResult.addSuccess(new CRUDSuccess(null, null, null, Sets.newHashSet()));
							result.add(opResult);
						}
						request.setPrevPayload(oldEntities);
						for (Map<String, String> fail : fails) {
							fail.entrySet().forEach(entry -> {
								String entityId = entry.getKey();
								String sqlstate = entry.getValue();
								request.getPayload().remove(entityId);
								NGSILDOperationResult opResult = new NGSILDOperationResult(
										AppConstants.MERGE_PATCH_REQUEST, entityId, tenant);
								if (sqlstate.equals(AppConstants.SQL_NOT_FOUND)) {
									opResult.addFailure(new ResponseException(ErrorType.NotFound, entityId));
								} else {
									opResult.addFailure(new ResponseException(ErrorType.InvalidRequest, sqlstate));
								}
								result.add(opResult);
							});

						}
						// if (!request.getPayload().isEmpty()) {
						//
						// try {
						// microServiceUtils.serializeAndSplitObjectAndEmit(request, messageSize,
						// entityEmitter,
						// objectMapper);
						// } catch (ResponseException e) {
						// return Uni.createFrom().failure(e);
						// }
						// }
						return Uni.createFrom().item(result);
					});

			unis.add(0, local);
		}
		if (unis.isEmpty()) {
			return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
		}
		return Uni.combine().all().unis(unis).with(resultLists -> {
			List<NGSILDOperationResult> result = Lists.newArrayList();
			resultLists.forEach(resultList -> {
				// A combined uni result may be a List, any other Collection (e.g. a HashSet from a
				// forwarded op), a single NGSILDOperationResult, or null (a forward that yielded nothing).
				// The old blind (List) cast + addAll threw ClassCastException / NPE once distributed
				// forwarding was in play, turning successful distributed batch ops into 500s.
				if (resultList == null) {
					return;
				}
				if (resultList instanceof java.util.Collection) {
					result.addAll((java.util.Collection<NGSILDOperationResult>) resultList);
				} else if (resultList instanceof NGSILDOperationResult) {
					result.add((NGSILDOperationResult) resultList);
				}
			});
			return result;
		});
	}

	public Uni<Void> updateValueField(String tenant, String id, String type, String attribId, String datasetId,
			Map<String, Object> expandedValue) {
		return entityDAO.updateValueField(tenant, id, attribId, datasetId, expandedValue);
	}

}
