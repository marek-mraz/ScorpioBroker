package eu.neclab.ngsildbroker.entityhandler.services;

import com.github.jsonldjava.core.JsonLDService;
import com.github.jsonldjava.core.JsonLdConsts;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import eu.neclab.ngsildbroker.commons.constants.AppConstants;
import eu.neclab.ngsildbroker.commons.constants.NGSIConstants;
import eu.neclab.ngsildbroker.commons.datatypes.RegistrationEntry;
import eu.neclab.ngsildbroker.commons.datatypes.requests.AppendEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.BatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.CreateEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteAttributeRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.DeleteEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.MergePatchRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.ReplaceAttribRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.ReplaceEntityRequest;
import eu.neclab.ngsildbroker.commons.datatypes.requests.UpdateEntityRequest;
import eu.neclab.ngsildbroker.commons.enums.ErrorType;
import eu.neclab.ngsildbroker.commons.exceptions.ResponseException;
import eu.neclab.ngsildbroker.commons.storage.ConnectionManager;
import eu.neclab.ngsildbroker.commons.tools.DBUtil;
import eu.neclab.ngsildbroker.commons.tools.EntityTools;
import eu.neclab.ngsildbroker.commons.tools.MicroServiceUtils;
import io.quarkus.logging.Log;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.tuples.Tuple2;
import io.smallrye.mutiny.tuples.Tuple3;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.mutiny.sqlclient.Row;
import io.vertx.mutiny.sqlclient.RowSet;
import io.vertx.mutiny.sqlclient.Tuple;
import io.vertx.pgclient.PgException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.inject.Inject;
import jakarta.enterprise.context.ApplicationScoped;
import io.quarkus.runtime.Startup;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ApplicationScoped
@Startup
public class EntityInfoDAO {

	private static Logger logger = LoggerFactory.getLogger(EntityInfoDAO.class);

	@Inject
	ConnectionManager connectionManager;

	@Inject
	JsonLDService ldService;

	public Uni<Map<String, Object>> batchCreateEntity(BatchRequest request) {

		List<Map<String, Object>> entities = Lists.newArrayList();
		request.getPayload().values().forEach(entityList -> {
			entities.addAll(entityList);
		});
		Tuple3<Boolean, List<Tuple>, List<String>> nullFoundAndTuple = EntityTools
				.removeNGSILDNullToTuplesWithIdSet(entities, true);

		return connectionManager.executeBatchQuery(request.getTenant(),
				"INSERT INTO ENTITY (id, e_types, entity) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING RETURNING id, true;",
				nullFoundAndTuple.getItem2(), true).onItem().transform(rows -> {
					List<String> ids = nullFoundAndTuple.getItem3();
					Map<String, Object> result = new HashMap<>(2);
					ArrayList<String> success = new ArrayList<>();
					ArrayList<Map<String, String>> failure = new ArrayList<>();
					result.put("success", success);
					result.put("failure", failure);
					while (rows != null) {
						rows.forEach(row -> {
							String id = row.getString(0);
							ids.remove(id);
							success.add(id);
						});
						rows = rows.next();
					}
					ids.forEach(id -> {
						failure.add(Map.of(id, AppConstants.SQL_ALREADY_EXISTS));
					});
					return result;
				}).onFailure().recoverWithUni(e -> {
					if (e instanceof PgException pge) {
						logger.error(pge.getDetail());
					}
					logger.error("Failed to store entities in batch create.", e);
					return Uni.createFrom().failure(e);
				});
	}

	public Uni<Map<String, Object>> batchUpsertEntity(BatchRequest request, boolean doReplace) {

		List<Map<String, Object>> entities = Lists.newArrayList();
		request.getPayload().values().forEach(entityList -> {
			entities.add(mergeAllEntities(entityList));
		});
		if (entities.isEmpty()) {
			Map<String, Object> result = new HashMap<>(2);
			result.put("success", new ArrayList<Map<String, Object>>(0));
			result.put("failure", new ArrayList<Map<String, Object>>(0));
			return Uni.createFrom().item(result);
		}
		Tuple2<Boolean, List<Tuple>> nullFoundAndTuple = EntityTools.removeNGSILDNullToTuples(entities, doReplace);
		StringBuilder sql = new StringBuilder(
				"""
						with a as (SELECT ID AS ID, ENTITY AS OLD_ENTITY FROM ENTITY WHERE ID = $1),
						b as(INSERT INTO ENTITY(ID, E_TYPES, ENTITY) VALUES ($1, $2, $3::jsonb) ON CONFLICT (id) DO UPDATE SET e_types = ARRAY(SELECT DISTINCT UNNEST(entity.e_types || EXCLUDED.e_types)),entity =
								""");

		if (doReplace) {
			sql.append("EXCLUDED.entity ");
		} else {
			if (nullFoundAndTuple.getItem1()) {
				sql.append("ngsild_update_entity(entity.entity, $4, true) ");
			} else {
				sql.append("ngsild_update_entity(entity.entity, excluded.entity, true) ");
			}
		}

		sql.append(
				"RETURNING id, entity, (xmax = 0) AS inserted) select b.id, b.inserted, b.entity, a.old_entity from b LEFT JOIN a ON b.id = a.id;");
		// logger.debug(sql.toString());

		return connectionManager
				.executeBatchQuery(request.getTenant(), sql.toString(), nullFoundAndTuple.getItem2(), true).onItem()
				.transform(rows -> {
					Map<String, Object> result = new HashMap<>(2);
					ArrayList<Map<String, Object>> success = new ArrayList<>(rows.size());
					result.put("success", success);
					result.put("failure", new ArrayList<Map<String, Object>>(0));
					while (rows != null) {
						rows.forEach(row -> {

							Map<String, Object> tmp = new HashMap<>(4);
							tmp.put("id", row.getString(0));
							tmp.put("updated", !row.getBoolean(1));
							JsonObject tmpObj = row.getJsonObject(3);
							if (tmpObj != null) {
								tmp.put("old", tmpObj.getMap());
							} else {
								tmp.put("old", null);
							}
							tmp.put("new", row.getJsonObject(2).getMap());
							success.add(tmp);
						});
						rows = rows.next();
					}
					return result;
				});

	}

	private Map<String, Object> mergeAllEntities(List<Map<String, Object>> entityList) {
		if (entityList.size() == 1) {
			return entityList.get(0);
		}
		Map<String, Object> first = new HashMap<>();
		for (int i = 0; i < entityList.size(); i++) {
			first.putAll(entityList.get(i));

		}
		return first;
	}

	// public Uni<Map<String, Object>> batchAppendEntity(BatchRequest request) {
	// return clientManager.getClient(request.getTenant(),
	// true).onItem().transformToUni(client -> {
	// List<Tuple> entities = Lists.newArrayList();
	// request.getPayload().values().forEach(entityList -> {
	// entityList.forEach(entity -> entities.add(Tuple.of(entity)));
	// });
	//
	//
	// return client.preparedQuery(
	// "UPDATE ENTITY (id, e_types, entity) VALUES ($1, $2, $3) ON CONFLICT DO
	// NOTHING RETURNING id, true;")
	// .executeBatch(nullFoundAndTuple.getItem2()).onItem().transform(rows -> {
	// Set<String> ids = nullFoundAndTuple.getItem3();
	// Map<String, Object> result = new HashMap<>(2);
	// ArrayList<String> success = new ArrayList<>();
	// ArrayList<Map<String, String>> failure = new ArrayList<>();
	// result.put("success", success);
	// result.put("failure", failure);
	// while (rows != null) {
	// rows.forEach(row -> {
	// String id = row.getString(0);
	// ids.remove(id);
	// success.add(id);
	// });
	// rows = rows.next();
	// }
	// ids.forEach(id -> {
	// failure.add(Map.of(id, AppConstants.SQL_ALREADY_EXISTS));
	// });
	// return result;
	// }).onFailure().recoverWithUni(e -> {
	// if (e instanceof PgException pge) {
	// logger.error(pge.getDetail());
	// }
	// logger.error("Failed to store entities in batch create.", e);
	// return Uni.createFrom().failure(e);
	// });
	// });
	// }

	public Uni<Map<String, Object>> batchAppendEntity(BatchRequest request) {

		List<Map<String, Object>> entities = Lists.newArrayList();
		request.getPayload().values().forEach(entityList -> {
			entities.addAll(entityList);
		});
		Tuple tuple = Tuple.of(new JsonArray(entities), request.isNoOverwrite());
		return connectionManager
				.executeQuery(request.getTenant(), "SELECT * FROM NGSILD_APPENDBATCH($1, $2)", tuple, false).onItem()
				.transform(rows -> {
					return rows.iterator().next().getJsonObject(0).getMap();
				});

	}

	public Uni<Map<String, Object>> batchDeleteEntity(String tenant, List<String> entityIds) {

		return connectionManager.executeQuery(tenant, "SELECT * FROM NGSILD_DELETEBATCH($1)",
				Tuple.of(new JsonArray(entityIds)), true).onItem().transform(rows -> {
					return rows.iterator().next().getJsonObject(0).getMap();
				});

	}

	@SuppressWarnings("unchecked")
	/**
	 * 
	 * @param request
	 * @return old version of the entity
	 */
	public Uni<Map<String, Object>> partialUpdateAttribute(UpdateEntityRequest request) {

		Object objPayload = request.getFirstPayload().get(request.getAttribName());
		Tuple tuple;
		List<Object> payloads = new ArrayList<>();
		if (objPayload instanceof List<?>) {
			payloads = (List<Object>) objPayload;
		} else {
			payloads.add(objPayload);
		}
		((Map<String, Object>) payloads.get(0)).remove(NGSIConstants.NGSI_LD_CREATED_AT);
		// target instance datasetId (null => default instance); partial update must only
		// touch an EXISTING instance, else 404 (NGSI-LD 5.6.4) instead of appending.
		String targetDatasetId = null;
		Object dsId = ((Map<String, Object>) payloads.get(0)).get(NGSIConstants.NGSI_LD_DATA_SET_ID);
		Map<String, Object> dsMap = null;
		if (dsId instanceof List<?> dsList && !dsList.isEmpty() && dsList.get(0) instanceof Map) {
			dsMap = (Map<String, Object>) dsList.get(0);
		} else if (dsId instanceof Map) {
			dsMap = (Map<String, Object>) dsId;
		}
		if (dsMap != null) {
			Object id = dsMap.get(JsonLdConsts.ID);
			if (id == null) {
				// datasetId in a partial-update fragment can expand to a Property node
				// {@type:Property, hasValue:[{@value:<id>}]} instead of {@id:<id>}
				Object hv = dsMap.get(NGSIConstants.NGSI_LD_HAS_VALUE);
				if (hv instanceof List<?> hvList && !hvList.isEmpty() && hvList.get(0) instanceof Map) {
					id = ((Map<String, Object>) hvList.get(0)).get(JsonLdConsts.VALUE);
				}
			}
			targetDatasetId = id == null ? null : id.toString();
		}
		// NGSI-LD 5.6.4: a partial update must not change an attribute's type. Only relevant when
		// the fragment carries an explicit type (value-only fragments never change the type).
		String fragmentType = null;
		Object ftObj = ((Map<String, Object>) payloads.get(0)).get(NGSIConstants.JSON_LD_TYPE);
		if (ftObj instanceof List<?> ftList && !ftList.isEmpty()) {
			fragmentType = ftList.get(0).toString();
		} else if (ftObj instanceof String ftStr) {
			fragmentType = ftStr;
		}

		tuple = Tuple.of(request.getAttribName(), new JsonArray(payloads), request.getFirstId(), targetDatasetId);
		String sql = """
				WITH old_entity AS (
				    SELECT ENTITY
				    FROM ENTITY
				    WHERE id = $3
				)
				UPDATE ENTITY
				SET ENTITY = NGSILD_PARTIALUPDATE(ENTITY, $1, $2)
				WHERE id = $3 AND ENTITY ? $1 AND EXISTS (
				    SELECT 1 FROM jsonb_array_elements(ENTITY->$1) elem
				    WHERE (elem #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@id}') IS NOT DISTINCT FROM $4::text
				)
				RETURNING (SELECT ENTITY FROM old_entity) AS old_entry;
				""";
		Uni<Map<String, Object>> doUpdate = connectionManager.executeQuery(request.getTenant(), sql, tuple, false)
				.onItem().transformToUni(rows -> {
					if (rows.size() == 0) {
						return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound,
								"Entity " + request.getFirstId() + " was not found"));
					}
					Row first = rows.iterator().next();
					JsonObject result = first.getJsonObject(0);
					if (result == null) {
						return Uni.createFrom().nullItem();
					}
					return Uni.createFrom().item(result.getMap());
				});
		if (fragmentType == null) {
			return doUpdate;
		}
		// Reject a type change with 400 (instead of silently applying it and returning 204) when the
		// targeted instance already exists with a different type. Read-only pre-check: leaves the
		// update path untouched for matching types / non-existent instances (404 stays 404).
		final String expectedType = fragmentType;
		String typeCheckSql = """
				SELECT elem #>> '{@type,0}' AS old_type
				FROM ENTITY e, jsonb_array_elements(e.ENTITY->$1) elem
				WHERE e.id = $2
				  AND (elem #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@id}') IS NOT DISTINCT FROM $3::text
				LIMIT 1
				""";
		Tuple typeCheckTuple = Tuple.of(request.getAttribName(), request.getFirstId(), targetDatasetId);
		return connectionManager.executeQuery(request.getTenant(), typeCheckSql, typeCheckTuple, false).onItem()
				.transformToUni(rows -> {
					if (rows.size() > 0) {
						String oldType = rows.iterator().next().getString(0);
						if (oldType != null && !oldType.equals(expectedType)) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.BadRequestData,
									"Attribute type cannot be changed in a partial update"));
						}
					}
					return doUpdate;
				});

	}

	/**
	 * 
	 * @param request
	 * @return old version of the entity
	 */
	public Uni<Map<String, Object>> deleteAttribute(DeleteAttributeRequest request) {

		StringBuilder sql = new StringBuilder("""
				WITH old_entity AS (
				    SELECT ENTITY
				    FROM ENTITY
				    WHERE id = $2
				)""");
		Tuple tuple;
		sql.append("UPDATE ENTITY SET ENTITY=");
		if (request.isDeleteAll()) {
			sql.append("ENTITY - $1 WHERE id=$2 AND ENTITY ? $1");
			tuple = Tuple.of(request.getAttribName(), request.getFirstId());
		} else if (request.getDatasetId() != null) {
			// $1/$3 are bind params, so they must NOT sit inside a quoted JSON literal
			// (not interpolated there -> the @> check never matched -> 404). Build the
			// containment object from the parameters instead.
			sql.append("NGSILD_DELETEATTRIB(ENTITY, $1, $3) WHERE id=$2 AND ENTITY @> jsonb_build_object($1::text, "
					+ "jsonb_build_array(jsonb_build_object('" + NGSIConstants.NGSI_LD_DATA_SET_ID
					+ "', jsonb_build_array(jsonb_build_object('@id', $3::text)))))");
			tuple = Tuple.of(request.getAttribName(), request.getFirstId(), request.getDatasetId());
		} else {
			sql.append(
					"NGSILD_DELETEATTRIB(ENTITY, $1, null) WHERE id=$2 AND ENTITY ? $1 AND EXISTS (SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITY->$1) WHERE NOT jsonb_array_elements ? '"
							+ NGSIConstants.NGSI_LD_DATA_SET_ID + "')");
			tuple = Tuple.of(request.getAttribName(), request.getFirstId());
		}
		sql.append(" RETURNING (SELECT ENTITY FROM old_entity) AS old_entity;");
		Log.debug(sql.toString());
		Log.debug(tuple.deepToString());
		return connectionManager.executeQuery(request.getTenant(), sql.toString(), tuple, false).onItem()
				.transformToUni(rows -> {
					if (rows.size() == 0) {
						if (request.getDatasetId() == null) {
							return Uni.createFrom().failure(
									new ResponseException(ErrorType.NotFound, "Attribute " + request.getAttribName()
											+ " on Entity " + request.getFirstId() + " was not found."));
						} else {
							return Uni.createFrom()
									.failure(new ResponseException(ErrorType.NotFound,
											"Attribute " + request.getAttribName() + " with datasetId "
													+ request.getDatasetId() + " on Entity " + request.getFirstId()
													+ " was not found."));
						}
					}
					Row first = rows.iterator().next();
					return Uni.createFrom().item(first.getJsonObject(0).getMap());
				});
	}

	public Uni<RowSet<Row>> upsertEntity(CreateEntityRequest request) {

		String sql = "SELECT * FROM NGSILD_UPSERTENTITY($1::jsonb)";
		return connectionManager
				.executeQuery(request.getTenant(), sql, Tuple.of(new JsonObject(request.getFirstPayload())), true)
				.onFailure().recoverWithUni(e -> Uni.createFrom().failure(e));

	}

	@SuppressWarnings("unchecked")
	public Uni<Void> createEntity(CreateEntityRequest request) {

		String[] types = ((List<String>) request.getFirstPayload().get(NGSIConstants.JSON_LD_TYPE))
				.toArray(new String[0]);
		return connectionManager
				.executeQuery(request.getTenant(), "INSERT INTO ENTITY(ID,E_TYPES, ENTITY) VALUES ($1, $2, $3)",
						Tuple.of(request.getFirstId(), types, new JsonObject(request.getFirstPayload())), true)
				.onFailure().recoverWithUni(e -> {
					if (e instanceof PgException pge) {
						if (pge.getSqlState().equals(AppConstants.SQL_ALREADY_EXISTS)) {
							return Uni.createFrom().failure(new ResponseException(ErrorType.AlreadyExists,
									request.getFirstId() + " already exists"));
						}
					}
					return Uni.createFrom().failure(e);
				}).onItem().transformToUni(v -> Uni.createFrom().voidItem());
	}

	public Uni<Tuple2<String, String>> getEndpoint(String entityId, String tenantId) {
		String query = "SELECT endpoint, csource_alias FROM csource, csourceinformation csi WHERE csource.id=csi.id AND csi.e_id='"
				+ entityId + "'";

		return connectionManager.executeQuery(tenantId, query, null, false).onItem().transform((rowSet) -> {
			if (rowSet.rowCount() == 0) {
				return null;
			}
			Row row = rowSet.iterator().next();
			return Tuple2.of(row.getString(0), row.getString(1));
		}).onFailure().recoverWithUni(Uni.createFrom().item(null));
	}

	public Uni<Table<String, String, List<RegistrationEntry>>> getAllRegistries() {
		return DBUtil.getAllRegistries(connectionManager, ldService,
				"SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription,queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap, csource_Alias FROM csourceinformation WHERE (createEntity OR createBatch OR updateEntity OR appendAttrs OR deleteAttrs OR deleteEntity OR upsertBatch OR updateBatch OR deleteBatch) AND reg_mode != 0",
				logger);

	}

	public Uni<Map<String, Object>> updateEntity(UpdateEntityRequest request) {

		String sql = """
				WITH a AS (
				    SELECT ENTITY
				    FROM ENTITY
				    WHERE ID = $1
				)
				UPDATE ENTITY SET entity = ngsild_update_entity(entity, $2, $3) WHERE ID = $1 RETURNING (SELECT ENTITY FROM a) AS old_entity, ENTITY.entity as new_entity;
				""";

		Tuple tuple = Tuple.of(request.getFirstId(), new JsonObject(request.getFirstPayload()),
				!request.isNoOverwrite());
		// logger.debug(sql);
		// logger.debug(tuple.deepToString());
		return connectionManager.executeQuery(request.getTenant(), sql, tuple, false).onFailure().recoverWithUni(e -> {
			e.printStackTrace();
			return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
		}).onItem().transformToUni(rows -> {
			if (rows.rowCount() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
			}
			Row first = rows.iterator().next();
			return Uni.createFrom().item(first.getJsonObject(0).getMap());
		});

	}

	/**
	 * Update Attributes (NGSI-LD 5.6.2): only attributes already present in the entity are
	 * modified; absent ones (incl. scope) are left out (the caller reports them as notUpdated).
	 * Returns the entity as it was BEFORE the update so the caller can diff old vs. payload.
	 */
	public Uni<Map<String, Object>> updateExistingAttribs(UpdateEntityRequest request) {

		String sql = """
				WITH a AS (
				    SELECT ENTITY
				    FROM ENTITY
				    WHERE ID = $1
				)
				UPDATE ENTITY SET entity = ngsild_update_existing_attribs(entity, $2, $3) WHERE ID = $1 RETURNING (SELECT ENTITY FROM a) AS old_entity, ENTITY.entity as new_entity;
				""";

		Tuple tuple = Tuple.of(request.getFirstId(), new JsonObject(request.getFirstPayload()),
				!request.isNoOverwrite());
		return connectionManager.executeQuery(request.getTenant(), sql, tuple, false).onFailure().recoverWithUni(e -> {
			e.printStackTrace();
			return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
		}).onItem().transformToUni(rows -> {
			if (rows.rowCount() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
			}
			Row first = rows.iterator().next();
			return Uni.createFrom().item(first.getJsonObject(0).getMap());
		});

	}

	/**
	 * 
	 * @param request
	 * @param noOverwrite
	 * @return the not added attribs
	 */
	public Uni<Tuple3<Map<String, Object>, Map<String, Object>, Set<String>>> appendToEntity2(
			AppendEntityRequest request, boolean noOverwrite) {

		String sql = """
				WITH a AS (
				    SELECT ENTITY
				    FROM ENTITY
				    WHERE ID = $1
				)
				UPDATE ENTITY SET entity = ngsild_update_entity(entity, $2, $3) WHERE ID = $1 RETURNING (SELECT ENTITY FROM a) AS old_entity, ENTITY.entity as new_entity;
				""";

		Tuple tuple = Tuple.of(request.getFirstId(), new JsonObject(request.getFirstPayload()), !noOverwrite);
		// logger.debug(sql);
		// logger.debug(tuple.deepToString());
		return connectionManager.executeQuery(request.getTenant(), sql, tuple, false).onFailure().recoverWithUni(e -> {
			return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
		}).onItem().transformToUni(rows -> {
			if (rows.rowCount() == 0) {
				return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
			}
			Row first = rows.iterator().next();
			Map<String, Object> oldEntity = first.getJsonObject(0).getMap();
			Map<String, Object> newEntity = first.getJsonObject(1).getMap();
			// noOverwrite (NGSI-LD 5.6.3): attributes already present in the entity are left
			// untouched -> report them as notUpdated (drives a 207). The instance-level skip is
			// done by ngsild_update_entity; attribute presence in the old entity is the signal.
			Set<String> notAppended = new HashSet<>(0);
			if (noOverwrite) {
				for (String attr : request.getFirstPayload().keySet()) {
					if (attr.startsWith("@") || NGSIConstants.ENTITY_BASE_PROPS.contains(attr)) {
						continue;
					}
					// instance-aware: the attribute was skipped only if noOverwrite left it
					// unchanged (a new instance/datasetId would have changed it).
					Object oldVal = oldEntity.get(attr);
					if (oldVal != null && oldVal.equals(newEntity.get(attr))) {
						notAppended.add(attr);
					}
				}
			}
			return Uni.createFrom().item(Tuple3.of(oldEntity, newEntity, notAppended));
		});

	}

	public Uni<Map<String, Object>> deleteEntity(DeleteEntityRequest request) {

		return connectionManager.executeQuery(request.getTenant(), "DELETE FROM ENTITY WHERE id=$1 RETURNING ENTITY",
				Tuple.of(request.getFirstId()), false).onItem().transformToUni(rows -> {
					if (rows.rowCount() == 0) {
						return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
					}
					return Uni.createFrom().item(rows.iterator().next().getJsonObject(0).getMap());
				});

	}

	public Uni<Map<String, Object>> mergePatch(MergePatchRequest request) {

		Map<String, Object> payload = request.getFirstPayload();
		payload.remove(NGSIConstants.NGSI_LD_CREATED_AT);
		if (payload.get(JsonLdConsts.TYPE) == null) {
			payload.remove(JsonLdConsts.TYPE);
		}
		String sql = "SELECT * FROM MERGE_JSON($1,$2);";
		Tuple tuple = Tuple.of(request.getFirstId(), new JsonObject(payload));
		return connectionManager.executeQuery(request.getTenant(), sql, tuple, false).onFailure().recoverWithUni(e -> {

			if (e instanceof PgException pge) {

				MicroServiceUtils.logPGE(pge, logger);
				if (pge.getSqlState().equals(AppConstants.SQL_NOT_FOUND)) {
					return Uni.createFrom().failure(
							new ResponseException(ErrorType.NotFound, request.getFirstId() + " not found"));
				}
				if (pge.getSqlState().startsWith("SB")) {
					return Uni.createFrom()
							.failure(new ResponseException(ErrorType.BadRequestData, pge.getErrorMessage()));
				}
			}
			logger.debug("database exception", e);
			return Uni.createFrom().failure(e);
		}).onItem().transformToUni(rows -> {
			if (rows.size() == 0)
				return Uni.createFrom()
						.failure(new ResponseException(ErrorType.NotFound, request.getFirstId() + " not found"));
			Row first = rows.iterator().next();
			JsonObject result = first.getJsonObject(0);
			if (result == null) {
				return Uni.createFrom()
						.failure(new ResponseException(ErrorType.NotFound, request.getFirstId() + " not found"));
			}
			return Uni.createFrom().item(result.getMap());
		});

	}

	/**
	 * 
	 * @param request
	 * @return old version of the entity
	 */
	public Uni<Map<String, Object>> replaceEntity(ReplaceEntityRequest request) {
		@SuppressWarnings("unchecked")
		String[] types = ((List<String>) request.getFirstPayload().get(NGSIConstants.JSON_LD_TYPE))
				.toArray(new String[0]);
		String sql = """
				WITH old_entity AS (
				SELECT ENTITY, ENTITY -> 'https://uri.etsi.org/ngsi-ld/createdAt' as createdAt, ENTITY -> 'https://uri.etsi.org/ngsi-ld/modifiedAt' as modifiedAt
				FROM ENTITY
				WHERE id = $1)
				UPDATE ENTITY SET ENTITY = jsonb_set($2,'{https://uri.etsi.org/ngsi-ld/createdAt}', olde.createdAt), E_TYPES = $3 FROM (SELECT * FROM old_entity) as olde WHERE id = $1
				RETURNING (SELECT ENTITY FROM old_entity) AS old_entity;""";
		Tuple tuple = Tuple.of(request.getFirstId(), new JsonObject(request.getFirstPayload()), types);

		return connectionManager.executeQuery(request.getTenant(), sql, tuple, false).onItem()
				.transformToUni(rows -> {
					if (rows.rowCount() == 0) {
						return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
					}
					Row first = rows.iterator().next();
					return Uni.createFrom().item(first.getJsonObject(0).getMap());
				});
	}

	/**
	 * 
	 * @param request
	 * @return old version of the entity
	 */
	public Uni<Map<String, Object>> replaceAttrib(ReplaceAttribRequest request) {
		String sql = """
				WITH old_entity AS (
				  SELECT ENTITY
				  FROM ENTITY
				  WHERE id = $2
				),
				elems AS (
				  SELECT (ordinality - 1)::int AS idx
				  FROM ENTITY, jsonb_array_elements(ENTITY->$3) WITH ORDINALITY
				  WHERE id = $2 AND (
				    ($4::text IS NULL AND NOT value ? 'https://uri.etsi.org/ngsi-ld/datasetId') OR
				    ($4::text IS NOT NULL AND value @> jsonb_build_object('https://uri.etsi.org/ngsi-ld/datasetId', jsonb_build_array(jsonb_build_object('@id', $4::text))))
				  )
				),
				json_data AS (
				  SELECT jsonb_set(($1::jsonb->$3)->0, '{https://uri.etsi.org/ngsi-ld/createdAt}', old_entity.entity->$3->(SELECT idx FROM elems)->'https://uri.etsi.org/ngsi-ld/createdAt', true) AS new_val
				  FROM old_entity
				)
				UPDATE entity
				SET entity = jsonb_set(entity, ARRAY[$3, (SELECT idx FROM elems)::text], (SELECT new_val FROM json_data))
				WHERE id = $2
				  AND EXISTS (SELECT 1 FROM elems)
				RETURNING (SELECT ENTITY FROM old_entity) AS old_entity;
				""";
		Tuple tuple = Tuple.of(new JsonObject(request.getFirstPayload()), request.getFirstId(),
				request.getAttribName(), request.getDatasetId());
		return connectionManager.executeQuery(request.getTenant(), sql, tuple, false)
				.onItem().transformToUni(rows -> {
					if (rows.rowCount() == 0) {
						return Uni.createFrom().failure(new ResponseException(ErrorType.NotFound));
					}
					Row first = rows.iterator().next();
					return Uni.createFrom().item(first.getJsonObject(0).getMap());
				});

	}

	public Uni<Table<String, String, List<RegistrationEntry>>> getAllQueryRegistries() {
		return DBUtil.getAllRegistries(connectionManager, ldService,
				"SELECT cs_id, c_id, e_id, e_id_p, e_type, e_prop, e_rel, ST_AsGeoJSON(i_location), scopes, EXTRACT(MILLISECONDS FROM expires), endpoint, tenant_id, headers, reg_mode, createEntity, updateEntity, appendAttrs, updateAttrs, deleteAttrs, deleteEntity, createBatch, upsertBatch, updateBatch, deleteBatch, upsertTemporal, appendAttrsTemporal, deleteAttrsTemporal, updateAttrsTemporal, deleteAttrInstanceTemporal, deleteTemporal, mergeEntity, replaceEntity, replaceAttrs, mergeBatch, retrieveEntity, queryEntity, queryBatch, retrieveTemporal, queryTemporal, retrieveEntityTypes, retrieveEntityTypeDetails, retrieveEntityTypeInfo, retrieveAttrTypes, retrieveAttrTypeDetails, retrieveAttrTypeInfo, createSubscription, updateSubscription, retrieveSubscription, querySubscription, deleteSubscription, queryEntityMap, createEntityMap, updateEntityMap, deleteEntityMap, retrieveEntityMap, csource_Alias FROM csourceinformation WHERE queryentity OR querybatch OR retrieveentity OR retrieveentitytypes OR retrieveentitytypedetails OR retrieveentitytypeinfo OR retrieveattrtypes OR retrieveattrtypedetails OR retrieveattrtypeinfo",
				logger);
	}

	public Uni<Void> updateValueField(String tenant, String id, String attribId, String datasetId,
			Map<String, Object> value) {
		Tuple t = Tuple.tuple();

		StringBuilder sql = new StringBuilder(
				"WITH JSON_DATA AS(SELECT VALUE, ORDINALITY FROM ENTITY, JSONB_ARRAY_ELEMENTS(ENTITY -> $1) WITH ORDINALITY WHERE ID=$2), "
						+ "ELEMENTS AS (SELECT VALUE, ORDINALITY - 1 AS INDEX FROM JSON_DATA WHERE VALUE ->> '");
		sql.append(NGSIConstants.NGSI_LD_DATA_SET_ID);
		sql.append("' ");

		t.addString(attribId);
		t.addString(id);
		int dollar;
		if (datasetId == null) {
			sql.append("IS NULL");
			dollar = 3;
		} else {
			sql.append("= $3");
			dollar = 4;
			t.addString(datasetId);
		}

		sql.append(") UPDATE ENTITY SET ENTITY=CASE WHEN entity#>>'{$1,@type,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_PROPERTY);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_VALUE);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false) " + "WHEN entity#>>'{$1,@type,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_RELATIONSHIP);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_OBJECT);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false)" + "WHEN entity#>>'{$1,$");
		sql.append(dollar);
		sql.append("ype,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_LISTRELATIONSHIP);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_OBJECT_LIST);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false)");
		sql.append("WHEN entity#>>'{$1,@type,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_LIST_PROPERTY);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_LIST);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false)" + "WHEN entity#>>'{$1,@type,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_LANGPROPERTY);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_LANGUAGE_MAP);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false)" + "WHEN entity#>>'{$1,@type,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_VOCAB_PROPERTY);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_VOCAB);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false)" + "WHEN entity#>>'{$1,@type,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_JSON_PROPERTY);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_JSON);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false)" + "WHEN entity#>>'{$1,$");
		sql.append(dollar);
		sql.append(",@type,0}' = '");
		sql.append(NGSIConstants.NGSI_LD_GEOPROPERTY);
		sql.append("' THEN JSONB_SET(ENTITY, ARRAY[$1,ELEMENTS.INDEX, '");
		sql.append(NGSIConstants.NGSI_LD_HAS_VALUE);
		sql.append("']::text[],$");
		sql.append(dollar);
		sql.append(",false)" + "ELSE ENTITY end FROM ELEMENTS WHERE ENTITY.ID=$2");
		t.addJsonObject(new JsonObject(value));

		return connectionManager.executeQuery(tenant, sql.toString(), t, false).onItem().transformToUni(result -> {

			return Uni.createFrom().voidItem();
		});

	}

	public Uni<Map<String, Object>> mergeBatchEntity(BatchRequest request) {
		List<Map<String, Object>> entities = Lists.newArrayList();
		request.getPayload().values().forEach(entityList -> {
			entities.addAll(entityList);
		});
		Tuple tuple = Tuple.of(new JsonArray(entities));
		return connectionManager.executeQuery(request.getTenant(), "SELECT * FROM MERGE_JSON_BATCH($1)", tuple, true)
				.onItem()
				.transform(rows -> {
					return rows.iterator().next().getJsonObject(0).getMap();
				});
	}

}
