package eu.neclab.ngsildbroker.commons.exceptions;

import java.util.Set;

import com.google.common.collect.Sets;

import eu.neclab.ngsildbroker.commons.enums.ErrorType;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.Provider;

/**
 * NGSI-LD 6.3.20 (since v1.7.1): an unrecognized URI query parameter must be
 * rejected with 400 InvalidRequest. Scorpio otherwise silently ignores unknown
 * parameters. The whitelist is the full set of NGSI-LD defined parameters across
 * all operations, so any valid request passes untouched.
 * ponytail: single global filter instead of per-controller validation.
 */
@Provider
public class UnknownQueryParameterFilter implements ContainerRequestFilter {

	private static final Set<String> ALLOWED = Sets.newHashSet("id", "idPattern", "type", "attrs", "q", "georel",
			"geometry", "coordinates", "geoproperty", "geometryProperty", "csf", "lang", "scopeQ", "datasetId",
			"options", "count", "offset", "limit", "format", "timerel", "timeAt", "endTimeAt", "timeproperty", "lastN",
			"firstN", "aggrMethods", "aggrPeriodDuration", "details", "local", "localOnly", "via", "entityMap", "join",
			"joinLevel", "containedBy", "pick", "omit", "expandValues", "prettyPrint", "deleteAll", "doNotCompact",
			"jsonKeys", "qtoken", "maxDistance", "observationspace", "operationspace",
			// additional parameters Scorpio's controllers accept (@QueryParam union)
			"orderBy", "bbox", "collation", "kind", "minDistance", "n", "nOrder", "offsetN", "orderFrom",
			"orderGeometry", "orderN", "reload", "splitEntities");

	@Override
	public void filter(ContainerRequestContext ctx) {
		String path = ctx.getUriInfo().getPath();
		if (path == null || !path.contains("ngsi-ld")) {
			return;
		}
		for (String name : ctx.getUriInfo().getQueryParameters().keySet()) {
			if (!ALLOWED.contains(name.trim())) {
				ResponseException re = new ResponseException(ErrorType.InvalidRequest,
						"Unrecognized query parameter: " + name);
				ctx.abortWith(Response.status(400).entity(re.getJson())
						.header("Content-Type", "application/json").build());
				return;
			}
		}
	}
}
