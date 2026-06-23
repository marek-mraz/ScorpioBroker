-- NGSI-LD partial attribute update (5.6.4) fix.
-- The previous ngsild_partialupdate only ever rewrote instance [0] via
--   jsonb_set(Entity,[attribName,'0'], (Entity->attribName->0) || (tmp->0))
-- which, for a multi-instance / datasetId-targeted attribute, merged the wrong
-- instances together and dropped the rest (e.g. patching the "speedometer"
-- instance produced two copies of the "gps" instance and lost "speedometer").
--
-- Correct semantics: a partial update carries a single attribute-instance fragment
-- matched by datasetId (null = the default instance). Merge that fragment into the
-- matching existing instance, preserving any sub-attributes the fragment does not
-- mention, and leave every other instance untouched.
CREATE OR REPLACE FUNCTION ngsild_partialupdate(entity jsonb, attribName text, attribValues jsonb)
 RETURNS jsonb
 LANGUAGE plpgsql
AS $function$
DECLARE
	result jsonb := '[]'::jsonb;
	originalEntry jsonb;
	fragment jsonb;
	datasetId text;
	insertDatasetId text;
	merged jsonb;
	nowTs jsonb;
BEGIN
	nowTs := jsonb_build_array(jsonb_build_object(
		'@type', 'https://uri.etsi.org/ngsi-ld/DateTime',
		'@value', to_char(timezone('utc', now()), 'YYYY-MM-DD"T"HH24:MI:SS') || 'Z'));
	fragment := attribValues -> 0;
	insertDatasetId := fragment #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@id}';
	FOR originalEntry IN SELECT jsonb_array_elements FROM jsonb_array_elements(ENTITY->attribName) LOOP
		datasetId := originalEntry #>> '{https://uri.etsi.org/ngsi-ld/datasetId,0,@id}';
		IF (insertDatasetId IS NULL AND datasetId IS NULL)
		   OR (insertDatasetId IS NOT NULL AND insertDatasetId = datasetId) THEN
			merged := originalEntry || fragment;
			IF NOT fragment ? 'https://uri.etsi.org/ngsi-ld/modifiedAt' THEN
				merged := jsonb_set(merged, Array['https://uri.etsi.org/ngsi-ld/modifiedAt'], nowTs, true);
			END IF;
			result := result || merged;
		ELSE
			result := result || originalEntry;
		END IF;
	END LOOP;
	RETURN jsonb_set(Entity, Array[attribName], result);
END;
$function$;
