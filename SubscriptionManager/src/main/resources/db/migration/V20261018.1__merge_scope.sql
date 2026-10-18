-- NGSI-LD 5.6.17.4 Merge Entity: scope in the fragment replaces the target Entity's scope.
-- MERGE_JSON sent scope through merge_attrib, which rejected it as an unknown attribute
-- type (400). Only the scope branch is new vs. V20240801.1; the rest is carried verbatim.
CREATE OR REPLACE FUNCTION PUBLIC.MERGE_JSON(IN A text,IN B JSONB) RETURNS JSONB LANGUAGE 'plpgsql' VOLATILE PARALLEL UNSAFE COST 100 AS $BODY$
DECLARE
  merged_json JSONB;
  key TEXT;
  value JSONB;
  value2 JSONB;
  previous_entity JSONB;
  deleted JSONB;
  updated JSONB;
BEGIN

Select entity into previous_entity from entity where id =a;
if previous_entity is null then
	RAISE EXCEPTION 'Entity not found.' USING ERRCODE = '02000';
end if;
Select entity into merged_json from entity where id =a;
deleted := '{}';
updated := '{}';
-- Iterate through keys in JSON B
FOR key, value IN SELECT * FROM JSONB_EACH(b)
LOOP
    if key = '@id' or key = 'https://uri.etsi.org/ngsi-ld/createdAt'then
		continue;
	elsif key = '@type' then
		value2 = merged_json -> key;
		WITH combined AS (
			SELECT jsonb_array_elements(value) AS elem
			UNION
			SELECT jsonb_array_elements(value2) AS elem
		)
		SELECT jsonb_agg(elem) into value2 AS merged_array FROM combined;
		merged_json = jsonb_set(merged_json, ARRAY[key]::text[], value2);
	elsif key = 'https://uri.etsi.org/ngsi-ld/modifiedAt' then
		merged_json = jsonb_set(merged_json, ARRAY[key]::text[], value);
	elsif key = 'https://uri.etsi.org/ngsi-ld/scope' then
		-- 5.6.17.4: merge always overwrites, so the fragment's scope replaces the entity's; NGSI-LD Null removes it
		if value @> '[{"@value": "urn:ngsi-ld:null"}]' then
			merged_json = merged_json - key;
			deleted = jsonb_set(deleted, ARRAY[key]::text[], '["@all"]'::jsonb);
		else
			merged_json = jsonb_set(merged_json, ARRAY[key]::text[], value);
		end if;
	else		
		value2 = merged_json -> key;
		value2 = merge_attrib(value, value2);
		if value2 ->'result' = 'null'::jsonb or jsonb_array_length(value2 ->'result') = 0 then
			merged_json = merged_json - key;
			deleted = jsonb_set(deleted, ARRAY[key]::text[], '["@all"]'::jsonb);
		else
			merged_json = jsonb_set(merged_json, ARRAY[key]::text[], value2 -> 'result');
			if jsonb_array_length(value2 -> 'deleted') != 0 then
				if deleted ? key then
					deleted = jsonb_set(deleted, ARRAY[key], ((deleted -> key) || (value2 -> 'deleted')));
				else
					deleted = jsonb_set(deleted, ARRAY[key], ((value2 -> 'deleted')));
				end if;
			end if;
			
			if jsonb_array_length(value2 -> 'updated') != 0 then
				if updated ? key then
					updated = jsonb_set(updated, ARRAY[key], ((updated -> key) || (value2 -> 'updated')));
				else
					updated = jsonb_set(updated, ARRAY[key], ((value2 -> 'updated')));
				end if;
			end if;
			
		end if;
		
		
	end if;
END LOOP;
update entity set entity = merged_json, e_types = ARRAY(SELECT jsonb_array_elements_text(merged_json->'@type')) where id = a;

RETURN jsonb_build_object('old', previous_entity, 'new', merged_json, 'deleted', deleted, 'updated', updated);
END;
$BODY$;
