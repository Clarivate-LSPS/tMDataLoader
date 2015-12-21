CREATE OR REPLACE FUNCTION I2B2_GET_NODE_NAME(concept_path character varying)
	RETURNS CHARACTER VARYING
SET search_path FROM CURRENT
AS $BODY$
	SELECT parse_nth_value(concept_path,length(concept_path)-length(replace(concept_path,'\','')),'\');
$BODY$
LANGUAGE SQL IMMUTABLE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;