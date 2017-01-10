CREATE OR REPLACE FUNCTION get_last_path_component(
	node_path CHARACTER VARYING)
	RETURNS CHARACTER VARYING AS
$BODY$
DECLARE
	parts      TEXT [];
	partsCount INT;
	lastComp   CHARACTER VARYING;
	pos        INT;
BEGIN
	parts := regexp_split_to_array(node_path, '\\');
	RETURN parts [array_length(parts, 1) - 1];
END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;