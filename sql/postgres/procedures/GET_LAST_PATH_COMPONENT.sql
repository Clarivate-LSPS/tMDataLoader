CREATE OR REPLACE FUNCTION get_last_path_component(
  node_path CHARACTER VARYING)
  RETURNS CHARACTER VARYING AS
$BODY$
DECLARE
  parts      TEXT [];
  partsCount INT;
  lastComp   CHARACTER VARYING;
BEGIN
  parts := regexp_split_to_array(node_path, '\\');
  partsCount:= array_length(parts, 1);

  lastComp := parts [partsCount - 1];

  RETURN lastComp;
END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;