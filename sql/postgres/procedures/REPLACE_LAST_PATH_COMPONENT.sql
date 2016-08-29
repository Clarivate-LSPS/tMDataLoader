CREATE OR REPLACE FUNCTION replace_last_path_component(
  node_path CHARACTER VARYING,
  new_value CHARACTER VARYING)
  RETURNS CHARACTER VARYING AS
$BODY$
DECLARE
  lastComp  CHARACTER VARYING;
  modString CHARACTER VARYING;
BEGIN
  lastComp := get_last_path_component(node_path);

  modString := regexp_replace(node_path, lastComp || '\\$', new_value || '\\');
  RETURN modString;
END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;