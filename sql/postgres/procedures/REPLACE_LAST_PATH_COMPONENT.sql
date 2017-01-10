CREATE OR REPLACE FUNCTION replace_last_path_component(
  node_path CHARACTER VARYING,
  new_value CHARACTER VARYING)
  RETURNS CHARACTER VARYING AS
$BODY$
DECLARE
  lastComp  CHARACTER VARYING;
  modString CHARACTER VARYING;
BEGIN
  RETURN regexp_replace(node_path, '\\[^\\]+\\$', '\\' || new_value || '\\');
END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;