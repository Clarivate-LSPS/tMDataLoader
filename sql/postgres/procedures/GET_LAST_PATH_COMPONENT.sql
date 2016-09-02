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
  partsCount:= array_length(parts, 1);

  IF (partsCount = 1)
  THEN
    IF (node_path ~ '\$$')
    THEN
      pos := 1;
    ELSE
      pos := 0;
    END IF;

    parts := regexp_split_to_array(node_path, '\+');

    partsCount:= array_length(parts, 1);
    lastComp := parts [partsCount - pos];
  ELSE
    lastComp := parts [partsCount - 1];
  END IF;

  RETURN lastComp;
END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;