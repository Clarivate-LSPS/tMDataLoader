CREATE OR REPLACE FUNCTION I2B2_GET_HLEVEL(concept_path character varying)
  RETURNS numeric
  SET search_path FROM CURRENT
  AS $BODY$

  DECLARE
    root_level INTEGER;
  BEGIN
    SELECT c_hlevel INTO root_level
    FROM i2b2metadata.table_access
    WHERE c_name = parse_nth_value(concept_path, 2, '\');

    RETURN ((length(concept_path) - coalesce(length(replace(concept_path, '\','')),0)) / length('\')) - 2 + root_level;
  END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;

ALTER FUNCTION I2B2_GET_HLEVEL(character varying) OWNER TO postgres;