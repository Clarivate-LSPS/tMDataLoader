CREATE OR REPLACE FUNCTION i2b2_modifi_last_part_path(
  leaf_node      CHARACTER VARYING,
  baseline_value CHARACTER VARYING
)
  RETURNS CHARACTER VARYING AS
$BODY$
DECLARE
  series_value VARCHAR(200) := NULL;
  result       VARCHAR(2000) := NULL;
  last_part    TEXT [];
  rawValue     NUMERIC(20, 0);
  diffValue    INTERVAL;
  secondValue  NUMERIC(4, 0);
  minuteValue  NUMERIC(4, 0);
  hourValue    NUMERIC(4, 0);
  dayValue     NUMERIC(4, 0);
  yearValue    NUMERIC(4, 0);
BEGIN
  series_value := '';
  last_part := regexp_matches(leaf_node, '.*\\([0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:?[0-9]{0,2})\\$');
  SELECT (to_timestamp(last_part [1], 'YYYY-MM-DD HH24:MI:SS') :: TIMESTAMP WITHOUT TIME ZONE -
          to_timestamp(baseline_value, 'YYYY-MM-DD HH24:MI:SS') :: TIMESTAMP WITHOUT TIME ZONE)
  INTO diffValue;

  SELECT EXTRACT(EPOCH FROM diffValue)
  INTO rawValue;
  IF (rawValue < 0)
  THEN series_value := '-';
    SELECT (to_timestamp(baseline_value, 'YYYY-MM-DD HH24:MI:SS') :: TIMESTAMP WITHOUT TIME ZONE -
            to_timestamp(last_part [1], 'YYYY-MM-DD HH24:MI:SS') :: TIMESTAMP WITHOUT TIME ZONE)
    INTO diffValue;
  END IF;

  SELECT EXTRACT(DAY FROM diffValue)
  INTO dayValue;
  SELECT EXTRACT(HOUR FROM diffValue)
  INTO hourValue;
  SELECT EXTRACT(MINUTE FROM diffValue)
  INTO minuteValue;
  SELECT EXTRACT(SECOND FROM diffValue)
  INTO secondValue;

  IF (rawValue = 0)
  THEN series_value := 'Baseline'; END IF;

  IF (yearValue <> 0)
  THEN series_value := series_value || yearValue || ' year';
    IF (yearValue > 1)
    THEN series_value := series_value || 's ';
    ELSE series_value := series_value || ' '; END IF; END IF;
  IF (dayValue <> 0)
  THEN series_value := series_value || dayValue || ' day';
    IF (dayValue > 1)
    THEN series_value := series_value || 's ';
    ELSE series_value := series_value || ' '; END IF; END IF;
  IF (hourValue <> 0)
  THEN series_value := series_value || hourValue || ' hour';
    IF (hourValue > 1)
    THEN series_value := series_value || 's ';
    ELSE series_value := series_value || ' '; END IF; END IF;
  IF (minuteValue <> 0)
  THEN series_value := series_value || minuteValue || ' minute';
    IF (minuteValue > 1)
    THEN series_value := series_value || 's ';
    ELSE series_value := series_value || ' '; END IF; END IF;
  IF (secondValue <> 0)
  THEN series_value := series_value || secondValue || ' second';
    IF (secondValue > 1)
    THEN series_value := series_value || 's ';
    ELSE series_value := series_value || ' '; END IF; END IF;

  series_value := trim(series_value);
  result := regexp_replace(leaf_node, last_part [1] || '\\$', series_value || '\\');
  RETURN result;
END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;