CREATE OR REPLACE FUNCTION "I2B2_MODIFI_LAST_PART_PATH"(
  leaf_node VARCHAR2,
  baseline  VARCHAR2)
  RETURN VARCHAR2 IS
  series_value     VARCHAR2(200) := NULL;
  series_unit_name VARCHAR2(200) := NULL;
  last_part        VARCHAR2(200);
  rawValue         LONG;
  diffValue        INTERVAL DAY TO SECOND;
  diffDate         NUMBER;
  secondValue      INTEGER;
  minuteValue      INTEGER;
  hourValue        INTEGER;
  dayValue         INTEGER;
  yearValue        INTEGER := 0;
  modString        VARCHAR2(4000);
  BEGIN
    series_value := '';
    last_part := REGEXP_SUBSTR(REGEXP_SUBSTR(leaf_node,
                                             '\\([0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:?[0-9]{0,2})\\$'),
                               '([0-9]{1,4}-[0-9]{1,2}-[0-9]{1,2} [0-9]{1,2}:[0-9]{1,2}:?[0-9]{0,2})');
    diffValue := TO_TIMESTAMP(last_part, 'YYYY-MM-DD HH24:MI:SS') - TO_TIMESTAMP(baseline, 'YYYY-MM-DD HH24:MI:SS');
    diffDate := TO_DATE(last_part, 'YYYY-MM-DD HH24:MI:SS') - TO_DATE(baseline, 'YYYY-MM-DD HH24:MI:SS');

    IF (diffDate < 0)
    THEN
      series_value := '-';
      diffValue := TO_TIMESTAMP(baseline, 'YYYY-MM-DD HH24:MI:SS') - TO_TIMESTAMP(last_part, 'YYYY-MM-DD HH24:MI:SS');
    END IF;

    SELECT EXTRACT(DAY FROM diffValue) / 365
    INTO yearValue
    FROM dual;
    SELECT EXTRACT(DAY FROM (diffValue))
    INTO dayValue
    FROM dual;
    SELECT EXTRACT(HOUR FROM (diffValue))
    INTO hourValue
    FROM dual;
    SELECT EXTRACT(MINUTE FROM (diffValue))
    INTO minuteValue
    FROM dual;
    SELECT EXTRACT(SECOND FROM (diffValue))
    INTO secondValue
    FROM dual;

    IF (diffDate = 0)
    THEN series_value := 'Baseline'; END IF;

    IF (yearValue != 0)
    THEN series_value := series_value || yearValue || ' year';
      IF (yearValue > 1)
      THEN series_value := series_value || 's ';
      ELSE series_value := series_value || ' '; END IF; END IF;
    IF (dayValue != 0)
    THEN series_value := series_value || dayValue || ' day';
      IF (dayValue > 1)
      THEN series_value := series_value || 's ';
      ELSE series_value := series_value || ' '; END IF; END IF;
    IF (hourValue != 0)
    THEN series_value := series_value || hourValue || ' hour';
      IF (hourValue > 1)
      THEN series_value := series_value || 's ';
      ELSE series_value := series_value || ' '; END IF; END IF;
    IF (minuteValue != 0)
    THEN series_value := series_value || minuteValue || ' minute';
      IF (minuteValue > 1)
      THEN series_value := series_value || 's ';
      ELSE series_value := series_value || ' '; END IF; END IF;
    IF (secondValue != 0)
    THEN series_value := series_value || secondValue || ' second';
      IF (secondValue > 1)
      THEN series_value := series_value || 's ';
      ELSE series_value := series_value || ' '; END IF; END IF;


    series_value := trim(series_value);
    modString := regexp_replace(leaf_node, last_part || '\\$', series_value || '\\');
    RETURN modString;
  END;
/
