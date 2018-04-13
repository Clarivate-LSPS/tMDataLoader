CREATE OR REPLACE FUNCTION "I2B2_BUILD_METADATA_XML"(
  display_name    VARCHAR2,
  data_type       VARCHAR2,
  valuetype_cd    VARCHAR2,
  sourcesystem_cd VARCHAR2,
  c_fullname      VARCHAR2
)
  RETURN CLOB IS
  series_value     VARCHAR2(200) := NULL;
  series_unit_name VARCHAR2(200) := NULL;
  iter             INT;
  minutes          NUMBER(18, 0);
  negative         INT := 0;
  studyNum         NUMBER(18, 0);
  rowCt            NUMBER(18, 0);
  trialVisitNum    NUMBER(38, 0);
  BEGIN
    IF valuetype_cd = 'TIMEPOINT'
    THEN
      IF display_name = 'Baseline'
      THEN
        series_value := '0';
        series_unit_name := 'minutes';
      ELSE
        IF (regexp_instr(display_name, '^[a-zA-Z]+ -?[0-9]+') > 0)
        THEN
          series_value := REGEXP_SUBSTR(display_name, '-?[0-9]+');
          series_unit_name := LOWER(REGEXP_SUBSTR(display_name, '[a-zA-Z]+'));
          IF series_unit_name  IN ('minute', 'minutes')
          THEN
            series_unit_name := 'minutes';
          ELSIF series_unit_name IN ('hour', 'hours')
            THEN
              series_unit_name := 'minutes';
              series_value := TO_CHAR(TO_NUMBER(series_value) * 60);
          ELSIF series_unit_name IN ('day', 'days')
            THEN
              series_unit_name := 'minutes';
              series_value := TO_CHAR(TO_NUMBER(series_value) * 60 * 24);
          ELSIF series_unit_name IN ('week', 'weeks')
            THEN
              series_unit_name := 'minutes';
              series_value := TO_CHAR(TO_NUMBER(series_value) * 60 * 24 * 7);
          ELSIF series_unit_name IN ('month', 'months')
            THEN
              series_unit_name := 'minutes';
              series_value := TO_CHAR(TO_NUMBER(series_value) * 60 * 24 * 30);
          ELSIF series_unit_name IN ('year', 'years')
            THEN
              series_unit_name := 'minutes';
              series_value := TO_CHAR(TO_NUMBER(series_value) * 60 * 24 * 30 * 12);
          END IF;
        ELSE
          iter := 1;
          minutes := 0;

          SELECT REGEXP_INSTR(display_name, '^\s*-', iter)
          INTO negative
          FROM dual;
          IF (negative > 0)
          THEN
            iter := negative + 1;
          END IF;


          WHILE (TRUE) LOOP
            iter := REGEXP_INSTR(display_name, '[0-9]+\s+(week|weeks|minute|minutes|hour|hours|day|days|year|years|month|months)+', iter);
            EXIT WHEN iter is null or iter < 1;

            series_value := REGEXP_SUBSTR(display_name, '[0-9]+', iter);
            iter := iter + LENGTH(series_value) + 1;
            series_unit_name := LOWER(
                REGEXP_SUBSTR(display_name, '(week|weeks|minute|minutes|hour|hours|day|days|year|years|month|months)+', iter));
            iter := iter + LENGTH(series_unit_name);

            IF series_unit_name IN ('minute', 'minutes')
            THEN
              series_unit_name := 'minutes';
              minutes := minutes + TO_NUMBER(series_value);
            ELSIF series_unit_name IN ('hour', 'hours')
              THEN
                series_unit_name := 'minutes';
                minutes := minutes + TO_NUMBER(series_value) * 60;
            ELSIF series_unit_name IN ('day', 'days')
              THEN
                series_unit_name := 'minutes';
                minutes := minutes + TO_NUMBER(series_value) * 60 * 24;
            ELSIF series_unit_name IN ('week', 'weeks')
              THEN
                series_unit_name := 'minutes';
                minutes := minutes + TO_NUMBER(series_value) * 60 * 24 * 7;
            ELSIF series_unit_name IN ('month', 'months')
              THEN
                series_unit_name := 'minutes';
                minutes := minutes + TO_NUMBER(series_value) * 60 * 24 * 30;
            ELSIF series_unit_name IN ('year', 'years')
              THEN
                series_unit_name := 'minutes';
                minutes := minutes + TO_NUMBER(series_value) * 60 * 24 * 30 * 12;
            END IF;
          END LOOP;
          IF (negative > 0)
          THEN
            minutes := -minutes;
          END IF;
          series_value := TO_CHAR(minutes);
        END IF;
      END IF;

      SELECT study_num
      INTO studyNum
      FROM i2b2demodata.study
      WHERE study_id = sourcesystem_cd;

      SELECT count(*)
      INTO rowCt
      FROM i2b2demodata.trial_visit_dimension
      WHERE
        study_num = studyNum AND
        REL_TIME_UNIT_CD = series_unit_name AND
        REL_TIME_NUM = series_value AND
        REL_TIME_LABEL = display_name;

      IF rowCt = 0
      THEN
        INSERT INTO I2B2DEMODATA.TRIAL_VISIT_DIMENSION (
          STUDY_NUM,
          REL_TIME_UNIT_CD,
          REL_TIME_NUM,
          REL_TIME_LABEL
        ) VALUES (
          studyNum,
          series_unit_name,
          series_value,
          display_name
        )
        RETURNING trial_visit_num
        INTO trialVisitNum;
      ELSE
        SELECT trial_visit_num
        INTO trialVisitNum
        FROM i2b2demodata.trial_visit_dimension
        WHERE
          study_num = studyNum AND
          REL_TIME_UNIT_CD = series_unit_name AND
          REL_TIME_NUM = series_value AND
          REL_TIME_LABEL = display_name;
      END IF;

      INSERT INTO tm_dataloader.concept_specific_trials
      (TRIAL_VISIT_NUM, c_fullname)
      VALUES (trialVisitNum, c_fullname);
    END IF;

    RETURN
    CASE
    WHEN series_unit_name IS NOT NULL
      THEN
        '<?xml version="1.0"?><ValueMetadata>' ||
        '<Version>3.02</Version>' ||
        '<CreationDateTime>' || sysdate || '</CreationDateTime>' ||
        '<Oktousevalues>Y</Oktousevalues>' ||
        '<UnitValues>' ||
        '  <NormalUnits>ratio</NormalUnits>' ||
        '</UnitValues>' ||
        '</ValueMetadata>'
    WHEN data_type = 'N'
      THEN
        '<?xml version="1.0"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>'
    ELSE NULL
    END;
  END;
/
