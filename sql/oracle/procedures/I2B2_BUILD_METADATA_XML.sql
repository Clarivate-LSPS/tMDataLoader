CREATE OR REPLACE FUNCTION "I2B2_BUILD_METADATA_XML"(
  display_name VARCHAR2,
  data_type    VARCHAR2,
  valuetype_cd VARCHAR2)
  RETURN CLOB IS
  series_value     VARCHAR2(200) := NULL;
  series_unit_name VARCHAR2(200) := NULL;
  iter             INT;
  minutes          NUMBER(18,0);
  negative         INT := 0 ;
  dName            VARCHAR2(4000);
  BEGIN
    IF valuetype_cd = 'TIMEPOINT'
    THEN
      IF display_name = 'Baseline'
      THEN
        series_value := '0';
        series_unit_name := 'minutes';
      ELSE
        iter := 1;
        minutes := 0;

        select INSTR(display_name, '-', 1) into negative from dual;
        if (negative > 0 ) then
          dName := substr(display_name, (negative + 1));
        ELSE
          dName := display_name;
        END IF;

        WHILE (REGEXP_INSTR(dName, '[0-9]+ (week|weeks|minute|minutes|hour|hours|day|days|year|years|month|months)+', iter) > 0) LOOP
          series_value := REGEXP_SUBSTR(dName, '[0-9]+', iter);
          series_unit_name := LOWER(REGEXP_SUBSTR(dName, '(week|weeks|minute|minutes|hour|hours|day|days|year|years|month|months)+', iter + 2));

          iter := REGEXP_INSTR(dName, '[0-9]+ (week|weeks|minute|minutes|hour|hours|day|days|year|years|month|months)+', iter) + 4;
          IF series_unit_name IN ('minute','minutes')
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
        if (negative > 0) THEN
          minutes := -1 * minutes;
        END IF;
        series_value := TO_CHAR(minutes);
      END IF;
    END IF;

    RETURN
    CASE
    WHEN series_unit_name IS NOT NULL
      THEN
        '<?xml version="1.0"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis>'
        || '<SeriesMeta><Value>' || series_value || '</Value>'
        || '<Unit>' || series_unit_name || '</Unit>'
        || '<DisplayName>' || display_name || '</DisplayName></SeriesMeta>'
        || '</ValueMetadata>'
    WHEN data_type = 'N'
      THEN
        '<?xml version="1.0"?><ValueMetadata><Version>3.02</Version><CreationDateTime>08/14/2008 01:22:59</CreationDateTime><TestID></TestID><TestName></TestName><DataType>PosFloat</DataType><CodeType></CodeType><Loinc></Loinc><Flagstouse></Flagstouse><Oktousevalues>Y</Oktousevalues><MaxStringLength></MaxStringLength><LowofLowValue>0</LowofLowValue><HighofLowValue>0</HighofLowValue><LowofHighValue>100</LowofHighValue>100<HighofHighValue>100</HighofHighValue><LowofToxicValue></LowofToxicValue><HighofToxicValue></HighofToxicValue><EnumValues></EnumValues><CommentsDeterminingExclusion><Com></Com></CommentsDeterminingExclusion><UnitValues><NormalUnits>ratio</NormalUnits><EqualUnits></EqualUnits><ExcludingUnits></ExcludingUnits><ConvertingUnits><Units></Units><MultiplyingFactor></MultiplyingFactor></ConvertingUnits></UnitValues><Analysis><Enums /><Counts /><New /></Analysis></ValueMetadata>'
    ELSE NULL
    END;
  END;


        /
