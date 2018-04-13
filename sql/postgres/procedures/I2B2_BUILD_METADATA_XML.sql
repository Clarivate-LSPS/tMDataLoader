CREATE OR REPLACE FUNCTION i2b2_build_metadata_xml(
	display_name    CHARACTER VARYING,
	data_type       CHARACTER VARYING,
	valuetype_cd    CHARACTER VARYING,
	sourcesystem_cd CHARACTER VARYING,
	c_fullname      CHARACTER VARYING
)
	RETURNS TEXT AS
$BODY$
DECLARE
	series_value     VARCHAR(200) := NULL;
	series_unit_name VARCHAR(200) := NULL;
	regTable         TEXT [];
	studyNum         NUMERIC(18, 0);
	rowCt            NUMERIC(18, 0);
	trialVisitNum    NUMERIC(38, 0);
BEGIN
	IF valuetype_cd = 'TIMEPOINT'
	THEN
		IF display_name = 'Baseline'
		THEN
			series_value := '0';
			series_unit_name := 'minutes';
		ELSIF lower(display_name) ~ '^[a-zA-Z]+ -?\d+' THEN
			series_value := substring(display_name from '-?[0-9]+');
			series_unit_name := lower(substring(display_name from '[a-zA-Z]+'));
			IF series_unit_name = 'minute'
			THEN
				series_unit_name := 'minutes';
			ELSIF series_unit_name IN ('hour', 'hours')
				THEN
					series_unit_name := 'minutes';
					series_value := (series_value::FLOAT * 60)::VARCHAR;
			ELSIF series_unit_name IN ('day', 'days')
				THEN
					series_unit_name := 'minutes';
					series_value := (series_value::FLOAT * 60 * 24)::VARCHAR;
			ELSIF series_unit_name IN ('week', 'weeks')
				THEN
					series_unit_name := 'minutes';
					series_value := (series_value::FLOAT * 60 * 24 * 7)::VARCHAR;
			ELSIF series_unit_name IN ('month', 'months')
				THEN
					series_unit_name := 'minutes';
					series_value := (series_value::FLOAT * 60 * 24 * 30)::VARCHAR;
			ELSIF series_unit_name IN ('year', 'years')
				THEN
					series_unit_name := 'minutes';
					series_value := (series_value::FLOAT * 60 * 24 * 30 * 12)::VARCHAR;
			END IF;
		ELSE
			regTable := regexp_matches(lower(display_name), '^(-?[0-9]{1,4} (week|weeks|minute|minutes|hour|hours|day|days|year|years|month|months))+');
			IF array_length(regTable, 1) > 0 THEN
        select EXTRACT(epoch FROM trim(display_name)::INTERVAL) / 60 into series_value;
        series_unit_name := 'minutes';
      ELSE
				RAISE EXCEPTION  'Check date format';
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

		INSERT INTO concept_specific_trials VALUES (trialVisitNum, c_fullname);
	END IF;

	RETURN
	CASE
	WHEN series_unit_name IS NOT NULL
		THEN
			'<?xml version="1.0"?><ValueMetadata>' ||
			'<Version>3.02</Version>' ||
			'<CreationDateTime>' || current_timestamp || '</CreationDateTime>' ||
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
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;
