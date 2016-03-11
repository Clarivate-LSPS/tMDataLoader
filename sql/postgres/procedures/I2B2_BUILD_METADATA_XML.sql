CREATE OR REPLACE FUNCTION i2b2_build_metadata_xml(
	display_name CHARACTER VARYING,
	data_type    CHARACTER VARYING,
	valuetype_cd CHARACTER VARYING
)
	RETURNS TEXT STABLE AS
$BODY$
DECLARE
	series_value     VARCHAR(200) := NULL;
	series_unit_name VARCHAR(200) := NULL;
BEGIN
	IF valuetype_cd = 'TIMEPOINT'
	THEN
		IF display_name = 'Baseline'
		THEN
			series_value := '0';
			series_unit_name := 'minutes';
		ELSE
			series_value := substring(display_name from '[0-9]+');
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
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;
