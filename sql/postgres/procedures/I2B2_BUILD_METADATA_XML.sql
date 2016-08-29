CREATE OR REPLACE FUNCTION i2b2_build_metadata_xml(
	display_name CHARACTER VARYING,
	data_type    CHARACTER VARYING,
	valuetype_cd CHARACTER VARYING
)
	RETURNS TEXT AS
$BODY$
DECLARE
	series_value     VARCHAR(200) := NULL;
	series_unit_name VARCHAR(200) := NULL;
	regTable					text[];
BEGIN
	IF valuetype_cd = 'TIMEPOINT'
	THEN
		IF display_name = 'Baseline'
		THEN
			series_value := '0';
			series_unit_name := 'minutes';
		ELSE
			regTable := regexp_matches(lower(display_name), '^(-?[0-9]{1,4} (week|weeks|minute|minutes|hour|hours|day|days|year|years|month|months))+');
			IF array_length(regTable, 1) > 0 THEN
        select EXTRACT(epoch FROM trim(display_name)::INTERVAL) / 60 into series_value;
        series_unit_name := 'minutes';
      ELSE
				RAISE EXCEPTION  'Check date format';
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
