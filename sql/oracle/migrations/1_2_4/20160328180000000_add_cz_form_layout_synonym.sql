DECLARE
	flag NUMBER(1,0);
	form_layout_table VARCHAR2(200);
BEGIN
	SELECT COUNT(*) INTO flag FROM all_synonyms WHERE OWNER = 'BIOMART_USER' AND SYNONYM_NAME = 'CZ_FORM_LAYOUT';

	-- Check if CZ_FORM_LAYOUT synonym is not yet exists
	IF flag = 0 THEN
		SELECT COUNT(*) INTO flag FROM all_tables WHERE OWNER = 'SEARCHAPP' AND TABLE_NAME = 'SEARCH_FORM_LAYOUT';

		-- Check if we have newer version of database
		IF flag = 1 THEN
			form_layout_table := 'searchapp.search_form_layout';
		ELSE
			form_layout_table := 'tm_cz.cz_form_layout';
		END IF;

		EXECUTE IMMEDIATE 'grant select on ' || form_layout_table || ' to biomart_user';
		EXECUTE IMMEDIATE 'create synonym biomart_user.cz_form_layout for ' || form_layout_table;
	END IF;
END;
/