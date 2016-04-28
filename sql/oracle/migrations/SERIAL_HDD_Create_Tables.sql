-- --------------------------------
-- SERIAL_HDD_Create_Tables.sql
-- --------------------------------
set define on;
set serveroutput on size unlimited
set linesize 180

DEFINE TM_LZ_SCHEMA='TM_DATALOADER';

DECLARE
	rows INT;
	drop_sql VARCHAR2(100);
	source VARCHAR2(100) := 'SERIAL_HDD_Create_Tables.sql';
BEGIN
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = '&TM_LZ_SCHEMA'
      AND table_name = 'LT_SRC_MRNA_XML_DATA';

	if rows > 0
	THEN
		drop_sql := 'DROP TABLE "&TM_LZ_SCHEMA".LT_SRC_MRNA_XML_DATA';
		dbms_output.put_line(drop_sql);
		EXECUTE IMMEDIATE drop_sql;
	END IF;
	
	EXCEPTION
		WHEN OTHERS THEN
		dbms_output.put_line(source || ':' || SQLERRM);
END;
/

CREATE TABLE "&TM_LZ_SCHEMA".LT_SRC_MRNA_XML_DATA (
    STUDY_ID varchar2(50) NOT NULL,
    CATEGORY_CD varchar2(2000) NOT NULL,
    C_METADATAXML varchar2(4000) NOT NULL
);
