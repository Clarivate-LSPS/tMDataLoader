-- ----------------------------------
-- SERIAL_HDD_DATA_CREATE_TABLES.sql
-- ----------------------------------
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 180
SET HEADING OFF

SELECT 'SERIAL_HDD_DATA_CREATE_TABLES.sql' FROM DUAL;
SELECT 'Creating table  tm_dataloader.lt_src_mrna_xml_data' FROM DUAL;

DECLARE
rows int;
create_sql VARCHAR2(2000);
BEGIN
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = 'TM_DATALOADER'
	  AND table_name = upper('lt_src_mrna_xml_data');

	IF rows < 1 THEN
		create_sql := 
		'CREATE TABLE tm_dataloader.lt_src_mrna_xml_data (
      	  STUDY_ID VARCHAR2(50) NOT NULL,
      	  CATEGORY_CD VARCHAR(2000) NOT NULL,
          C_METADATAXML CLOB NOT NULL
         ) NOLOGGING';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
END;
/

