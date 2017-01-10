-- --------------------------------
-- CREATE_INDEXES_FOR_ACGH_DATA.sql
-- --------------------------------
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 180

DECLARE
	rows INT;
	source VARCHAR2(100) := 'CREATE_INDEXES_FOR_ACGH_DATA.sql';
	create_sql VARCHAR2(2000);
BEGIN
	dbms_output.put_line(source);

	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'DEAPP'
	AND index_name = upper('IDX_DE_CHROMOSAL_REGION');

	if rows < 1
	THEN
		create_sql := 'CREATE INDEX DEAPP.IDX_DE_CHROMOSAL_REGION ON DEAPP.de_chromosomal_region (GPL_ID, GENE_SYMBOL) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
		END IF;
EXCEPTION
	WHEN OTHERS THEN
	dbms_output.put_line(source || ':' || SQLERRM);

END;
/	
