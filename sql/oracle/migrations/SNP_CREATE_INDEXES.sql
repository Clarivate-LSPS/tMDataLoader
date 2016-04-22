-- -----------------------
-- SNP_CREATE_INDEXES.sql
-- ----------------------
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 180

DECLARE
	rows int;
	create_sql VARCHAR2(2000);
	source VARCHAR2(100) := 'SNP_CREATE_INDEXES.sql';
BEGIN
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'DEAPP'
	AND index_name = upper('de_snp_cLs_by_gsm_pat_nm_idx');

	if rows < 1
	THEN
		create_sql := 'create index DEAPP.de_snp_cLs_by_gsm_pat_nm_idx on deapp.de_snp_calls_by_gsm(patient_num) ';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'DEAPP'
	AND index_name = upper('de_snp_copy_number_num_idx');
	
	if rows < 1
	THEN
		create_sql := 'create index deapp.de_snp_calls_by_gsm_patient_num_idx on deapp.de_snp_calls_by_gsm(patient_num)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	
	EXCEPTION
	WHEN OTHERS THEN
	dbms_output.put_line(source || ':' || SQLERRM);
END;
/
