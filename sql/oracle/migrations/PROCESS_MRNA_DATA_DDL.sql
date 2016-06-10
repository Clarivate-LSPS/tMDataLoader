-- -------------------------------------------------------
-- PROCESS_MRNA_DATA_DDL.sql
-- Copy tables from tm_wz schema to tm_dataloader schema
-- -------------------------------------------------------
set serveroutput on size unlimited
set linesize 180
set head off

DECLARE 
	rows int;
	drop_sql VARCHAR2(500);
	create_sql VARCHAR2(500);
	source VARCHAR2(100) := 'PROCESS_MRNA_DATA_DDL.sql';
BEGIN
	dbms_output.put_line(source);
	dbms_output.put_line('Copying tables from tm_wz to tm_dataloader schema');
	
	SELECT COUNT(*)
	into rows
	FROM dba_tables
	WHERE upper(owner) = 'TM_DATALOADER'
	  AND upper(table_name) = 'WT_SUBJECT_MRNA_PROBESET';
	  
	IF rows > 0
	THEN
		drop_sql := 'drop table tm_dataloader.wt_subject_mrna_probeset';
		dbms_output.put_line(drop_sql);
		EXECUTE IMMEDIATE drop_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = 'TM_DATALOADER'
	AND table_name = upper('wt_subject_microarray_logs');
	
	IF rows > 0
	THEN
		drop_sql := 'drop table tm_dataloader.wt_subject_microarray_logs';
		dbms_output.put_line(drop_sql);
		EXECUTE IMMEDIATE drop_sql;
	END IF;
	create_sql := 'create table tm_dataloader.wt_subject_microarray_logs as ' ||
				'select * from tm_wz.wt_subject_microarray_logs NOLOGGING';
	dbms_output.put_line(create_sql);
	EXECUTE IMMEDIATE create_sql;
		
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = 'TM_DATALOADER'
	AND table_name = upper('wt_subject_microarray_calcs');
	
	IF rows > 0
	THEN
		drop_sql := 'drop table tm_dataloader.wt_subject_microarray_calcs';
		dbms_output.put_line(drop_sql);
		EXECUTE IMMEDIATE drop_sql;
	END IF;

	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE table_name = 'MIRNA_PROBESET_DEAPP'
	  and owner = 'TM_DATALOADER';
	
	IF rows > 0
	THEN
		drop_sql := 'DROP TABLE tm_dataloader.mirna_probeset_deapp';
                dbms_output.put_line(drop_sql);
                EXECUTE IMMEDIATE drop_sql;
	END IF;

	EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line(source || SQLERRM);
END;
/

@@mirna_probeset_deapp.sql
@@wt_subject_microarray_calcs.sql
@@wt_subject_mrna_probeset.sql
-- Alter types for working tables to increase calculations speed (float point arithmetic much faster than numeric)
alter table tm_dataloader.wt_subject_microarray_logs modify log_intensity number;
alter table tm_dataloader.wt_subject_microarray_logs modify raw_intensity number;

alter table tm_dataloader.wt_subject_microarray_calcs modify mean_intensity number;
alter table tm_dataloader.wt_subject_microarray_calcs modify median_intensity number;
alter table tm_dataloader.wt_subject_microarray_calcs modify stddev_intensity number;
alter table tm_dataloader.wt_subject_mrna_probeset modify intensity_value number;
