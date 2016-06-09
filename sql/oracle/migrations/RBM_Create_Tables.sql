set define on;
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 180

DEFINE TM_WZ_SCHEMA='TM_WZ';
DEFINE TM_LZ_SCHEMA='TM_LZ';
DEFINE TM_CZ_SCHEMA='TM_DATALOADER';

DECLARE
	rows INT;
	drop_sql VARCHAR2(100);
	source VARCHAR2(100);
BEGIN
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = '&TM_CZ_SCHEMA'
	  AND table_name = 'STG_SUBJECT_RBM_DATA';

	if rows > 0
	THEN
		drop_sql := 'drop table "&TM_CZ_SCHEMA".STG_SUBJECT_RBM_DATA';
		dbms_output.put_line(drop_sql);
		EXECUTE IMMEDIATE drop_sql;
	END IF;

	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = '&TM_CZ_SCHEMA'
	  AND table_name = 'STG_SUBJECT_RBM_DATA_RAW';
	  
  	if rows > 0
  	THEN
  		drop_sql := 'drop table "&TM_CZ_SCHEMA".STG_SUBJECT_RBM_DATA_RAW';
  		dbms_output.put_line(drop_sql);
  		EXECUTE IMMEDIATE drop_sql;
  	END IF;

	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = '"&TM_CZ_SCHEMA'
	  AND table_name = 'PATIENT_INFO';
	  
  	if rows > 0
  	THEN
  		drop_sql := 'DROP TABLE &TM_CZ_SCHEMA.PATIENT_INFO';
  		dbms_output.put_line(drop_sql);
  		EXECUTE IMMEDIATE drop_sql;
  	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = '&TM_CZ_SCHEMA'
	  AND table_name = 'STG_RBM_ANTIGEN_GENE';
	  
  	if rows > 0
  	THEN
  		drop_sql := 'DROP TABLE "&TM_CZ_SCHEMA".STG_RBM_ANTIGEN_GENE';
  		dbms_output.put_line(drop_sql);
  		EXECUTE IMMEDIATE drop_sql;
  	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = '&TM_CZ_SCHEMA'
	  AND table_name = UPPER('tmp_subject_rbm_log');
	  
  	if rows > 0
  	THEN
  		drop_sql := 'DROP TABLE "&TM_CZ_SCHEMA".tmp_subject_rbm_log';
  		dbms_output.put_line(drop_sql);
  		EXECUTE IMMEDIATE drop_sql;
  	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = '&TM_WZ_SCHEMA'
	  AND table_name = UPPER('tmp_subject_rbm_logs');
	  
  	if rows > 0
  	THEN
  		drop_sql := 'DROP TABLE "&TM_WZ_SCHEMA".tmp_subject_rbm_logs';
  		dbms_output.put_line(drop_sql);
  		EXECUTE IMMEDIATE drop_sql;
  	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = '&TM_WZ_SCHEMA'
	  AND table_name = UPPER('tmp_subject_rbm_calcs');
	  
  	if rows > 0
  	THEN
  		drop_sql := 'DROP TABLE "&TM_WZ_SCHEMA".tmp_subject_rbm_calcs';
  		dbms_output.put_line(drop_sql);
  		EXECUTE IMMEDIATE drop_sql;
  	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = '&TM_WZ_SCHEMA'
	  AND table_name = UPPER('tmp_subject_rbm_med');
	  
  	if rows > 0
  	THEN
  		drop_sql := 'DROP TABLE "&TM_WZ_SCHEMA".tmp_subject_rbm_med';
  		dbms_output.put_line(drop_sql);
  		EXECUTE IMMEDIATE drop_sql;
  	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = '&TM_WZ_SCHEMA'
	  AND table_name = 'DE_SUBJECT_RBM_DATA';
	  
  	if rows > 0
  	THEN
  		drop_sql := 'DROP TABLE "&TM_WZ_SCHEMA".DE_SUBJECT_RBM_DATA';
  		dbms_output.put_line(drop_sql);
  		EXECUTE IMMEDIATE drop_sql;
  	END IF;

	  
	EXCEPTION
		WHEN OTHERS THEN
		dbms_output.put_line(source || ':' || SQLERRM);
END;
/

create table "&TM_CZ_SCHEMA".STG_SUBJECT_RBM_DATA
(
  TRIAL_NAME varchar(100),
  ANTIGEN_NAME varchar(100),
  VALUE_TEXT varchar(100),
  VALUE_NUMBER NUMBER,
  TIMEPOINT varchar(100),
  ASSAY_ID varchar(100),
  SAMPLE_ID varchar(100),
  SUBJECT_ID varchar(100),
  SITE_ID varchar(100)
);

create table "&TM_CZ_SCHEMA".STG_SUBJECT_RBM_DATA_RAW
(
  TRIAL_NAME varchar(100),
  ANTIGEN_NAME varchar(100),
  VALUE_TEXT varchar(100),
  VALUE_NUMBER NUMBER,
  TIMEPOINT varchar(100),
  ASSAY_ID varchar(100),
  SAMPLE_ID varchar(100),
  SUBJECT_ID varchar(100),
  SITE_ID varchar(100)
);

create or replace view "&TM_CZ_SCHEMA".PATIENT_INFO
as select TRIAL_NAME as STUDY_ID, SUBJECT_ID, SITE_ID, REGEXP_REPLACE(TRIAL_NAME || ':' || SITE_ID || ':' || SUBJECT_ID,
                   '(::){1,}', ':') as usubjid from "&TM_CZ_SCHEMA".stg_subject_rbm_data;


create table "&TM_CZ_SCHEMA".STG_RBM_ANTIGEN_GENE
(
  ANTIGEN_NAME varchar(255),
  GENE_SYMBOL varchar(100),
  gene_id varchar(100)
);

create table "&TM_CZ_SCHEMA".tmp_subject_rbm_logs as
				  select trial_name
                  ,antigen_name
                  ,n_value
                  ,patient_id
                  ,gene_symbol
                  ,gene_id
                  ,assay_id
                  ,normalized_value
                  ,concept_cd
                  ,timepoint
                  ,value
                  ,n_value as log_intensity
                  from deapp.de_subject_rbm_data
                  where 1=2;

create table "&TM_CZ_SCHEMA".tmp_subject_rbm_calcs as
               select trial_name
				,gene_symbol
				,antigen_name
				,log_intensity as mean_intensity
				,log_intensity as median_intensity
				,log_intensity as stddev_intensity
				from deapp.de_subject_rbm_data
				where 1=2;

create table "&TM_CZ_SCHEMA".tmp_subject_rbm_med as
				select trial_name
                    ,antigen_name
	                ,n_value
	                ,patient_id
                    ,gene_symbol
                    ,gene_id
	                ,assay_id
	                ,normalized_value
	                ,concept_cd
	                ,timepoint
                    ,log_intensity
	                ,value
                    ,log_intensity as mean_intensity
	                ,log_intensity as stddev_intensity
	                ,log_intensity as median_intensity
                    ,LOG_INTENSITY as ZSCORE
                   from deapp.de_subject_rbm_data
		where 1=2;

           

create table "&TM_CZ_SCHEMA".DE_SUBJECT_RBM_DATA
as select * from deapp.de_subject_rbm_data;

@@wt_subject_rbm_probeset.sql
@@lt_src_rbm_subj_samp_map.sql
@@lt_src_rbm_data.sql
@@lt_src_rbm_annotation.sql
@@lt_src_rbm_display_mapping.sql
@@wt_rbm_nodes.sql
@@wt_rbm_node_values.sql
@@wt_subject_rbm_calcs.sql
@@wt_subject_rbm_logs.sql
@@wt_subject_rbm_med.sql
