-- ----------------------
-- 1_run_first.sql
-- ----------------------
SET DEFINE ON;
SET SERVEROUTPUT ON SIZE UNLIMITED
SET HEADING OFF
SET LINESIZE 180

DEFINE TM_WZ_SCHEMA='TM_DATALOADER';
DEFINE TM_LZ_SCHEMA='TM_DATALOADER';
DEFINE TM_CZ_SCHEMA='TM_DATALOADER';
select '1_run_first.sql' FROM dual;

select 'Creating job tables...' from DUAL;
@@cz_job_audit.sql
@@cz_job_error.sql
@@cz_job_master.sql
@@cz_job_message.sql


DECLARE
	rows INT;
	drop_sql VARCHAR2(100);
	create_sql VARCHAR2(2000);
	alter_sql VARCHAR2(2000);
	grant_sql VARCHAR2(1000);
	source VARCHAR2(200) := '1_run_first.sql';
BEGIN
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'I2B2DEMODATA'
	  AND index_name = 'OF_CTX_BLOB';

	if rows > 0
	THEN
		drop_sql := 'DROP INDEX I2B2DEMODATA.OF_CTX_BLOB';
		dbms_output.put_line(drop_sql);
		EXECUTE IMMEDIATE drop_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = '"&TM_WZ_SCHEMA"'
	AND index_name = 'IDX_WTN_LOAD_CLINICAL';
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX "&TM_WZ_SCHEMA"."IDX_WTN_LOAD_CLINICAL" ON "&TM_WZ_SCHEMA"."WT_TRIAL_NODES"
  	  (
    "LEAF_NODE",
    "CATEGORY_CD",
    "DATA_LABEL"
  	) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;

	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'I2B2DEMODATA'
	AND index_name = 'IDX_PD_SOURCESYSTEMCD_PNUM';
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX "I2B2DEMODATA"."IDX_PD_SOURCESYSTEMCD_PNUM" ON "I2B2DEMODATA"."PATIENT_DIMENSION"
  	  	(
   	   "SOURCESYSTEM_CD",
   	  "PATIENT_NUM"
 	 	)  TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = '&TM_CZ_SCHEMA'
	AND index_name = 'IDX_TMP_SUBJ_USUBJID';
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX "&TM_CZ_SCHEMA"."IDX_TMP_SUBJ_USUBJID" ON "&TM_CZ_SCHEMA"."TMP_SUBJECT_INFO "
  	  	(
    	  "USUBJID"
  		) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = '&TM_LZ_SCHEMA'
	AND index_name = 'IDX_SCD_STUDY';
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX "&TM_LZ_SCHEMA"."IDX_SCD_STUDY" ON "&TM_LZ_SCHEMA"."LT_SRC_CLINICAL_DATA"
  	  	(
    		"STUDY_ID"
  	  	)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = '&TM_CZ_SCHEMA'
	AND index_name = upper('CZ_JOB_AUDIT_JOBID_DATE');
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX "&TM_CZ_SCHEMA".CZ_JOB_AUDIT_JOBID_DATE ON "&TM_CZ_SCHEMA".CZ_JOB_AUDIT (JOB_ID, JOB_DATE) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'DEAPP'
	AND index_name = 'IX_DE_SUBJECT_SMPL_MPNG_MRNA';
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX DEAPP.IX_DE_SUBJECT_SMPL_MPNG_MRNA ON DEAPP.DE_SUBJECT_SAMPLE_MAPPING (TRIAL_NAME, PLATFORM, SOURCE_CD, CONCEPT_CODE) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'I2B2METADATA'
	AND index_name = 'IX_I2B2_SOURCE_SYSTEM_CD';
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX I2B2METADATA.IX_I2B2_SOURCE_SYSTEM_CD ON I2B2METADATA.I2B2 (SOURCESYSTEM_CD) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	alter_sql := 'ALTER TABLE "&TM_WZ_SCHEMA"."WRK_CLINICAL_DATA" CACHE';
	dbms_output.put_line(alter_sql);
	EXECUTE IMMEDIATE alter_sql;
	
	alter_sql := 'ALTER TABLE "&TM_WZ_SCHEMA"."WRK_CLINICAL_DATA" NOLOGGING';
	dbms_output.put_line(alter_sql);
	EXECUTE IMMEDIATE alter_sql;
	
	alter_sql := 'ALTER TABLE "&TM_LZ_SCHEMA"."LT_SRC_CLINICAL_DATA" CACHE';
	dbms_output.put_line(alter_sql);
	EXECUTE IMMEDIATE alter_sql;

	alter_sql := 'ALTER TABLE "&TM_LZ_SCHEMA"."LT_SRC_CLINICAL_DATA" NOLOGGING';
	dbms_output.put_line(alter_sql);
	EXECUTE IMMEDIATE alter_sql;
	
	-- NULLABLE?
	SELECT COUNT(*)
	INTO rows
	FROM dba_tab_cols
	WHERE owner = 'I2B2DEMODATA'
	 AND table_name = 'OBSERVATION_FACT'
	 AND column_name = 'INSTANCE_NUM';
	 
	IF rows < 1
	THEN
		alter_sql := 'ALTER TABLE i2b2demodata.observation_fact modify (instance_num null)';
		dbms_output.put_line(alter_sql);
		EXECUTE IMMEDIATE alter_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'I2B2DEMODATA'
	AND index_name = 'IDX_CCONCEPT_PATH';
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX i2b2demodata.IDX_CCONCEPT_PATH ON i2b2demodata.CONCEPT_COUNTS (CONCEPT_PATH) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;

	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'I2B2METADATA'
	AND index_name = 'IDX_I2B2_SECURE_FULLNAME';
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX I2B2METADATA.IDX_I2B2_SECURE_FULLNAME ON I2B2METADATA.I2B2_SECURE (C_FULLNAME) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'I2B2METADATA'
	AND index_name = 'IDX_I2B2_FULLNAME_BASECODE';
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX "I2B2METADATA"."IDX_I2B2_FULLNAME_BASECODE" ON "I2B2METADATA"."I2B2"
  	  	(
    		"C_FULLNAME",
    		"C_BASECODE"
  	  	) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = '&TM_WZ_SCHEMA'
	AND index_name = 'IDX_WT_TRIALNODES';
	
	if rows > 0
	THEN
		drop_sql := 'DROP INDEX "&TM_WZ_SCHEMA".IDX_WT_TRIALNODES';
		dbms_output.put_line(drop_sql);
		EXECUTE IMMEDIATE drop_sql;
	END IF;
	create_sql := 'CREATE INDEX "&TM_WZ_SCHEMA"."IDX_WT_TRIALNODES" ON "&TM_WZ_SCHEMA"."WT_TRIAL_NODES"
  		(
    		"LEAF_NODE",
    		"NODE_NAME"
  	  	) TABLESPACE "INDX"';
	dbms_output.put_line(create_sql);
	EXECUTE IMMEDIATE create_sql;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'DEAPP'
	AND index_name = 'IDX_DE_SUBJ_SMPL_TRIAL_CCODE';
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX "DEAPP"."IDX_DE_SUBJ_SMPL_TRIAL_CCODE" ON "DEAPP"."DE_SUBJECT_SAMPLE_MAPPING"
  	  					(
    					"TRIAL_NAME",
    					"CONCEPT_CODE"
  				  		) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = '&TM_WZ_SCHEMA'
	AND table_name = 'I2B2_LOAD_PATH';
	
	if rows < 1
	THEN
		create_sql := 'CREATE TABLE "&TM_WZ_SCHEMA"."I2B2_LOAD_PATH"
  	  					(
    						"PATH" VARCHAR2(700 BYTE),
    						"RECORD_ID" ROWID,
    						PRIMARY KEY ("PATH", "RECORD_ID")
  					  	)  
  					  ORGANIZATION INDEX NOLOGGING';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = '&TM_WZ_SCHEMA'
	AND table_name = 'I2B2_LOAD_TREE_FULL';
	
	if rows < 1
	THEN
		create_sql := 'CREATE TABLE "&TM_WZ_SCHEMA"."I2B2_LOAD_TREE_FULL"
  	  					(
    						"IDROOT"          ROWID,
    						"IDCHILD"         ROWID,
	  					  PRIMARY KEY("IDROOT", "IDCHILD")
  						)  
  					  ORGANIZATION INDEX NOLOGGING';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	create_sql := 'create or replace synonym "&TM_CZ_SCHEMA"."I2B2_LOAD_TREE_FULL" for "&TM_WZ_SCHEMA"."I2B2_LOAD_TREE_FULL"';
	dbms_output.put_line(create_sql);
	EXECUTE IMMEDIATE create_sql;
	
	create_sql := 'create or replace synonym "&TM_CZ_SCHEMA"."I2B2_LOAD_PATH" for "&TM_WZ_SCHEMA"."I2B2_LOAD_PATH"';
	dbms_output.put_line(create_sql);
	EXECUTE IMMEDIATE create_sql;

	create_sql := 'grant all privileges on "&TM_WZ_SCHEMA"."I2B2_LOAD_TREE_FULL" to "&TM_CZ_SCHEMA"';
	dbms_output.put_line(create_sql);
	EXECUTE IMMEDIATE create_sql;

	create_sql := 'grant all privileges on "&TM_WZ_SCHEMA"."I2B2_LOAD_PATH" to "&TM_CZ_SCHEMA"';
	dbms_output.put_line(create_sql);
	EXECUTE IMMEDIATE create_sql;

	---------------------------------------------
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = '&TM_WZ_SCHEMA'
	AND table_name = 'I2B2_LOAD_PATH_WITH_COUNT';
	
	IF rows > 0
	THEN
		drop_sql := 'DROP TABLE "&TM_WZ_SCHEMA".I2B2_LOAD_PATH_WITH_COUNT';
		dbms_output.put_line(drop_sql);
		EXECUTE IMMEDIATE drop_sql;
	END IF;
 
	create_sql := 'CREATE TABLE "&TM_WZ_SCHEMA"."I2B2_LOAD_PATH_WITH_COUNT"
	  (
	    "C_FULLNAME"          VARCHAR2(700 BYTE),
	    "NBR_CHILDREN"         NUMBER,
	    PRIMARY KEY(C_FULLNAME)
	  )  
	  ORGANIZATION INDEX NOLOGGING';
  	dbms_output.put_line(create_sql);
  	EXECUTE IMMEDIATE create_sql;
	
	grant_sql := 'grant all privileges on "&TM_WZ_SCHEMA"."I2B2_LOAD_PATH_WITH_COUNT" to "&TM_CZ_SCHEMA"';
	dbms_output.put_line(grant_sql);
	EXECUTE IMMEDIATE grant_sql;
	
	create_sql := 'create or replace synonym "&TM_CZ_SCHEMA"."I2B2_LOAD_PATH_WITH_COUNT" for "&TM_WZ_SCHEMA"';
  	dbms_output.put_line(create_sql);
  	EXECUTE IMMEDIATE create_sql;
 
	---- DEC 4, 2013 CHANGES --------------------

	drop_sql := 'DROP INDEX "I2B2DEMODATA"."IDX_OB_FACT_PATIENT_NUMBER"';
	dbms_output.put_line(drop_sql);
	EXECUTE IMMEDIATE drop_sql;

	create_sql := 'CREATE INDEX "I2B2DEMODATA"."IDX_OB_FACT_PATIENT_NUMBER" ON "I2B2DEMODATA"."OBSERVATION_FACT" ("PATIENT_NUM", "CONCEPT_CD") TABLESPACE "INDX"';
  	dbms_output.put_line(create_sql);
  	EXECUTE IMMEDIATE create_sql;

	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = '&TM_WZ_SCHEMA'
	  AND index_name = 'IDX_WRK_CD';
	  
	IF rows < 1
	THEN
		create_sql := 'CREATE INDEX "&TM_WZ_SCHEMA"."IDX_WRK_CD" 
			ON "&TM_WZ_SCHEMA"."WRK_CLINICAL_DATA" (DATA_TYPE ASC, DATA_VALUE ASC, VISIT_NAME ASC, DATA_LABEL ASC, CATEGORY_CD ASC, USUBJID ASC) TABLESPACE "INDX"';
  	  	dbms_output.put_line(create_sql);
  		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	-- JUL 23, 2014 CHANGES --------------------
	-- changes moved to file 20140724000004000_SNP_CREATE_TABLES.sql.
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'I2B2DEMODATA'
	AND index_name = 'QT_PATIENT_SET_COLLECTION_IDX1';

	if rows < 1 
	THEN
		create_sql := 'create index I2B2DEMODATA.QT_PATIENT_SET_COLLECTION_IDX1 on I2B2DEMODATA.QT_PATIENT_SET_COLLECTION(RESULT_INSTANCE_ID, PATIENT_NUM) TABLESPACE "INDX"'; 
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql; 
	END IF;

	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'BIOMART'
	AND index_name = upper('bio_marker_correl_mv_abm_idx');
	
	if rows < 1
	THEN
		create_sql := 'create index BIOMART.bio_marker_correl_mv_abm_idx on BIOMART.bio_marker_correl_mv(asso_bio_marker_id)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;

	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'SEARCHAPP'
	AND index_name = upper('search_keyword_uid_idx');
	
	if rows < 1
	THEN
		create_sql := 'create index searchapp.search_keyword_uid_idx on searchapp.search_keyword(unique_id) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;

	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'DEAPP'
	AND index_name ='DE_SNP_CALLS_BY_GSM_PN_GN_IDX';
	
	if rows < 1
	THEN
		create_sql := 'create index deapp.DE_SNP_CALLS_BY_GSM_PN_GN_IDX on deapp.de_snp_calls_by_gsm(patient_num, gsm_num) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;

	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'DEAPP'
	AND index_name = upper('de_snp_calls_by_gsm_sn_nm_idx');
	
	if rows < 1
	THEN
		create_sql := 'create index deapp.de_snp_calls_by_gsm_sn_nm_idx on deapp.de_snp_calls_by_gsm(snp_name) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'DEAPP'
	AND index_name = 'IDX_DE_CHROMOSAL_REGION';
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX DEAPP.IDX_DE_CHROMOSAL_REGION ON DEAPP.de_chromosomal_region (GPL_ID, GENE_SYMBOL) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;

	SELECT COUNT(*)
	INTO rows
	FROM dba_constraints
	WHERE owner = 'I2B2METADATA'
	AND table_name = 'I2B2_SECURE'
	and constraint_name = upper('i2b2_SECURE_uk');
	
	IF rows < 1
	THEN
		alter_sql := 'alter table i2b2metadata.i2b2_SECURE add constraint i2b2_SECURE_uk unique (c_fullname)';
		dbms_output.put_line(alter_sql);
		EXECUTE IMMEDIATE alter_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'I2B2DEMODATA'
	and table_name = 'CONCEPT_COUNTS'
	and index_name = 'CONCEPT_COUNTS_I_PCP';
	
	IF rows < 1
	THEN
		create_sql := 'create index I2B2DEMODATA.CONCEPT_COUNTS_I_PCP on I2B2DEMODATA.CONCEPT_COUNTS(parent_concept_path)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
 	EXCEPTION
		WHEN OTHERS THEN
		dbms_output.put_line(source || ':' || SQLERRM);
END;
/	
update amapp.am_tag_template set ACTIVE_IND= '1' where ACTIVE_IND= 't';

update amapp.am_tag_template set ACTIVE_IND= '0' where ACTIVE_IND= 'f';

update amapp.am_tag_item set ACTIVE_IND= '1' where ACTIVE_IND= 't';

update amapp.am_tag_item set ACTIVE_IND= '0' where ACTIVE_IND= 'f';

declare
  l_cons varchar2(4000);
  l_col varchar2(4000);
  l_owner varchar2(4000);
  l_sql varchar2(4000);
  l_table varchar2(4000);
  source VARCHAR2(100) := 'Update Constraints';
begin
  for i in (
    select owner, constraint_name, table_name
      from dba_constraints
     where     r_owner= 'SEARCHAPP'
           and constraint_type= 'R'
           and delete_rule= 'NO ACTION'
           and r_constraint_name in (
                   select constraint_name
                     from dba_constraints
                    where     constraint_type= 'P'
                          and owner= 'SEARCHAPP'
                          and table_name= 'SEARCH_SECURE_OBJECT'
               )
  ) loop
    l_cons := i.constraint_name;
    l_col := null;
    l_owner := i.owner;
    l_table := i.table_name;
    for j in (
      select column_name
        from dba_cons_columns
       where     owner= l_owner
             and constraint_name= l_cons
             and table_name= l_table
    )
    loop
      if l_col is not null then
        raise TOO_MANY_ROWS;
      end if;
      l_col := j.column_name;
    end loop;
    if l_col is null then
      raise NO_DATA_FOUND;
    end if;
    l_sql := 'alter table '||l_owner||'.'||l_table||' drop constraint '||l_cons;
    dbms_output.put_line(l_sql);
    execute immediate l_sql;
    l_sql := 'alter table '||l_owner||'.'||l_table||' add constraint '||l_cons||' foreign key('||l_col||') references SEARCHAPP.SEARCH_SECURE_OBJECT on delete cascade';
    dbms_output.put_line(l_sql);
    execute immediate l_sql;
  end loop;
exception
	when others then
  	dbms_output.put_line(source || SQLERRM);
  	raise;
end;
/

