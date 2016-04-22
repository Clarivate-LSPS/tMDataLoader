-- ------------------------------------------------
-- CREATE_INDEXES_FOR_CLINICAL_DATA.sql
-- ------------------------------------------------
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 180

DECLARE
	source VARCHAR2(100) := 'CREATE_INDEXES_FOR_CLINICAL_DATA.sql';
	create_sql VARCHAR2(2000);
	alter_sql VARCHAR2(2000);
	rows INT;
	
BEGIN

	SELECT COUNT(*)
	INTO rows
	FROM dba_tab_columns
	WHERE owner = 'I2B2METADATA'
	AND table_name = 'I2B2'
	And column_name = 'RECORD_ID';
	
	if rows < 1
	THEN
		alter_sql := 'ALTER TABLE I2B2Metadata.i2b2 ADD RECORD_ID NUMBER';
		create_sql := 'CREATE INDEX I2B2METADATA.I2B2META_IDX_RECORD_ID ON I2B2Metadata.i2b2 (RECORD_ID)';
		dbms_output.put_line(alter_sql);
		EXECUTE IMMEDIATE alter_sql;
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'I2B2DEMODATA'
	AND index_name = upper('idx_concept_path');

	if rows < 1
	THEN
		create_sql := 'CREATE INDEX i2b2demodata.idx_concept_path ON i2b2demodata.concept_counts(concept_path)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'I2B2DEMODATA'
	AND index_name = upper('idx_pd_sourcesystemcd_pnum');
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX I2B2DEMODATA.idx_pd_sourcesystemcd_pnum ON i2b2demodata.patient_dimension(sourcesystem_cd, patient_num)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'I2B2METADATA'
	AND index_name = upper('IX_I2B2_SOURCE_SYSTEM_CD');
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX I2B2METADATA.IX_I2B2_SOURCE_SYSTEM_CD ON i2b2metadata.i2b2(sourcesystem_cd)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'I2B2METADATA'
	AND index_name = upper('IDX_I2B2_BASECODE');
	
	if rows < 1
	THEN
		create_sql := 'CREATE I2B2METADATA.INDEX IDX_I2B2_BASECODE ON i2b2metadata.i2b2(c_basecode, record_id, c_visualattributes)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'DEAPP'
	AND index_name = upper('IDX_DE_SUBJ_SMPL_TRIAL_CCODE');
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX DEAPP.IDX_DE_SUBJ_SMPL_TRIAL_CCODE ON DEAPP.DE_SUBJECT_SAMPLE_MAPPING(TRIAL_NAME, CONCEPT_CODE)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'I2B2METADATA'
	AND index_name = upper('IDX_I2B2_SECURE_FULLNAME');
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX I2B2METADATA.IDX_I2B2_SECURE_FULLNAME ON I2B2METADATA.I2B2_SECURE(C_FULLNAME)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'I2B2DEMODATA'
	AND index_name = upper('IDX_FACT_PATIENT_NUM');
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX IDX_FACT_PATIENT_NUM ON I2B2DEMODATA.OBSERVATION_FACT(PATIENT_NUM)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	

EXCEPTION
	WHEN OTHERS THEN
	dbms_output.put_line(source || ':' || SQLERRM);
END;
/	