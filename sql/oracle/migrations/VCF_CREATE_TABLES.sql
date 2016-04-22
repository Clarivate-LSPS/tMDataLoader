-- ---------------------
-- VCF_CREATE_TABLES.sql
-- ---------------------
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 180
SET HEADING OFF

DECLARE
	rows int;
	create_sql varchar2(2000);
	alter_sql VARCHAR2(2000);
	source VARCHAR2(100) := 'VCF_CREATE_TABLES.sql';
BEGIN
	dbms_output.put_line(source);
	dbms_output.put_line('Creating VCF tables');

	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = 'DEAPP'
	  AND table_name = upper('de_variant_dataset');
	  
	IF rows < 1 THEN
		 create_sql := 
		 'CREATE TABLE deapp.de_variant_dataset (
      dataset_id VARCHAR2(50) NOT NULL PRIMARY KEY,
      datasource_id VARCHAR2(200),
      etl_id VARCHAR2(20),
      etl_date date,
      genome character VARCHAR2(50) NOT NULL,
      metadata_comment CLOB,
      variant_dataset_type VARCHAR2(50)
  ) ';
		 dbms_output.put_line(create_sql);
		 EXECUTE IMMEDIATE create_sql;
	END IF;
	
	--
	-- Name: de_variant_subject_idx_seq; Type: SEQUENCE; Schema: deapp; Owner: -
	--
	SELECT COUNT(*)
	INTO rows
	FROM dba_sequences
	WHERE sequence_owner = 'DEAPP'
	  AND sequence_name = upper('de_variant_subject_idx_seq');
	  
	  IF rows < 1THEN
	  	create_sql := 
		'CREATE SEQUENCE deapp.de_variant_subject_idx_seq
    START WITH 1
    INCREMENT BY 1
    NOMINVALUE
    NOMAXVALUE
    CACHE 1';
	  	dbms_output.put_line(create_sql);
	  	EXECUTE IMMEDIATE create_sql;
	  END IF;
	
	  --
	  -- Name: variant_subject_idx_uk; Type: INDEX; Schema: deapp; Owner: -
	  --
  	SELECT COUNT(*)
  	INTO rows
  	FROM dba_tables
  	WHERE owner = 'DEAPP'
  	  AND table_name = upper('de_variant_subject_idx');
	  
  	IF rows < 1 THEN
  		create_sql := 
  		'CREATE TABLE deapp.de_variant_subject_idx (
        dataset_id VARCHAR2(50),
        subject_id VARCHAR2(50),
        "position" number,
        variant_subject_idx_id number DEFAULT deapp.de_variant_subject_idx_seq.NEXTVAL
    )';
  		dbms_output.put_line(create_sql);
  		EXECUTE IMMEDIATE create_sql;
  	END IF;

  	SELECT COUNT(*)
  	INTO rows
  	FROM dba_indexes
  	WHERE owner = 'DEAPP'
  	  AND table_name = upper('de_variant_subject_idx')
  	 AND index_name = upper('variant_subject_idx_uk');
	 
	 IF rows < 1 THEN
	 	create_sql := 
		'CREATE UNIQUE INDEX variant_subject_idx_uk
    ON deapp.de_variant_subject_idx (dataset_id, subject_id, position)
    TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_constraints
	WHERE owner = 'DEAPP'
	and constraint_name = upper('variant_subject_idx_fk');
	
	IF rows < 1 THEN
		alter_sql := ' ALTER TABLE deapp.de_variant_subject_idx
      		ADD CONSTRAINT variant_subject_idx_fk FOREIGN KEY (dataset_id) REFERENCES deapp.de_variant_dataset(dataset_id)';
		dbms_output.put_line(alter_sql);
		EXECUTE IMMEDIATE alter_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_sequences
	WHERE sequence_owner = 'DEAPP'
	  AND SEQUENCE_NAME = upper('de_variant_subject_summary_seq');
	IF rows < 1 THEN
	  create_sql := 
	  'CREATE SEQUENCE deapp.de_variant_subject_summary_seq
 START WITH 1
 INCREMENT BY 1
 NOMINVALUE
 NOMAXVALUE
 CACHE 1';
	  dbms_output.put_line(create_sql);
	  EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = 'DEAPP'
	  AND table_name = upper('de_variant_dataset');
	  
	IF rows < 1 THEN
	  	create_sql := 
		'CREATE TABLE deapp.de_variant_subject_summary (
      variant_subject_summary_id NUMBER DEFAULT deapp.de_variant_subject_summary_seq.NEXTVAL NOT NULL,
      chr VARCHAR2(50),
      pos NUMBER,
      dataset_id VARCHAR2(50) NOT NULL,
      subject_id VARCHAR2(50) NOT NULL,
      rs_id VARCHAR2(50),
      variant VARCHAR2(1000),
      variant_format VARCHAR2(100),
      variant_type VARCHAR2(100),
      reference BIT,
      allele1 integer,
      allele2 integer,
      assay_id NUMBER;
  )';
	  	dbms_output.put_line(create_sql);
	  	EXECUTE IMMEDIATE create_sql;
	 END IF;
	 
	 SELECT COUNT(*)
	 INTO rows
	 FROM dba_constraints
	 WHERE OWNER = 'DEAPP'
	 AND table_name = upper('variant_subject_summary')
	 AND constraint_type = 'P';
	 
	 IF rows < 1 THEN

		 alter_sql := 'ALTER TABLE deapp.de_variant_subject_summary
  	   	 ADD CONSTRAINT variant_subject_summary_id PRIMARY KEY (variant_subject_summary_id)';
		 dbms_output.put_line(alter_sql);
		 EXECUTE IMMEDIATE alter_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_constraints
	WHERE owner = 'DEAPP'
	AND table_name = upper('de_variant_subject_summary')
	AND constraint_type = 'R';
	
	IF rows < 1 THEN
		alter_sql := 'ALTER TABLE  deapp.de_variant_subject_summary
                  ADD CONSTRAINT deapp.variant_subject_summary_fk FOREIGN KEY (dataset_id) REFERENCES deapp.de_variant_dataset(dataset_id)';
		dbms_output.put_line(alter_sql);
		EXECUTE IMMEDIATE alter_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_sequences
	WHERE sequence_owner = 'DEAPP'
	AND sequence_name = upper('de_variant_subject_detail_seq');
	
	IF rows < 1 THEN
		create_sql := 'CREATE SEQUENCE deapp.de_variant_subject_detail_seq
      START WITH 1
      INCREMENT BY 1
      NO MINVALUE
      NO MAXVALUE
      CACHE 1;';
	  dbms_output.put_line(create_sql);
	  EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = 'DEAPP'
	AND table_name = upper('de_variant_subject_detail');
	
	IF rows < 1 THEN
		create_sql := 'CREATE TABLE deapp.de_variant_subject_detail (
      variant_subject_detail_id NUMBER DEFAULT deapp.de_variant_subject_detail_seq.NEXTVAL NOT NULL,
      dataset_id VARCHAR2(50),
      chr VARCHAR2(50),
      pos NUMBER,
      rs_id VARCHAR2(50),
      ref VARCHAR2(500),
      alt VARCHAR2(500),
      qual VARCHAR2(100),
      filter VARCHAR2(50),
      info VARCHAR2(4000),
      format VARCHAR2(500),
      variant_value CLOB
  	)';

		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_constraints
	WHERE owner = 'DEAPP'
	AND table_name = upper('de_variant_subject_detail')
	AND constraint_type = 'P';
	
	IF rows < 1 THEN
		alter_sql := 'ALTER TABLE deapp.de_variant_subject_detail
     	 ADD CONSTRAINT variant_subject_detail_id PRIMARY KEY (variant_subject_detail_id)';
		 dbms_output.put_line(alter_sql);
		 EXECUTE IMMEDIATE alter_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_ind_columns
	WHERE index_owner = 'DEAPP'
	AND table_owner = 'DEAPP'
	AND table_name = upper('de_variant_subject_detail')
	AND (column_name = 'DATASET_ID' or column_name = 'CHR')
	AND index_name = upper('de_variant_sub_detail_idx2');
	
	IF rows < 1
	THEN
		create_sql := 'CREATE INDEX deapp.de_variant_sub_detail_idx2
  	  	ON deapp.de_variant_subject_detail
  	  	(dataset_id, chr)TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;

	SELECT COUNT(*)
	INTO rows
	FROM dba_ind_columns
	WHERE index_owner = 'DEAPP'
	AND table_owner = 'DEAPP'
	AND table_name = upper('de_variant_subject_detail')
	AND (column_name = upper('dataset_id') or column_name = upper('rs_id'))
	AND index_name = upper('de_variant_subject_detail');
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX de_variant_sub_dt_idx1
  	  	ON deapp.de_variant_subject_detail
  	  	(dataset_id, rs_id)
  	  	TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_ind_columns
	WHERE index_owner = 'DEAPP'
	AND table_owner = 'DEAPP'
	AND table_name = upper('variant_subject_detail')
	AND (column_name = upper('dataset_id') or column_name = upper('chr')
		 or column_name = upper('pos') or column_name = upper('rs_id'))
	AND index_name = upper('variant_subject_detail_uk');
	
	if rows < 1
	THEN
		create_sql := 'CREATE UNIQUE INDEX variant_subject_detail_uk
  	  					ON deapp.de_variant_subject_detail
  					  	(dataset_id, chr, pos, rs_id)
  					  	TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_constraints
	WHERE owner = 'DEAPP'
	and constraint_name = upper('variant_subject_detail_fk');

	if rows < 1
	THEN
		alter_sql := 'ALTER TABLE deapp.de_variant_subject_detail
      		ADD CONSTRAINT variant_subject_detail_fk FOREIGN KEY (dataset_id) REFERENCES deapp.de_variant_dataset(dataset_id)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_sequences
	WHERE sequence_owner = 'deapp'
	  AND sequence_name = 'de_var_pop_data_seq';
	
  	if rows < 1
  	THEN
  		create_sql := 'CREATE SEQUENCE deapp.de_var_pop_data_seq
      		START WITH 1
      	  	INCREMENT BY 1
      		NOMINVALUE
      		NOMAXVALUE
      		CACHE 1;';
  		dbms_output.put_line(create_sql);
  		EXECUTE IMMEDIATE create_sql;
  	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = 'DEAPP'
	and table_name = upper('de_variant_population_data');
	
	
	if rows < 1
	THEN
		create_sql := 'CREATE TABLE deapp.de_variant_population_data (
      variant_population_data_id NUMBER DEFAULT deapp.de_var_pop_data_seq.NEXTVAL NOT NULL,
      dataset_id VARCHAR2(50),
      chr VARCHAR2(50),
      pos NUMBER,
      info_name VARCHAR2(100),
      info_index integer DEFAULT 0,
      integer_value NUMBER,
      float_value double precision,
      text_value VARCHAR2(4000)
  		)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;

	SELECT COUNT(*)
	INTO rows
	FROM dba_constraints
	WHERE owner = 'DEAPP'
	AND table_name = upper('de_variant_population_data')
	AND constraint_type ='P';

	if rows < 1
	THEN
		alter_sql := 'ALTER TABLE deapp.de_variant_population_data
      		ADD CONSTRAINT deapp.de_var_pop_data_id_idx PRIMARY KEY (variant_population_data_id)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;	


	SELECT COUNT(*)
	INTO rows
	FROM dba_ind_columns
	WHERE index_owner = 'DEAPP'
	AND table_owner = 'DEAPP'
	AND table_name = upper('de_variant_population_data')
	AND (column_name = upper('dataset_id') or column_name = upper('chr')
		 or column_name = upper('pos') or column_name = upper('info_name'))
	or index_name = upper('de_var_pop_data_default_idx');
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX deapp.de_var_pop_data_default_idx
  	  		ON deapp.de_variant_population_data
  		  (dataset_id, chr, pos, info_name)
  		  TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_constraints
	WHERE owner = 'DEAPP'
	and constraint_name = upper('de_var_pop_data_fk');
	
	if rows < 1
	THEN
		alter_sql := 'ALTER TABLE deapp.de_variant_population_data
      		ADD CONSTRAINT deapp.de_var_pop_data_fk FOREIGN KEY (dataset_id) REFERENCES deapp.de_variant_dataset(dataset_id)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;

	SELECT COUNT(*)
	INTO rows
	FROM dba_sequences
	WHERE sequence_owner = 'deapp'
	  AND sequence_name = 'de_variant_population_info_seq';
	  
	IF rows < 1 THEN
	  	create_sql := 
	  'CREATE SEQUENCE deapp.de_variant_population_info_seq
      START WITH 1
      INCREMENT BY 1
      NOMINVALUE
      NOMAXVALUE
      CACHE 1';
	  dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = 'DEAPP'
	AND table_name = upper('de_variant_population_info');
	
	IF rows < 1 THEN
		create_sql := 
	'CREATE TABLE deapp.de_variant_population_info (
      variant_population_info_id bigint DEFAULT deapp.de_variant_population_info_seq.NEXTVAL NOT NULL,
      dataset_id VARCHAR2(50),
      info_name VARCHAR2(100),
      description CLOB,
      type VARCHAR2(30),
      number VARCHAR2(10)
  )';
  		dbms_output.put_line(create_sql);
  		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_constraints
	WHERE owner = 'DEAPP'
	and table_name = upper('de_variant_population_info')
	and constraint_type = 'P';
	
	if rows < 1
	THEN
		alter_sql := 'ALTER TABLE deapp.de_variant_population_info
      		ADD CONSTRAINT deapp.de_var_pop_info_id_idx PRIMARY KEY (variant_population_info_id)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'DEAPP'
	AND table_name = upper('de_variant_population_info')
	and index_name = upper('var_pop_inf_data_nm');
	
	if rows < 1
	THEN
		create_sql := 'CREATE INDEX deapp.var_pop_inf_data_nm
  	  		ON deapp.de_variant_population_info
  		  	(dataset_id, info_name)
  		  	TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;

	SELECT COUNT(*)
	INTO rows
	FROM dba_constraints
	WHERE owner = 'DEAPP'
	AND index_name = upper('de_var_pop_info_fk');
	
	if rows < 1
	THEN
		alter_sql := 'ALTER TABLE  deapp.de_variant_population_info
      		ADD CONSTRAINT deapp.de_var_pop_info_fk FOREIGN KEY (dataset_id) REFERENCES deapp.de_variant_dataset(dataset_id)';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line(source || SQLERRM);
END;
/	 
  --
  -- Name: COLUMN de_variant_subject_summary.reference; Type: COMMENT; Schema: deapp; Owner: -
  --
  COMMENT ON COLUMN deapp.de_variant_subject_summary.reference
  IS 'This column contains a flag whether this subject has a reference value on this variant, or not.';

  COMMENT ON COLUMN deapp.de_variant_subject_summary.assay_id
  IS 'Reference to deapp.de_subject_sample_mapping';
 

