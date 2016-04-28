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
      dataset_id VARCHAR2(50 BYTE) NOT NULL PRIMARY KEY,
      datasource_id VARCHAR2(200 BYTE),
      etl_id VARCHAR2(20 BYTE),
      etl_date date,
      genome VARCHAR2(50 BYTE) NOT NULL,
      metadata_comment CLOB,
      variant_dataset_type VARCHAR2(50 BYTE)
  		) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  	  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  		PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
 	   LOB ("METADATA_COMMENT") STORE AS BASICFILE (
  		 ENABLE STORAGE IN ROW CHUNK 8192
  	   NOCACHE LOGGING
  	   STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  	   PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT))';
		 dbms_output.put_line(create_sql);
		 EXECUTE IMMEDIATE create_sql;
	ELSE
		SELECT COUNT(*)
		INTO rows
		FROM dba_tab_columns
		WHERE owner = 'DEAPP'
		AND table_name = 'DE_VARIANT_DATASET'
		AND column_name = 'GENOME'
		AND NULLABLE = 'y';
		
		IF rows > 0
		THEN
			alter_sql := 'ALTER TABLE "DEAPP"."DE_VARIANT_DATASET" MODIFY ("GENOME" NOT NULL ENABLE)';
			dbms_output.put_line(alter_sql);
			EXECUTE IMMEDIATE alter_sql;
		end if;
	END IF;
	
	SELECT count(*)
	INTO rows
	FROM dba_indexes
	WHERE owner = 'DEAPP'
	and index_name = 'DE_VARIANT_DATASET_I_DS_ID';
	
	IF rows < 1
	THEN
 		create_sql := 'CREATE UNIQUE INDEX "DEAPP"."DE_VARIANT_DATASET_I_DS_ID" ON "DEAPP"."DE_VARIANT_DATASET" ("DATASET_ID")
  					PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
  					STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
					PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT) TABLESPACE "INDX"';
 		dbms_output.put_line(create_sql);
 	   EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_constraints
	WHERE table_name = 'DE_VARIANT_DATASET'
	and constraint_type = 'P';
	
	IF rows < 1
	THEN
		alter_sql := 'ALTER TABLE "DEAPP"."DE_VARIANT_DATASET" ADD PRIMARY KEY ("DATASET_ID") ENABLE
  	  		USING INDEX "DEAPP"."DE_VARIANT_DATASET_I_DS_ID"';
		dbms_output.put_line(alter_sql);
		EXECUTE IMMEDIATE alter_sql;
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
    CACHE 2';
	  	dbms_output.put_line(create_sql);
	  	EXECUTE IMMEDIATE create_sql;
	  END IF;
	
  	SELECT COUNT(*)
  	INTO rows
  	FROM dba_tables
  	WHERE owner = 'DEAPP'
  	  AND table_name = upper('de_variant_subject_idx');
	  
  	IF rows < 1 THEN
  		create_sql := 
  		'CREATE TABLE deapp.de_variant_subject_idx (
        dataset_id VARCHAR2(50 BYTE),
        subject_id VARCHAR2(50 BYTE),
        "position" number(10,0),
        variant_subject_idx_id number(18,0) DEFAULT deapp.de_variant_subject_idx_seq.NEXTVAL
    	) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  	  	STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  		  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)';
  		dbms_output.put_line(create_sql);
  		EXECUTE IMMEDIATE create_sql;
		
		create_sql := 'CREATE OR REPLACE TRIGGER deapp.de_var_sub_idx_incr
		BEFORE INSERT ON deapp.DE_VARIANT_SUBJECT_IDX
		FOR EACH ROW

		BEGIN
	 	 SELECT deapp.de_variant_population_info_seq.NEXTVAL
	 	 INTO   :new.VARIANT_SUBJECT_IDX_ID
	  	FROM   dual;
		END;';
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
    	PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
    	STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
    		PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
    		TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_constraints
	WHERE owner = 'DEAPP'
	AND table_name = 'DE_VARIANT_SUBJECT_DETAIL'
	AND constraint_type = 'P';
	
	IF rows < 1
	THEN
	alter_sql := 'ALTER TABLE "DEAPP"."DE_VARIANT_SUBJECT_DETAIL" ADD PRIMARY KEY ("VARIANT_SUBJECT_DETAIL_ID")
  		USING INDEX PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
  	  	STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  		PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
  	  	ENABLE';
		dbms_output.put_line(alter_sql);
		EXECUTE IMMEDIATE alter_sql;	
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_constraints
	WHERE owner = 'DEAPP'
	AND table_name = upper('de_variant_subject_idx')
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
 		CACHE 2';
	  dbms_output.put_line(create_sql);
	  EXECUTE IMMEDIATE create_sql;
	END IF;
	
	SELECT COUNT(*)
	INTO rows
	FROM dba_tables
	WHERE owner = 'DEAPP'
	AND table_name = 'DE_VARIANT_SUBJECT_SUMMARY';
	
	IF rows < 1
	THEN
		create_sql := 'CREATE TABLE "DEAPP"."DE_VARIANT_SUBJECT_SUMMARY"
		(	"VARIANT_SUBJECT_SUMMARY_ID" NUMBER(9,0),
		"CHR" VARCHAR2(50 BYTE),
		"POS" NUMBER(20,0),
		"DATASET_ID" VARCHAR2(50 BYTE),
		"SUBJECT_ID" VARCHAR2(50 BYTE),
		"RS_ID" VARCHAR2(50 BYTE),
		"VARIANT" VARCHAR2(100 BYTE),
		"VARIANT_FORMAT" VARCHAR2(100 BYTE),
		"VARIANT_TYPE" VARCHAR2(100 BYTE),
		"REFERENCE" VARCHAR2(100),
        "ALLELE1" integer,
        "ALLELE2" integer,
        "ASSAY_ID" NUMBER(10,0)
   		) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  	  	STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  		PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)';
  	  	dbms_output.put_line(create_sql);
  	  	EXECUTE IMMEDIATE create_sql;
		
		create_sql := 'CREATE OR REPLACE TRIGGER deapp.de_var_sub_summary_incr
		BEFORE INSERT ON  "DEAPP"."DE_VARIANT_SUBJECT_SUMMARY"
		FOR EACH ROW

		BEGIN
	  		SELECT deapp.de_variant_subject_summary_seq.NEXTVAL
	  		INTO   :new.VARIANT_SUBJECT_SUMMARY_ID
	  		FROM   dual;
		END;';
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
	 ELSE
	 	SELECT COUNT(*)
	 	INTO rows
		FROM dba_tab_columns
		WHERE owner = 'DEAPP'
		and table_name = 'DE_VARIANT_SUBJECT_SUMMARY'
		and column_name = 'DATASET_ID'
		and nullable = 'y';
		
		IF rows > 0
		THEN
			alter_sql := 'ALTER TABLE "DEAPP"."DE_VARIANT_SUBJECT_SUMMARY" MODIFY ("DATASET_ID" NOT NULL ENABLE)';
			dbms_output.put_line(alter_sql);
			EXECUTE IMMEDIATE alter_sql;
		END IF;
		
	 	SELECT COUNT(*)
	 	INTO rows
		FROM dba_tab_columns
		WHERE owner = 'DEAPP'
		and table_name = 'DE_VARIANT_SUBJECT_SUMMARY'
		and column_name = 'SUBJECT_ID'
		and nullable = 'y';
		
		IF rows > 0
		THEN
			alter_sql := 'ALTER TABLE "DEAPP"."DE_VARIANT_SUBJECT_SUMMARY" MODIFY ("SUBJECT_ID" NOT NULL ENABLE)';
			dbms_output.put_line(alter_sql);
			EXECUTE IMMEDIATE alter_sql;
		END IF;
		
		
	 END IF;
	 
	 SELECT COUNT(*)
	 INTO rows
	 FROM dba_constraints
	 WHERE OWNER = 'DEAPP'
	 AND table_name = upper('variant_subject_summary')
	 AND constraint_type = 'P';
	 
	 IF rows < 1 THEN
	 	create_sql := 'CREATE UNIQUE INDEX "DEAPP"."DE_VSS_I_VSS_ID" ON "DEAPP"."DE_VARIANT_SUBJECT_SUMMARY" ("VARIANT_SUBJECT_SUMMARY_ID")
  	  		PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
  		  	STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  			PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT) TABLESPACE "INDX"';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
		
		alter_sql := 'ALTER TABLE deapp.de_variant_subject_summary
  	   	 ADD CONSTRAINT variant_subject_summary_id PRIMARY KEY (variant_subject_summary_id)
		 USING INDEX "DEAPP"."DE_VSS_I_VSS_ID"';
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
	FROM dba_indexes
	WHERE owner = 'DEAPP'
	AND index_name = 'VARIANT_SUBJECT_SUMMARY_UK';
	
	IF rows < 1
	THEN
		create_sql := 'CREATE INDEX "DEAPP"."VARIANT_SUBJECT_SUMMARY_UK" ON "DEAPP"."DE_VARIANT_SUBJECT_SUMMARY" ("DATASET_ID", "CHR", "POS", "RS_ID", "SUBJECT_ID")
  	  		PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
  		  	STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  			PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT) TABLESPACE "INDX"';
	  	dbms_output.put_line(create_sql);
	  	EXECUTE IMMEDIATE create_sql;
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
      variant_subject_detail_id NUMBER(9,0) DEFAULT deapp.de_variant_subject_detail_seq.NEXTVAL NOT NULL,
      dataset_id VARCHAR2(50 BYTE),
      chr VARCHAR2(50 BYTE),
      pos NUMBER(20,0),
      rs_id VARCHAR2(50 BYTE),
      ref VARCHAR2(500 BYTE), -- Same as in postgres
      alt VARCHAR2(500 BYTE), -- Same as in postgres
      qual VARCHAR2(100 BYTE),
      filter VARCHAR2(50 BYTE),
      info VARCHAR2(4000 BYTE), - Maximum size
      format VARCHAR2(500 BYTE), - Same as postgres
      variant_value CLOB
  	) PCTFREE 10 PCTUSED 40 INITRANS 1 MAXTRANS 255 NOCOMPRESS LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
 LOB ("VARIANT_VALUE") STORE AS BASICFILE (
  ENABLE STORAGE IN ROW CHUNK 8192
  NOCACHE LOGGING
  STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT))
  CACHE';

		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
		
		create_sql := 'CREATE OR REPLACE TRIGGER deapp.DE_VAR_SUB_DETAIL_incr
		BEFORE INSERT ON deapp.DE_VARIANT_SUBJECT_DETAIL
		FOR EACH ROW

		BEGIN
	 		SELECT deapp.DE_VARIANT_SUBJECT_DETAIL_seq.NEXTVAL
	  	  	INTO   :new.VARIANT_SUBJECT_DETAIL_ID
	  	  	FROM   dual;
		END;	';
		dbms_output.put_line(create_sql);
		EXECUTE IMMEDIATE create_sql;
		
		create_sql := 'CREATE UNIQUE INDEX "DEAPP"."DE_VSD_I_VSD_ID" ON "DEAPP"."DE_VARIANT_SUBJECT_DETAIL" ("VARIANT_SUBJECT_DETAIL_ID")
  	  		PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
  		  	STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  			PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)  TABLESPACE "INDX"';
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
  	  	(dataset_id, chr) PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
  	  	STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
  		  PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT) TABLESPACE "INDX"';
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
	    PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
	    STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
	    PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT) TABLESPACE "INDX"';
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
					    PCTFREE 10 INITRANS 2 MAXTRANS 255 COMPUTE STATISTICS
					    STORAGE(INITIAL 65536 NEXT 1048576 MINEXTENTS 1 MAXEXTENTS 2147483645
					    PCTINCREASE 0 FREELISTS 1 FREELIST GROUPS 1 BUFFER_POOL DEFAULT)
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
	  AND sequence_name = 'de_variant_population_data_seq';
	
  	if rows < 1
  	THEN
  		create_sql := 'CREATE SEQUENCE deapp.de_variant_population_data_seq
      		START WITH 1
      	  	INCREMENT BY 1
      		NOMINVALUE
      		NOMAXVALUE
      		CACHE 2;';
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
      variant_population_data_id NUMBER DEFAULT deapp.de_variant_population_data_seq.NEXTVAL NOT NULL,
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
		
		create_sql := 'CREATE OR REPLACE TRIGGER deapp.de_var_pop_data_incr
						BEFORE INSERT ON deapp.de_variant_population_data
						FOR EACH ROW

						BEGIN
  					  SELECT deapp.de_variant_population_data_seq.NEXTVAL
  					  INTO   :new.variant_population_data_id
  				  	  FROM   dual;
					 END;';
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
	and constraint_name = upper('de_variant_population_data_fk');
	
	if rows < 1
	THEN
		alter_sql := 'ALTER TABLE deapp.de_variant_population_data
      		ADD CONSTRAINT deapp.de_variant_population_data_fk FOREIGN KEY (dataset_id) REFERENCES deapp.de_variant_dataset(dataset_id)';
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
      CACHE 2';
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
      	  variant_population_info_id number(10,0) DEFAULT deapp.de_variant_population_info_seq.NEXTVAL NOT NULL,
      	dataset_id VARCHAR2(50),
      	info_name VARCHAR2(100),
      	description CLOB,
      	type VARCHAR2(30),
      	number VARCHAR2(10)
  		)';
  		dbms_output.put_line(create_sql);
  		EXECUTE IMMEDIATE create_sql;
		
		create_sql := 'CREATE OR REPLACE TRIGGER deapp.de_var_pop_info_incr
			BEFORE INSERT ON deapp.de_variant_population_info
			FOR EACH ROW

			BEGIN
	  	  SELECT deapp.de_variant_population_info_seq.NEXTVAL
	  	  INTO   :new.variant_population_info_id
	  	  FROM   dual;
		  END;';
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
 

