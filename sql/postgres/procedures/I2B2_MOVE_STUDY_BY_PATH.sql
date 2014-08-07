--DROP FUNCTION i2b2_move_study_by_path(character varying,character varying,numeric);

CREATE OR REPLACE
FUNCTION I2B2_MOVE_STUDY_BY_PATH
  (old_path_in  CHARACTER VARYING,
   new_path_in  CHARACTER VARYING,
   currentJobID NUMERIC DEFAULT -1
  )
  RETURNS INTEGER AS
  $BODY$

  DECLARE
--Audit variables
    newJobFlag              INTEGER;
    databaseName            VARCHAR(100);
    procedureName           VARCHAR(100);
    jobID                   NUMERIC(18, 0);
    stepCt                  NUMERIC(18, 0);
    tText                   VARCHAR(2000);
    rtnCd                   integer;
    rowCt			              numeric(18,0);
	  errorNumber		          character varying;
	  errorMessage	          character varying;

    old_path                VARCHAR(2000);
    new_path                VARCHAR(2000);
    old_root_node           VARCHAR(2000);
    new_root_node           VARCHAR(2000);
    new_root_node_name      VARCHAR(2000);
    new_path_last_node_name VARCHAR(2000);
    rowsExists              INTEGER;
    counter                 INTEGER;
    lvl_num_to_remove       INTEGER;
    old_level_num           INTEGER;
    new_level_num           INTEGER;
    parent_path_node        VARCHAR(2000);
    current_path            VARCHAR(2000);
    current_path_level      INTEGER;
    current_path_attr_name  VARCHAR(2000);
    tmp                     VARCHAR(2000);

  BEGIN

--Audit JOB Initialization
    stepCt := 0;

--Set Audit Parameters
    newJobFlag := 0; -- False (Default)

    jobID := currentJobID;
    databaseName := current_schema();
    procedureName := 'I2B2_MOVE_STUDY_BY_PATH';

--If Job ID does not exist, then this is a single procedure run and we need to create it
    IF (jobID IS NULL OR jobID < 1)
    THEN
      newJobFlag := 1; -- True
      select cz_start_audit (procedureName, databaseName) into jobID;
    END IF;

    stepCt := 0;
    stepCt := stepCt + 1;
    tText := 'Start i2b2_move_study_by_path from ' || old_path || ' to ' || new_path;
    select cz_write_audit(jobId,databaseName,procedureName,'Starting i2b2_process_snp_data',0,stepCt,'Done') into rtnCd;




    old_path := trim(old_path_in);
    new_path := trim(new_path_in);

    IF old_path = '' or new_path=''
    THEN
      stepCt := stepCt + 1;
      select cz_write_audit(jobId,databaseName,procedureName,'New or old path is empty. Please check input parameters',0,stepCt,'Done') into rtnCd;
      select cz_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
      select cz_end_audit (jobID, 'FAIL') into rtnCd;
      return -16;
    END IF;

    -- check duplicates
    IF old_path = new_path
    THEN
      stepCt := stepCt + 1;
      select cz_write_audit(jobId,databaseName,procedureName,'Please select different old and new paths',0,stepCt,'Done') into rtnCd;
      select cz_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
      select cz_end_audit (jobID, 'FAIL') into rtnCd;
      return -16;
    END IF;
    -- check old root node exists
    SELECT
      count(*)
    INTO rowsExists
    FROM i2b2metadata.i2b2
    WHERE c_fullname = old_path;

    IF rowsExists = 0
    THEN
      stepCt := stepCt + 1;
      select cz_write_audit(jobId,databaseName,procedureName,'Please select exists study path to move',0,stepCt,'Done') into rtnCd;
      select cz_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
      select cz_end_audit (jobID, 'FAIL') into rtnCd;
      return -16;
    END IF;


-- check first and last /
    SELECT
      substring (old_path FROM 1 FOR 1)
    INTO tmp;

    IF tmp <> '\'
    THEN
      old_path := '\' || old_path;
    END IF;

    SELECT
      substring(new_path FROM 1 FOR 1)
    INTO tmp;

    IF tmp <> '\'
    THEN
      new_path := '\' || new_path;
    END IF;

    SELECT
      substring(old_path FROM '.{1}$')
    INTO tmp;

    IF tmp <> '\'
    THEN
      old_path := old_path || '\';
    END IF;

    SELECT
      substring(new_path FROM '.{1}$')
    INTO tmp;

    IF tmp <> '\'
    THEN
      new_path := new_path || '\';
    END IF;

    old_root_node := REGEXP_REPLACE(old_path, '(\\(\w|\s)*\\)(.*)', '\1');
    new_root_node := REGEXP_REPLACE(new_path, '(\\(\w|\s)*\\)(.*)', '\1');
    new_root_node_name := REGEXP_REPLACE(new_path, '\\((\w|\s)*)\\(.*)', '\1');
    new_path_last_node_name := REGEXP_REPLACE(new_path, '(.*)\\((\w|\s)*)\\', '\2');


-- check new root node exists

    SELECT
      count(*)
    INTO rowsExists
    FROM i2b2metadata.i2b2
    WHERE c_fullname = new_root_node;

    IF rowsExists = 0
    THEN
-- create new root in table_access,
      begin
      INSERT INTO i2b2metadata.table_access
        SELECT
          new_root_node_name  AS c_table_cd,
          'i2b2'              AS c_table_name,
          'N'                 AS protected_access,
          0                   AS c_hlevel,
          new_root_node       AS c_fullname,
          new_root_node_name  AS c_name,
          'N'                 AS c_synonym_cd,
          'CA'                AS c_visualattributes,
          null                AS c_totalnum,
          null                AS c_basecode,
          null                AS c_metadataxml,
          'concept_cd'        AS c_facttablecolumn,
          'concept_dimension' AS c_dimtablename,
          'concept_path'      AS c_columnname,
          'T'                 AS c_columndatatype,
          'LIKE'              AS c_operator,
          new_root_node       AS c_dimcode,
          null                AS c_comment,
          new_root_node       AS c_tooltip,
          current_timestamp   AS c_entry_date,
          null                AS c_change_date,
          null                AS c_status_cd,
          null                AS valuetype_cd
        WHERE NOT exists
        (SELECT
           1
         FROM i2b2metadata.table_access x
         WHERE x.c_table_cd = new_root_node);

      get diagnostics rowCt := ROW_COUNT;
      exception
        when others then
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
          --End Proc
          select cz_end_audit (jobID, 'FAIL') into rtnCd;
          return -16;
      end;

      stepCt := stepCt + 1;
      select cz_write_audit(jobId, databaseName, procedureName, 'Create new root node in table_access', rowCt, stepCt,
                     'Done')  into rtnCd;


-- create new root in i2b2
      begin
      INSERT INTO i2b2metadata.i2b2 (
        C_HLEVEL,
        C_FULLNAME,
        UPDATE_DATE,
        C_DIMCODE,
        C_TOOLTIP,
        C_NAME,
        C_SYNONYM_CD,
        C_VISUALATTRIBUTES,
        C_FACTTABLECOLUMN,
        C_TABLENAME,
        C_COLUMNNAME,
        C_COLUMNDATATYPE,
        C_OPERATOR,
        M_APPLIED_PATH
      )
        VALUES (0,
                new_root_node,
                current_timestamp,
                new_root_node,
                new_root_node,
                new_root_node_name,
                '',
                '',
                '',
                '',
                '',
                '',
                '',
                '');

      UPDATE i2b2metadata.i2b2
      SET
        C_SYNONYM_CD = old_root_i2b2.C_SYNONYM_CD,
         C_VISUALATTRIBUTES = old_root_i2b2.C_VISUALATTRIBUTES,
         C_BASECODE = old_root_i2b2.C_BASECODE,
         C_FACTTABLECOLUMN = old_root_i2b2.C_FACTTABLECOLUMN,
         C_TABLENAME = old_root_i2b2.C_TABLENAME,
         C_COLUMNNAME = old_root_i2b2.C_COLUMNNAME,
         C_COLUMNDATATYPE = old_root_i2b2.C_COLUMNDATATYPE,
         C_OPERATOR = old_root_i2b2.C_OPERATOR,
         C_COMMENT = old_root_i2b2.C_COMMENT,
         DOWNLOAD_DATE = old_root_i2b2.DOWNLOAD_DATE,
         IMPORT_DATE = old_root_i2b2.IMPORT_DATE,
         M_APPLIED_PATH = old_root_i2b2.M_APPLIED_PATH
        FROM
        (SELECT
           C_SYNONYM_CD,
           C_VISUALATTRIBUTES,
           C_BASECODE,
           C_FACTTABLECOLUMN,
           C_TABLENAME,
           C_COLUMNNAME,
           C_COLUMNDATATYPE,
           C_OPERATOR,
           C_COMMENT,
           DOWNLOAD_DATE,
           IMPORT_DATE,
           M_APPLIED_PATH
         FROM i2b2metadata.i2b2
         WHERE C_FULLNAME = old_root_node) AS old_root_i2b2
      WHERE C_FULLNAME = new_root_node;

      get diagnostics rowCt := ROW_COUNT;
      exception
        when others then
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
          --End Proc
          select cz_end_audit (jobID, 'FAIL') into rtnCd;
          return -16;
      end;


      stepCt := stepCt + 1;
      select cz_write_audit(jobId, databaseName, procedureName, 'Create new root node in i2b2', rowCt, stepCt, 'Done')  into rtnCd;


-- create new root in i2b2_secure
      begin
      INSERT INTO i2b2metadata.i2b2_secure (
        C_HLEVEL,
        C_FULLNAME,
        UPDATE_DATE,
        C_DIMCODE,
        C_TOOLTIP,
        C_NAME
      )
        VALUES (0,
                new_root_node,
                current_timestamp,
                new_root_node,
                new_root_node,
                new_root_node_name);

      UPDATE i2b2metadata.i2b2_secure
      SET
        C_SYNONYM_CD = old_root_i2b2_secure.C_SYNONYM_CD,
         C_VISUALATTRIBUTES = old_root_i2b2_secure.C_VISUALATTRIBUTES,
         C_BASECODE = old_root_i2b2_secure.C_BASECODE,
         C_FACTTABLECOLUMN = old_root_i2b2_secure.C_FACTTABLECOLUMN,
         C_TABLENAME = old_root_i2b2_secure.C_TABLENAME,
         C_COLUMNNAME = old_root_i2b2_secure.C_COLUMNNAME,
         C_COLUMNDATATYPE = old_root_i2b2_secure.C_COLUMNDATATYPE,
         C_OPERATOR = old_root_i2b2_secure.C_OPERATOR,
         C_COMMENT = old_root_i2b2_secure.C_COMMENT,
         DOWNLOAD_DATE = old_root_i2b2_secure.DOWNLOAD_DATE,
         IMPORT_DATE = old_root_i2b2_secure.IMPORT_DATE,
         M_APPLIED_PATH = old_root_i2b2_secure.M_APPLIED_PATH,
         SECURE_OBJ_TOKEN = old_root_i2b2_secure.SECURE_OBJ_TOKEN
        FROM
        (SELECT
           C_SYNONYM_CD,
           C_VISUALATTRIBUTES,
           C_BASECODE,
           C_FACTTABLECOLUMN,
           C_TABLENAME,
           C_COLUMNNAME,
           C_COLUMNDATATYPE,
           C_OPERATOR,
           C_COMMENT,
           DOWNLOAD_DATE,
           IMPORT_DATE,
           M_APPLIED_PATH,
           SECURE_OBJ_TOKEN
         FROM i2b2metadata.i2b2_secure
         WHERE C_FULLNAME = old_root_node) AS old_root_i2b2_secure
      WHERE C_FULLNAME = new_root_node;

      get diagnostics rowCt := ROW_COUNT;
      exception
        when others then
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
          --End Proc
          select cz_end_audit (jobID, 'FAIL') into rtnCd;
          return -16;
      end;

      stepCt := stepCt + 1;
      select cz_write_audit(jobId, databaseName, procedureName, 'Create new root node in i2b2_secure', rowCt, stepCt,
                     'Done')  into rtnCd;


-- create new root in i2b2demodata.concept_dimension
      begin
      INSERT INTO i2b2demodata.concept_dimension
      (concept_cd
        , concept_path
        , name_char
        , update_date
        , download_date
        , import_date
      )
        VALUES( nextval('i2b2demodata.concept_id'),
                new_root_node,
                new_root_node_name,
                current_timestamp,
                current_timestamp,
                current_timestamp );

      UPDATE i2b2demodata.concept_dimension
      SET sourcesystem_cd = (SELECT
                               sourcesystem_cd
                             FROM i2b2demodata.concept_dimension
                             WHERE concept_path = old_root_node)
      WHERE concept_path = new_root_node;

      get diagnostics rowCt := ROW_COUNT;
      exception
        when others then
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
          --End Proc
          select cz_end_audit (jobID, 'FAIL') into rtnCd;
          return -16;
      end;

      stepCt := stepCt + 1;
      select cz_write_audit(jobId, databaseName, procedureName, 'Create new root node in i2b2demodata.concept_dimension', rowCt, stepCt,
                     'Done')  into rtnCd;

    END IF;

-- check if old root has another child
    SELECT
      count(c_fullname)
    INTO counter
    FROM i2b2metadata.i2b2
    WHERE c_fullname LIKE old_root_node || '%' ESCAPE '`' AND
          c_fullname NOT IN
          (SELECT
             c_fullname
           FROM i2b2metadata.i2b2
           WHERE c_fullname LIKE old_path || '%' ESCAPE '`');

    IF old_root_node <> new_root_node AND counter = 1
    THEN

-- if has not - remove old root node from i2b2, i2b2_secure, table_access
      begin
      DELETE FROM i2b2metadata.i2b2
      WHERE c_fullname = old_root_node;
      DELETE FROM i2b2metadata.i2b2_secure
      WHERE c_fullname = old_root_node;
      DELETE FROM i2b2metadata.table_access
      WHERE c_fullname = old_root_node;
      DELETE FROM i2b2demodata.concept_dimension
      WHERE concept_path = old_root_node;

      get diagnostics rowCt := ROW_COUNT;
      exception
        when others then
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
          --End Proc
          select cz_end_audit (jobID, 'FAIL') into rtnCd;
          return -16;
      end;

      stepCt := stepCt + 1;
      select cz_write_audit(jobId, databaseName, procedureName, 'Remove old root node from i2b2, i2b2_secure, table_access',
                     rowCt, stepCt, 'Done')  into rtnCd;


    END IF;


-- check new level need to be added
    SELECT
      length(old_path) - length(replace(old_path, '\', ''))
    INTO old_level_num;

    SELECT
      length(new_path) - length(replace(new_path, '\', ''))
    INTO new_level_num;


    IF new_level_num < old_level_num
    THEN
        lvl_num_to_remove := old_level_num - new_level_num;
        -- parse old_path to levels
        DROP TABLE IF EXISTS temp_t CASCADE;
        CREATE TEMP TABLE temp_t AS SELECT node_name
        FROM regexp_split_to_table(old_path, '\\') AS node_name;

        DELETE FROM temp_t WHERE node_name='';

        DROP TABLE IF EXISTS temp_t_levels CASCADE;
        CREATE TEMP TABLE temp_t_levels (lvl integer, parent_lvl integer,node_name varchar);

        FOR i IN 0..((select count(*) from temp_t)-1) LOOP
           INSERT INTO temp_t_levels (node_name) SELECT node_name FROM temp_t LIMIT 1 OFFSET i;
           UPDATE temp_t_levels SET lvl=i+1, parent_lvl=i  WHERE node_name=(SELECT node_name FROM temp_t LIMIT 1 OFFSET i);
        END LOOP;

        DROP TABLE IF EXISTS temp_t_paths CASCADE;
        CREATE TEMP TABLE temp_t_paths (lvl integer, parent_lvl integer,node_name varchar,node_path varchar);

        WITH RECURSIVE temp_t_paths ("lvl", "parent_lvl", "node_name", "node_path") AS (
        SELECT  T1.lvl,T1.parent_lvl, T1.node_name,  '\'|| T1.node_name
            FROM temp_t_levels T1 WHERE T1.lvl=1
        union
        select T2.lvl, T2.parent_lvl, T2.node_name, temp_t_paths.node_path ||'\'|| T2.node_name
             FROM temp_t_levels T2 INNER JOIN temp_t_paths ON (temp_t_paths.lvl= T2.parent_lvl))
             INSERT INTO temp_t_paths (select * from temp_t_paths);

        UPDATE temp_t_paths set node_path=node_path || '\';

        FOR i IN 1..(select count(*) from temp_t_paths) LOOP
              SELECT node_path INTO current_path FROM temp_t_paths WHERE lvl=i;
              current_path_level:= i - 1;
              SELECT node_name INTO current_path_attr_name FROM temp_t_paths WHERE lvl=i;

              IF current_path_level > 0 AND lvl_num_to_remove > 0
              THEN
      -- remove old level path
                  begin
                      SELECT
                        COUNT(*)
                      INTO counter
                      FROM i2b2metadata.i2b2
                      WHERE c_fullname = current_path;

                      IF counter > 0
                      THEN
                        DELETE FROM i2b2metadata.i2b2
                        WHERE c_fullname = current_path;

                        stepCt := stepCt + 1;
                        get diagnostics rowCt := ROW_COUNT;
                        select cz_write_audit(jobId, databaseName, procedureName, 'Remove ' || current_path || ' from i2b2metadata.i2b2',
                                     rowCt, stepCt,
                                     'Done') into rtnCd;
                      END IF;

                      SELECT
                        COUNT(*)
                      INTO counter
                      FROM i2b2metadata.i2b2_secure
                      WHERE c_fullname = current_path;

                      IF counter > 0
                      THEN

                        DELETE FROM i2b2metadata.i2b2_secure
                        WHERE c_fullname = current_path;


                        stepCt := stepCt + 1;
                        get diagnostics rowCt := ROW_COUNT;
                        select cz_write_audit(jobId, databaseName, procedureName,
                                     'Remove ' || current_path || ' from i2b2metadata.i2b2_secure', rowCt, stepCt,
                                     'Done') into rtnCd;
                      END IF;


                      SELECT
                        COUNT(*)
                      INTO counter
                      FROM i2b2demodata.concept_counts
                      WHERE concept_path = current_path;

                      IF counter > 0
                      THEN

                        DELETE FROM i2b2demodata.concept_counts
                        WHERE concept_path = current_path;


                        stepCt := stepCt + 1;
                        get diagnostics rowCt := ROW_COUNT;
                        select cz_write_audit(jobId, databaseName, procedureName, 'Remove ' || current_path || ' i2b2demodata.concept_counts',
                                       rowCt, stepCt,
                                       'Done') into rtnCd;
                      END IF;



                      SELECT
                        COUNT(*)
                      INTO counter
                      FROM i2b2demodata.concept_dimension
                      WHERE concept_path = current_path;

                      IF counter > 0
                      THEN

                        DELETE FROM i2b2demodata.concept_dimension
                        WHERE concept_path = current_path;

                        stepCt := stepCt + 1;
                        get diagnostics rowCt := ROW_COUNT;
                        select cz_write_audit(jobId, databaseName, procedureName,
                                     'Remove ' || current_path || ' i2b2demodata.concept_dimension', rowCt, stepCt,
                                     'Done') into rtnCd;
                      END IF;


                      lvl_num_to_remove := lvl_num_to_remove - 1;

                      get diagnostics rowCt := ROW_COUNT;
                      exception
                      when others then
                          errorNumber := SQLSTATE;
                          errorMessage := SQLERRM;
                          --Handle errors.
                          select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
                          --End Proc
                          select cz_end_audit (jobID, 'FAIL') into rtnCd;
                          return -16;
                  end;

            END IF;

        END LOOP;

    END IF;

-- rename paths in i2b2 and i2b2_secure
    begin
    UPDATE i2b2metadata.i2b2
    SET c_fullname=replace(c_fullname, old_path, new_path),
      c_dimcode=replace(c_dimcode, old_path, new_path),
      c_tooltip=replace(c_tooltip, old_path, new_path)
    WHERE c_fullname LIKE old_path || '%' ESCAPE '`';

    UPDATE i2b2metadata.i2b2_secure
    SET c_fullname=replace(c_fullname, old_path, new_path),
      c_dimcode=replace(c_dimcode, old_path, new_path),
      c_tooltip=replace(c_tooltip, old_path, new_path)
    WHERE c_fullname LIKE old_path || '%' ESCAPE '`';


    get diagnostics rowCt := ROW_COUNT;
    exception
        when others then
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
          --End Proc
          select cz_end_audit (jobID, 'FAIL') into rtnCd;
          return -16;
    end;

    stepCt := stepCt + 1;
    select cz_write_audit(jobId, databaseName, procedureName, 'Rename paths in i2b2 and i2b2_secure', rowCt, stepCt,
                   'Done') into rtnCd;


-- rename c_name in i2b2 and i2b2_secure
    begin
    UPDATE i2b2metadata.i2b2
    SET c_name=new_path_last_node_name
    WHERE c_fullname = new_path;
    UPDATE i2b2metadata.i2b2_secure
    SET c_name=new_path_last_node_name
    WHERE c_fullname = new_path;

    get diagnostics rowCt := ROW_COUNT;
    exception
        when others then
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
          --End Proc
          select cz_end_audit (jobID, 'FAIL') into rtnCd;
          return -16;
    end;

    stepCt := stepCt + 1;
    select cz_write_audit(jobId, databaseName, procedureName, 'Update c_name in i2b2 and i2b2_secure', rowCt, stepCt,
                   'Done') into rtnCd;

--rename paths in concept_dimension
    begin
    UPDATE i2b2demodata.concept_dimension
    SET concept_path = replace(concept_path, old_path, new_path)
    WHERE concept_path LIKE old_path || '%' ESCAPE '`';

    get diagnostics rowCt := ROW_COUNT;
    exception
        when others then
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
          --End Proc
          select cz_end_audit (jobID, 'FAIL') into rtnCd;
          return -16;
    end;

    stepCt := stepCt + 1;
    select cz_write_audit(jobId, databaseName, procedureName, 'Rename paths in concept_dimension', rowCt, stepCt,
                   'Done') into rtnCd;

-- rename old_root_node in concept_counts
    begin
    UPDATE i2b2demodata.concept_counts
    SET parent_concept_path=new_root_node
    WHERE concept_path = old_path;
    get diagnostics rowCt := ROW_COUNT;
    exception
        when others then
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
          --End Proc
          select cz_end_audit (jobID, 'FAIL') into rtnCd;
          return -16;
    end;

    stepCt := stepCt + 1;
    select cz_write_audit(jobId, databaseName, procedureName, 'Update parent_concept_path in concept_counts', rowCt,
                   stepCt, 'Done') into rtnCd;
-- update concept_counts
    begin
    UPDATE i2b2demodata.concept_counts
    SET concept_path=replace(concept_path, old_path, new_path),
      parent_concept_path=replace(parent_concept_path, old_path, new_path)
    WHERE concept_path LIKE old_path || '%' ESCAPE '`';

    get diagnostics rowCt := ROW_COUNT;
    exception
        when others then
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
          --End Proc
          select cz_end_audit (jobID, 'FAIL') into rtnCd;
          return -16;
    end;

    stepCt := stepCt + 1;
    select cz_write_audit(jobId, databaseName, procedureName, 'Rename paths in concept_counts', rowCt, stepCt, 'Done') into rtnCd;

  IF new_level_num > old_level_num
    THEN

        lvl_num_to_remove := old_level_num - new_level_num;
        -- parse old_path to levels
        DROP TABLE IF EXISTS temp_t CASCADE;
        CREATE TEMP TABLE temp_t AS SELECT node_name
        FROM regexp_split_to_table(new_path, '\\') AS node_name;

        DELETE FROM temp_t WHERE node_name='';

        DROP TABLE IF EXISTS temp_t_levels CASCADE;
        CREATE TEMP TABLE temp_t_levels (lvl integer, parent_lvl integer,node_name varchar);

        FOR i IN 0..((select count(*) from temp_t)-1) LOOP
           INSERT INTO temp_t_levels (node_name) SELECT node_name FROM temp_t LIMIT 1 OFFSET i;
           UPDATE temp_t_levels SET lvl=i+1, parent_lvl=i  WHERE node_name=(SELECT node_name FROM temp_t LIMIT 1 OFFSET i);
        END LOOP;

        DROP TABLE IF EXISTS temp_t_paths CASCADE;
        CREATE TEMP TABLE temp_t_paths (lvl integer, parent_lvl integer,node_name varchar,node_path varchar);

        WITH RECURSIVE temp_t_paths ("lvl", "parent_lvl", "node_name", "node_path") AS (
        SELECT  T1.lvl,T1.parent_lvl, T1.node_name,  '\'|| T1.node_name
            FROM temp_t_levels T1 WHERE T1.lvl=1
        union
        select T2.lvl, T2.parent_lvl, T2.node_name, temp_t_paths.node_path ||'\'|| T2.node_name
             FROM temp_t_levels T2 INNER JOIN temp_t_paths ON (temp_t_paths.lvl= T2.parent_lvl))
             INSERT INTO temp_t_paths (select * from temp_t_paths);

        UPDATE temp_t_paths set node_path=node_path || '\';

        FOR i IN 1..(select count(*) from temp_t_paths) LOOP
              SELECT node_path INTO current_path FROM temp_t_paths WHERE lvl=i;
              current_path_level:= i - 1;
              SELECT node_name INTO current_path_attr_name FROM temp_t_paths WHERE lvl=i;

              IF current_path_level > 0
              THEN -- check all except root_node

                SELECT
                  count(*)
                INTO counter
                FROM i2b2demodata.concept_dimension
                WHERE concept_path = current_path;

                IF counter = 0
                THEN
                -- insert new level nodes to i2b2demodata.concept_dimension
                  begin
                  INSERT INTO i2b2demodata.concept_dimension
                  (concept_cd
                    , concept_path
                    , name_char
                    , update_date
                    , download_date
                    , import_date
                  )
                    VALUES (nextval('i2b2demodata.concept_id'),
                            current_path,
                            current_path_attr_name,
                            current_timestamp,
                            current_timestamp,
                            current_timestamp
                            );


                  UPDATE i2b2demodata.concept_dimension
                  SET sourcesystem_cd = cd.sourcesystem_cd FROM (SELECT
                                           sourcesystem_cd
                                         FROM i2b2demodata.concept_dimension
                                         WHERE concept_path = new_root_node) AS cd
                  WHERE concept_path = current_path;

                  get diagnostics rowCt := ROW_COUNT;
                  exception
                  when others then
                    errorNumber := SQLSTATE;
                    errorMessage := SQLERRM;
                    --Handle errors.
                    select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
                    --End Proc
                    select cz_end_audit (jobID, 'FAIL') into rtnCd;
                    return -16;
                  end;

                  stepCt := stepCt + 1;
                  select cz_write_audit(jobId, databaseName, procedureName,
                                 'Insert ' || current_path || ' into i2b2demodata.concept_dimension', rowCt, stepCt,
                                 'Done') into rtnCd;

                END IF;

                SELECT
                  count(*)
                INTO counter
                FROM i2b2metadata.i2b2
                WHERE c_fullname = current_path;

                IF counter = 0
                THEN
                  begin
                -- create new level path in i2b2
                  INSERT INTO i2b2metadata.i2b2 (
                        C_HLEVEL,
                        C_FULLNAME,
                        UPDATE_DATE,
                        C_DIMCODE,
                        C_TOOLTIP,
                        C_NAME,
                        C_SYNONYM_CD,
                        C_VISUALATTRIBUTES,
                        C_FACTTABLECOLUMN,
                        C_TABLENAME,
                        C_COLUMNNAME,
                        C_COLUMNDATATYPE,
                        C_OPERATOR,
                        M_APPLIED_PATH
                  )
                    VALUES (current_path_level,
                            current_path,
                            current_timestamp,
                            current_path,
                            current_path,
                            current_path_attr_name,
                            '',
                            'FA',
                            '',
                            '',
                            '',
                            '',
                            '',
                            ''
                    );

                  UPDATE i2b2metadata.i2b2
                  SET
                    C_SYNONYM_CD = old_root_i2b2.C_SYNONYM_CD,
                     C_BASECODE = old_root_i2b2.C_BASECODE,
                     C_FACTTABLECOLUMN = old_root_i2b2.C_FACTTABLECOLUMN,
                     C_TABLENAME = old_root_i2b2.C_TABLENAME,
                     C_COLUMNNAME = old_root_i2b2.C_COLUMNNAME,
                     C_COLUMNDATATYPE = old_root_i2b2.C_COLUMNDATATYPE,
                     C_OPERATOR = old_root_i2b2.C_OPERATOR,
                     C_COMMENT = old_root_i2b2.C_COMMENT,
                     DOWNLOAD_DATE = old_root_i2b2.DOWNLOAD_DATE,
                     IMPORT_DATE = old_root_i2b2.IMPORT_DATE,
                     M_APPLIED_PATH = old_root_i2b2.M_APPLIED_PATH
                    FROM
                    (SELECT
                       C_SYNONYM_CD,
                       C_BASECODE,
                       C_FACTTABLECOLUMN,
                       C_TABLENAME,
                       C_COLUMNNAME,
                       C_COLUMNDATATYPE,
                       C_OPERATOR,
                       C_COMMENT,
                       DOWNLOAD_DATE,
                       IMPORT_DATE,
                       M_APPLIED_PATH
                     FROM i2b2metadata.i2b2
                     WHERE C_FULLNAME = new_root_node) AS old_root_i2b2
                  WHERE C_FULLNAME = current_path;


                  UPDATE i2b2metadata.i2b2
                  SET C_BASECODE=cd.CONCEPT_CD FROM (SELECT
                                    CONCEPT_CD
                                  FROM i2b2demodata.concept_dimension
                                  WHERE CONCEPT_PATH = current_path) AS cd
                  WHERE C_FULLNAME = current_path;

                  get diagnostics rowCt := ROW_COUNT;
                  exception
                    when others then
                      errorNumber := SQLSTATE;
                      errorMessage := SQLERRM;
                      --Handle errors.
                      select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
                      --End Proc
                      select cz_end_audit (jobID, 'FAIL') into rtnCd;
                      return -16;
                  end;

                  stepCt := stepCt + 1;
                  select cz_write_audit(jobId, databaseName, procedureName,
                                 'Insert ' || current_path || ' into i2b2metadata.i2b2', rowCt, stepCt,
                                 'Done') into rtnCd;


                END IF;

                SELECT
                  count(*)
                INTO counter
                FROM i2b2metadata.i2b2_secure
                WHERE c_fullname = current_path;

                IF counter = 0
                THEN
                -- create new level path in i2b2_secure
                  begin
                  INSERT INTO i2b2metadata.i2b2_secure (
                    C_HLEVEL,
                    C_FULLNAME,
                    UPDATE_DATE,
                    C_DIMCODE,
                    C_TOOLTIP,
                    C_NAME,
                    C_VISUALATTRIBUTES
                  )
                    VALUES (current_path_level,
                            current_path,
                            current_timestamp,
                            current_path,
                            current_path,
                            current_path_attr_name,
                            'FA');

                   UPDATE i2b2metadata.i2b2_secure
                   SET
                      C_SYNONYM_CD = old_root_i2b2_secure.C_SYNONYM_CD,
                       C_VISUALATTRIBUTES = old_root_i2b2_secure.C_VISUALATTRIBUTES,
                       C_BASECODE = old_root_i2b2_secure.C_BASECODE,
                       C_FACTTABLECOLUMN = old_root_i2b2_secure.C_FACTTABLECOLUMN,
                       C_TABLENAME = old_root_i2b2_secure.C_TABLENAME,
                       C_COLUMNNAME = old_root_i2b2_secure.C_COLUMNNAME,
                       C_COLUMNDATATYPE = old_root_i2b2_secure.C_COLUMNDATATYPE,
                       C_OPERATOR = old_root_i2b2_secure.C_OPERATOR,
                       C_COMMENT = old_root_i2b2_secure.C_COMMENT,
                       DOWNLOAD_DATE = old_root_i2b2_secure.DOWNLOAD_DATE,
                       IMPORT_DATE = old_root_i2b2_secure.IMPORT_DATE,
                       M_APPLIED_PATH = old_root_i2b2_secure.M_APPLIED_PATH,
                       SECURE_OBJ_TOKEN = old_root_i2b2_secure.SECURE_OBJ_TOKEN
                      FROM
                      (SELECT
                         C_SYNONYM_CD,
                         C_VISUALATTRIBUTES,
                         C_BASECODE,
                         C_FACTTABLECOLUMN,
                         C_TABLENAME,
                         C_COLUMNNAME,
                         C_COLUMNDATATYPE,
                         C_OPERATOR,
                         C_COMMENT,
                         DOWNLOAD_DATE,
                         IMPORT_DATE,
                         M_APPLIED_PATH,
                         SECURE_OBJ_TOKEN
                       FROM i2b2metadata.i2b2_secure
                       WHERE C_FULLNAME = parent_path_node) AS old_root_i2b2_secure
                    WHERE C_FULLNAME = current_path;

                  UPDATE i2b2metadata.i2b2_secure
                  SET C_BASECODE=cd.CONCEPT_CD FROM (SELECT
                                    CONCEPT_CD
                                  FROM i2b2demodata.concept_dimension
                                  WHERE CONCEPT_PATH = current_path) AS cd
                  WHERE C_FULLNAME = current_path;

                  get diagnostics rowCt := ROW_COUNT;
                  exception
                    when others then
                      errorNumber := SQLSTATE;
                      errorMessage := SQLERRM;
                      --Handle errors.
                      select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
                      --End Proc
                      select cz_end_audit (jobID, 'FAIL') into rtnCd;
                      return -16;
                  end;

                  stepCt := stepCt + 1;
                  select cz_write_audit(jobId, databaseName, procedureName,
                                 'Insert ' || current_path || ' into i2b2metadata.i2b2_secure', rowCt, stepCt,
                                 'Done') into rtnCd;
                END IF;

                begin
                UPDATE i2b2demodata.concept_counts
                SET parent_concept_path = parent_path_node
                WHERE concept_path = current_path;

                get diagnostics rowCt := ROW_COUNT;
                  exception
                    when others then
                      errorNumber := SQLSTATE;
                      errorMessage := SQLERRM;
                      --Handle errors.
                      select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
                      --End Proc
                      select cz_end_audit (jobID, 'FAIL') into rtnCd;
                      return -16;
                end;

                stepCt := stepCt + 1;
                select cz_write_audit(jobId, databaseName, procedureName,
                'Update parent path of ' || current_path || ' in i2b2demodata.concept_counts',rowCt, stepCt,
                'Done') into rtnCd;

            END IF;
        parent_path_node := current_path;
        END LOOP;

    END IF;

    begin
      UPDATE i2b2metadata.i2b2
      SET C_HLEVEL = (length(C_FULLNAME) - coalesce(length(replace(C_FULLNAME, '\', '')), 0)) / length('\') - 2
      WHERE c_fullname LIKE new_path || '%' ESCAPE '`';
      get diagnostics rowCt := ROW_COUNT;
       exception
          when others then
            errorNumber := SQLSTATE;
            errorMessage := SQLERRM;
            --Handle errors.
            select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
            --End Proc
            select cz_end_audit (jobID, 'FAIL') into rtnCd;
            return -16;
    end;

    stepCt := stepCt + 1;
    select cz_write_audit(jobId, databaseName, procedureName,
                                 'Update levels in i2b2metadata.i2b2', rowCt, stepCt,
                                 'Done') into rtnCd;

    begin
      UPDATE i2b2metadata.i2b2_secure
      SET C_HLEVEL = (length(C_FULLNAME) - coalesce(length(replace(C_FULLNAME, '\', '')), 0)) / length('\') - 2
      WHERE c_fullname LIKE new_path || '%' ESCAPE '`';
      get diagnostics rowCt := ROW_COUNT;
      exception
          when others then
            errorNumber := SQLSTATE;
            errorMessage := SQLERRM;
            --Handle errors.
            select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
            --End Proc
            select cz_end_audit (jobID, 'FAIL') into rtnCd;
            return -16;

    end;

     stepCt := stepCt + 1;
     select cz_write_audit(jobId, databaseName, procedureName,
                                 'Update levels in i2b2metadata.i2b2_secure', rowCt, stepCt,
                                 'Done') into rtnCd;

    ---Cleanup OVERALL JOB if this proc is being run standalone
    IF newJobFlag = 1
    THEN
      select cz_end_audit (jobID, 'SUCCESS') into rtnCD;
    END IF;

    return 1;
  END;

  $BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;

ALTER FUNCTION i2b2_move_study_by_path( CHARACTER VARYING, CHARACTER VARYING, NUMERIC )
OWNER TO postgres;
