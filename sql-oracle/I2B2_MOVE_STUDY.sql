CREATE OR REPLACE
PROCEDURE "I2B2_MOVE_STUDY"
  (old_path_in  VARCHAR2,
   new_path_in  VARCHAR2,
   currentJobID NUMBER := null
  )
AS
--Audit variables
  newJobFlag              INTEGER (1);
  databaseName            VARCHAR(100);
  procedureName           VARCHAR(100);
  jobID                   NUMBER(18, 0);
  stepCt                  NUMBER(18, 0);
  tText                   VARCHAR2(2000);


  old_path                VARCHAR2(700 BYTE);
  new_path                VARCHAR2(700 BYTE);
  old_root_node           VARCHAR2(700 BYTE);
  new_root_node           VARCHAR2(700 BYTE);
  new_root_node_name      VARCHAR2(700 BYTE);
  new_path_last_node_name VARCHAR2(700 BYTE);
  rowsExists              INT;
  counter                 INT;
  lvl_num_to_remove       INT;
  old_level_num           INT;
  new_level_num           INT;
  parent_path_node        VARCHAR2(700 BYTE);
  current_path            VARCHAR2(700 BYTE);
  current_path_level      INT;
  current_path_attr_name  VARCHAR2(700 BYTE);
  tmp                     VARCHAR2(700 BYTE);

  TYPE row_type IS RECORD
  ( path VARCHAR2(700 BYTE),
    lvl NUMBER,
    attr_name VARCHAR2(700 BYTE)
  );

  TYPE paths_tab_type IS TABLE OF row_type;
  paths_tab               paths_tab_type := paths_tab_type();

    old_study_missed EXCEPTION;
    new_study_exists EXCEPTION;
    duplicated_paths EXCEPTION;

  BEGIN

--Audit JOB Initialization
    stepCt := 0;

--Set Audit Parameters
    newJobFlag := 0;
-- False (Default)
    jobID := currentJobID;

    SELECT
      sys_context('USERENV', 'CURRENT_SCHEMA')
    INTO databaseName
    FROM dual;
    procedureName := $$PLSQL_UNIT;

--If Job ID does not exist, then this is a single procedure run and we need to create it
    IF (jobID IS NULL OR jobID < 1)
    THEN
      newJobFlag := 1;
-- True
      cz_start_audit(procedureName, databaseName, jobID);
    END IF;

    stepCt := 0;

    stepCt := stepCt + 1;
    tText := 'Start i2b2_move_study from ' || old_path || ' to ' || new_path;
    cz_write_audit(jobId, databaseName, procedureName, tText, 0, stepCt, 'Done');


-- check first and last /

    old_path := old_path_in;
    new_path := new_path_in;

    SELECT
      SUBSTR(old_path, 1, 1)
    INTO tmp
    FROM dual;
    IF tmp <> '\'
    THEN
      old_path := '\' || old_path;
    END IF;

    SELECT
      SUBSTR(new_path, 1, 1)
    INTO tmp
    FROM dual;
    IF tmp <> '\'
    THEN
      new_path := '\' || new_path;
    END IF;

    SELECT
      SUBSTR(old_path, -1, 1)
    INTO tmp
    FROM dual;
    IF tmp <> '\'
    THEN
      old_path := old_path || '\';
    END IF;

    SELECT
      SUBSTR(new_path, -1, 1)
    INTO tmp
    FROM dual;
    IF tmp <> '\'
    THEN
      new_path := new_path || '\';
    END IF;

    old_root_node := REGEXP_REPLACE(old_path, '(\\(\w|\s)*\\)(.*)', '\1');
--\Test Studies\
    new_root_node := REGEXP_REPLACE(new_path, '(\\(\w|\s)*\\)(.*)', '\1');
-- '\Test Studies 2\';
    new_root_node_name := REGEXP_REPLACE(new_path, '\\((\w|\s)*)\\(.*)', '\1');
--Test Studies 2
    new_path_last_node_name := REGEXP_REPLACE(new_path, '(.*)\\((\w|\s)*)\\', '\2');
-- ClinicalSample

    IF old_path = new_path
    THEN
      RAISE duplicated_paths;
    END IF;
-- check old root node exists
    SELECT
      count(*)
    INTO rowsExists
    FROM i2b2metadata.i2b2
    WHERE c_fullname = old_path;
--1
    IF rowsExists = 0
    THEN
      RAISE old_study_missed;
    END IF;

/*SELECT
  count(*)
INTO rowsExists
FROM i2b2metadata.i2b2
WHERE c_fullname = new_path;
--0
IF rowsExists <> 0
THEN
  RAISE new_study_exists;
END IF; */

-- if 1
-- check new root node exists

--select count(*) from i2b2metadata.i2b2 where c_fullname=REGEXP_REPLACE('\Test Studies\ClinicalSample2\','(\\.*\\)(.*\\)', '\1'); --1
--select count(*) from i2b2metadata.i2b2 where c_fullname=REGEXP_REPLACE('\Test Studies 2\ClinicalSample','(\\.*\\)(.*\\)', '\1'); --0

    SELECT
      count(*)
    INTO rowsExists
    FROM i2b2metadata.i2b2
    WHERE c_fullname = new_root_node;

    IF rowsExists = 0
    THEN
-- create new root in table_access,
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
          sysdate             AS c_entry_date,
          null                AS c_change_date,
          null                AS c_status_cd,
          null                AS valuetype_cd
        FROM dual
        WHERE NOT exists
        (SELECT
           1
         FROM i2b2metadata.table_access x
         WHERE x.c_table_cd = new_root_node);

      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName, 'Create new root node in table_access', SQL%ROWCOUNT, stepCt,
                     'Done');
      COMMIT;

-- create new root in i2b2
      INSERT INTO i2b2metadata.i2b2 (
        C_HLEVEL,
        C_FULLNAME,
        UPDATE_DATE,
        C_DIMCODE,
        C_TOOLTIP,
        C_NAME
      )
        VALUES (0,
                new_root_node,
                sysdate,
                new_root_node,
                new_root_node,
                new_root_node_name);

      UPDATE i2b2metadata.i2b2
      SET
        (C_SYNONYM_CD,
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
         M_APPLIED_PATH)
        =
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
         WHERE C_FULLNAME = old_root_node)
      WHERE C_FULLNAME = new_root_node;

      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName, 'Create new root node in i2b2', SQL%ROWCOUNT, stepCt, 'Done');
      COMMIT;

-- create new root in i2b2_secure
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
                sysdate,
                new_root_node,
                new_root_node,
                new_root_node_name);

      UPDATE i2b2metadata.i2b2_secure
      SET
        (C_SYNONYM_CD,
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
         SECURE_OBJ_TOKEN)
        =
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
         WHERE C_FULLNAME = old_root_node)
      WHERE C_FULLNAME = new_root_node;

      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName, 'Create new root node in i2b2_secure', SQL%ROWCOUNT, stepCt,
                     'Done');
      COMMIT;


-- create new root in i2b2demodata.concept_dimension
      INSERT INTO i2b2demodata.concept_dimension
      (concept_cd
        , concept_path
        , name_char
        , update_date
        , download_date
        , import_date
        , table_name
      )
        VALUES (concept_id.nextval,
                new_root_node,
                new_root_node_name,
                sysdate,
                sysdate,
                sysdate,
                'CONCEPT_DIMENSION');

      UPDATE i2b2demodata.concept_dimension
      SET sourcesystem_cd = (SELECT
                               sourcesystem_cd
                             FROM i2b2demodata.concept_dimension
                             WHERE concept_path = old_root_node)
      WHERE concept_path = new_root_node;

    END IF;

-- check if old root has another child
    SELECT
      count(c_fullname)
    INTO counter
    FROM i2b2metadata.i2b2
    WHERE c_fullname LIKE old_root_node || '%' AND
          c_fullname NOT IN
          (SELECT
             c_fullname
           FROM i2b2metadata.i2b2
           WHERE c_fullname LIKE old_path || '%');

    IF old_root_node <> new_root_node AND counter = 1
    THEN
-- if has not - remove old root node from i2b2, i2b2_secure, table_access
      DELETE FROM i2b2metadata.i2b2
      WHERE c_fullname = old_root_node;
      DELETE FROM i2b2metadata.i2b2_secure
      WHERE c_fullname = old_root_node;
      DELETE FROM i2b2metadata.table_access
      WHERE c_fullname = old_root_node;

      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName, 'Remove old root node from i2b2, i2b2_secure, table_access',
                     SQL%ROWCOUNT, stepCt, 'Done');
      COMMIT;
    END IF;


-- check new level need to be added
    SELECT
      length(old_path) - length(replace(old_path, '\'))
    INTO old_level_num
    FROM dual;
    SELECT
      length(new_path) - length(replace(new_path, '\'))
    INTO new_level_num
    FROM dual;

    IF new_level_num < old_level_num
    THEN
      lvl_num_to_remove := old_level_num - new_level_num;


-- parse old_path to levels
      SELECT
        SYS_CONNECT_BY_PATH(STR, '\') || '\',
        LVL,
        STR
      BULK COLLECT INTO paths_tab
      FROM (

        SELECT
          LEVEL                                                  LVL,
          LEVEL - 1                                              PARENT_LEVEL,
          REGEXP_SUBSTR(ltrim(old_path, '\'), '[^\]+', 1, LEVEL) STR
        FROM dual
        CONNECT BY LEVEL <= LENGTH(REGEXP_REPLACE(ltrim(old_path, '\'), '[^\]+'))

      )
      START WITH PARENT_LEVEL = 0
      CONNECT BY PRIOR LVL = PARENT_LEVEL;

      FOR i IN 1 .. paths_tab.COUNT
      LOOP
        current_path := paths_tab(i).path;
        current_path_level := paths_tab(i).lvl - 1;
        current_path_attr_name := paths_tab(i).attr_name;

      IF current_path_level > 0 AND lvl_num_to_remove > 0
      THEN
-- remove old level path
        SELECT
          COUNT(*)
        INTO counter
        FROM i2b2metadata.i2b2
        WHERE c_fullname = current_path;

        IF counter > 0
        THEN
          DELETE FROM i2b2metadata.i2b2
          WHERE c_fullname = current_path;
        END IF;

        stepCt := stepCt + 1;
        cz_write_audit(jobId, databaseName, procedureName, 'Remove ' || current_path || ' from i2b2metadata.i2b2',
                       SQL%ROWCOUNT, stepCt,
                       'Done');
        COMMIT;

        SELECT
          COUNT(*)
        INTO counter
        FROM i2b2metadata.i2b2_secure
        WHERE c_fullname = current_path;

        IF counter > 0
        THEN
          DELETE FROM i2b2metadata.i2b2_secure
          WHERE c_fullname = current_path;
        END IF;

        stepCt := stepCt + 1;
        cz_write_audit(jobId, databaseName, procedureName,
                       'Remove ' || current_path || ' from i2b2metadata.i2b2_secure', SQL%ROWCOUNT, stepCt,
                       'Done');
        COMMIT;

        SELECT
          COUNT(*)
        INTO counter
        FROM i2b2demodata.concept_counts
        WHERE concept_path = current_path;

        IF counter > 0
        THEN
          DELETE FROM i2b2demodata.concept_counts
          WHERE concept_path = current_path;
        END IF;

        stepCt := stepCt + 1;
        cz_write_audit(jobId, databaseName, procedureName, 'Remove ' || current_path || ' i2b2demodata.concept_counts',
                       SQL%ROWCOUNT, stepCt,
                       'Done');
        COMMIT;

        SELECT
          COUNT(*)
        INTO counter
        FROM i2b2demodata.concept_dimension
        WHERE concept_path = current_path;

        IF counter > 0
        THEN
          DELETE FROM i2b2demodata.concept_dimension
          WHERE concept_path = current_path;
        END IF;

        stepCt := stepCt + 1;
        cz_write_audit(jobId, databaseName, procedureName,
                       'Remove ' || current_path || ' i2b2demodata.concept_dimension', SQL%ROWCOUNT, stepCt,
                       'Done');
        COMMIT;

        lvl_num_to_remove := lvl_num_to_remove - 1;

      END IF;
      END LOOP;

    END IF;

-- rename paths in i2b2 and i2b2_secure
    UPDATE i2b2metadata.i2b2
    SET c_fullname=replace(c_fullname, old_path, new_path),
      c_dimcode=replace(c_dimcode, old_path, new_path),
      c_tooltip=replace(c_tooltip, old_path, new_path)
    WHERE c_fullname LIKE old_path || '%';

    UPDATE i2b2metadata.i2b2_secure
    SET c_fullname=replace(c_fullname, old_path, new_path),
      c_dimcode=replace(c_dimcode, old_path, new_path),
      c_tooltip=replace(c_tooltip, old_path, new_path)
    WHERE c_fullname LIKE old_path || '%';

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Rename paths in i2b2 and i2b2_secure', SQL%ROWCOUNT, stepCt,
                   'Done');
    COMMIT;


-- rename c_name in i2b2 and i2b2_secure
    UPDATE i2b2metadata.i2b2
    SET c_name=new_path_last_node_name
    WHERE c_fullname = new_path;
    UPDATE i2b2metadata.i2b2_secure
    SET c_name=new_path_last_node_name
    WHERE c_fullname = new_path;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Update c_name in i2b2 and i2b2_secure', SQL%ROWCOUNT, stepCt,
                   'Done');
    COMMIT;

--rename paths in concept_dimension
    UPDATE i2b2demodata.concept_dimension
    SET concept_path = replace(concept_path, old_path, new_path)
    WHERE concept_path LIKE old_path || '%';
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Rename paths in concept_dimension', SQL%ROWCOUNT, stepCt,
                   'Done');
    COMMIT;

-- rename old_root_node in concept_counts
    UPDATE i2b2demodata.concept_counts
    SET parent_concept_path=new_root_node
    WHERE concept_path = old_path;
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Update parent_concept_path in concept_counts', SQL%ROWCOUNT,
                   stepCt, 'Done');
    COMMIT;

-- update concept_counts
    UPDATE i2b2demodata.concept_counts
    SET concept_path=replace(concept_path, old_path, new_path),
      parent_concept_path=replace(parent_concept_path, old_path, new_path)
    WHERE concept_path LIKE old_path || '%';
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Rename paths in concept_counts', SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

--rename paths in concept_dimension
    UPDATE concept_dimension
    SET CONCEPT_PATH = replace(concept_path, old_path, new_path)
    WHERE concept_path LIKE old_path || '%';
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Rename paths in concept_dimension', SQL%ROWCOUNT, stepCt,
                   'Done');
    COMMIT;

-- check new level need to be added
    SELECT
      length(old_path) - length(replace(old_path, '\'))
    INTO old_level_num
    FROM dual;
    SELECT
      length(new_path) - length(replace(new_path, '\'))
    INTO new_level_num
    FROM dual;

    IF new_level_num > old_level_num
    THEN
-- select all levels to temp table
      SELECT
        SYS_CONNECT_BY_PATH(STR, '\') || '\',
        LVL,
        STR
      BULK COLLECT INTO paths_tab
      FROM (

        SELECT
          LEVEL                                                  LVL,
          LEVEL - 1                                              PARENT_LEVEL,
          REGEXP_SUBSTR(ltrim(new_path, '\'), '[^\]+', 1, LEVEL) STR
        FROM dual
        CONNECT BY LEVEL <= LENGTH(REGEXP_REPLACE(ltrim(new_path, '\'), '[^\]+'))

      )
      START WITH PARENT_LEVEL = 0
      CONNECT BY PRIOR LVL = PARENT_LEVEL;

-- add new level nodes to i2b2, i2b2_secure, concept_counts, concept_dimension
      FOR i IN 1 .. paths_tab.COUNT
      LOOP

        current_path := paths_tab(i).path;
        current_path_level := paths_tab(i).lvl - 1;
        current_path_attr_name := paths_tab(i).attr_name;

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
          INSERT INTO i2b2demodata.concept_dimension
          (concept_cd
            , concept_path
            , name_char
            , update_date
            , download_date
            , import_date
            , table_name
          )
            VALUES (concept_id.nextval,
                    current_path,
                    current_path_attr_name,
                    sysdate,
                    sysdate,
                    sysdate,
                    'CONCEPT_DIMENSION');
          COMMIT;

          UPDATE i2b2demodata.concept_dimension
          SET sourcesystem_cd = (SELECT
                                   sourcesystem_cd
                                 FROM i2b2demodata.concept_dimension
                                 WHERE concept_path = new_root_node)
          WHERE concept_path = current_path;

          stepCt := stepCt + 1;
          cz_write_audit(jobId, databaseName, procedureName,
                         'Insert ' || current_path || ' into i2b2demodata.concept_dimension', SQL%ROWCOUNT, stepCt,
                         'Done');
          COMMIT;
        END IF;

-- insert new level nodes to i2b2demodata.concept_counts
        SELECT
          count(*)
        INTO counter
        FROM i2b2demodata.concept_counts
        WHERE concept_path = current_path;

        IF counter = 0
        THEN
          INSERT INTO i2b2demodata.concept_counts (concept_path, parent_concept_path)
            VALUES (current_path, parent_path_node);
          COMMIT;

          UPDATE i2b2demodata.concept_counts
          SET patient_count = (SELECT
                                 patient_count
                               FROM i2b2demodata.concept_counts
                               WHERE concept_path = new_root_node)
          WHERE concept_path = current_path;

          stepCt := stepCt + 1;
          cz_write_audit(jobId, databaseName, procedureName,
                         'Insert ' || current_path || ' into i2b2demodata.concept_counts', SQL%ROWCOUNT, stepCt,
                         'Done');
          COMMIT;
        END IF;

        SELECT
          count(*)
        INTO counter
        FROM i2b2metadata.i2b2
        WHERE c_fullname = current_path;

        IF counter = 0
        THEN
-- create new level path in i2b2
          INSERT INTO i2b2metadata.i2b2 (
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
                    sysdate,
                    current_path,
                    current_path,
                    current_path_attr_name,
                    'FA'
            );
          COMMIT;

          UPDATE i2b2metadata.i2b2
          SET
            (C_SYNONYM_CD,
             C_FACTTABLECOLUMN,
             C_TABLENAME,
             C_COLUMNNAME,
             C_COLUMNDATATYPE,
             C_OPERATOR,
             C_COMMENT,
             DOWNLOAD_DATE,
             IMPORT_DATE,
             M_APPLIED_PATH)
            =
            (SELECT
               C_SYNONYM_CD,
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
             WHERE C_FULLNAME = new_root_node)
          WHERE C_FULLNAME = current_path;
          COMMIT;

          UPDATE i2b2metadata.i2b2
          SET C_BASECODE=(SELECT
                            CONCEPT_CD
                          FROM i2b2demodata.concept_dimension
                          WHERE CONCEPT_PATH = current_path)
          WHERE C_FULLNAME = current_path;

          stepCt := stepCt + 1;
          cz_write_audit(jobId, databaseName, procedureName,
                         'Insert ' || current_path || ' into i2b2metadata.i2b2', SQL%ROWCOUNT, stepCt,
                         'Done');
          COMMIT;

        END IF;

        SELECT
          count(*)
        INTO counter
        FROM i2b2metadata.i2b2_secure
        WHERE c_fullname = current_path;

        IF counter = 0
        THEN
-- create new level path in i2b2_secure
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
                    sysdate,
                    current_path,
                    current_path,
                    current_path_attr_name,
                    'FA');
          COMMIT;

          UPDATE i2b2metadata.i2b2_secure
          SET
            (C_SYNONYM_CD,
             C_FACTTABLECOLUMN,
             C_TABLENAME,
             C_COLUMNNAME,
             C_COLUMNDATATYPE,
             C_OPERATOR,
             C_COMMENT,
             DOWNLOAD_DATE,
             IMPORT_DATE,
             M_APPLIED_PATH,
             SECURE_OBJ_TOKEN)
            =
            (SELECT
               C_SYNONYM_CD,
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
             WHERE C_FULLNAME = parent_path_node)
          WHERE C_FULLNAME = current_path;
          COMMIT;

          UPDATE i2b2metadata.i2b2_secure
          SET C_BASECODE=(SELECT
                            CONCEPT_CD
                          FROM i2b2demodata.concept_dimension
                          WHERE CONCEPT_PATH = current_path)
          WHERE C_FULLNAME = current_path;

          stepCt := stepCt + 1;
          cz_write_audit(jobId, databaseName, procedureName,
                         'Insert ' || current_path || ' into i2b2metadata.i2b2_secure', SQL%ROWCOUNT, stepCt,
                         'Done');

        END IF;

      END IF;
        parent_path_node := current_path;
      END LOOP;

    END IF;


    UPDATE i2b2metadata.i2b2
    SET C_HLEVEL = (length(C_FULLNAME) - nvl(length(replace(C_FULLNAME, '\')), 0)) / length('\') - 2
    WHERE c_fullname LIKE new_path || '%';
    COMMIT;


    UPDATE i2b2metadata.i2b2_secure
    SET C_HLEVEL = (length(C_FULLNAME) - nvl(length(replace(C_FULLNAME, '\')), 0)) / length('\') - 2
    WHERE c_fullname LIKE new_path || '%';
    COMMIT;

    EXCEPTION
    WHEN old_study_missed THEN
    cz_write_audit(jobId, databasename, procedurename, 'Please select exists study path to move', 1, stepCt, 'ERROR');
    cz_error_handler(jobid, procedurename);
    cz_end_audit(jobId, 'FAIL');
    DBMS_OUTPUT.PUT_LINE('old_study_missed');

    WHEN new_study_exists THEN
    cz_write_audit(jobId, databasename, procedurename, 'Selected path to move already exists in db', 1, stepCt,
                   'ERROR');
    cz_error_handler(jobid, procedurename);
    cz_end_audit(jobId, 'FAIL');
    DBMS_OUTPUT.PUT_LINE('new_study_exists');

    WHEN duplicated_paths THEN
    cz_write_audit(jobId, databasename, procedurename, 'Please select different old and new paths', 1, stepCt, 'ERROR');
    cz_error_handler(jobid, procedurename);
    cz_end_audit(jobId, 'FAIL');
    DBMS_OUTPUT.PUT_LINE('duplicated_paths');

    WHEN OTHERS THEN
--Handle errors.
    cz_error_handler(jobID, procedureName);
--End Proc
    cz_end_audit(jobID, 'FAIL');
  END;