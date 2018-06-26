CREATE OR REPLACE
PROCEDURE "I2B2_MOVE_CROSS_BY_PATH"
  (old_path_in  VARCHAR2,
   new_path_in  VARCHAR2,
   saveSecurity VARCHAR2,
   currentJobID NUMBER := NULL
  )
AS
--Audit variables
  newJobFlag    INTEGER(1);
  databaseName  VARCHAR(100);
  procedureName VARCHAR(100);
  jobID         NUMBER(18, 0);
  stepCt        NUMBER(18, 0);
  tText         VARCHAR2(2000);
  x             VARCHAR2(700 BYTE);
  genPath       VARCHAR2(700 BYTE);
  rCount        INT;
  trialId       VARCHAR2(700 BYTE);
  trialId2                VARCHAR2(700 BYTE);

  old_path                VARCHAR2(2000 BYTE);
  old_study_path          VARCHAR2(2000 BYTE);
  new_path                VARCHAR2(2000 BYTE);
  old_parent_path         VARCHAR2(2000 BYTE);
  new_parent_path         VARCHAR2(2000 BYTE);
  new_root_node           VARCHAR2(700 BYTE);
  new_root_node_name      VARCHAR2(700 BYTE);
  new_path_last_node_name VARCHAR2(700 BYTE);
  new_study_path          VARCHAR2(2000 BYTE);
  rowsExists        INT;
  counter           INT;
  substringPos      INT;
  substringPos2     INT;
  lvl_num_to_remove INT;
  old_level_num     INT;
  new_level_num     INT;
  is_sub_node       BOOLEAN;
  tmp               VARCHAR2(700 BYTE);
  studyNum          NUMBER(18, 0);
  studyNumNew NUMBER(18, 0);
  pCount INT;

    old_study_missed EXCEPTION;
    empty_paths EXCEPTION;
    duplicated_paths EXCEPTION;
    new_node_root_exception EXCEPTION;
    new_path_exists_exception EXCEPTION;
    new_path_is_not_a_study_root EXCEPTION;
    subnode_exists_exception EXCEPTION;
    subfolder_outside_of_study EXCEPTION;
    need_set_folder EXCEPTION;

  accession_old VARCHAR2(50);
  accession_new VARCHAR2(50);
  CURSOR r1(path VARCHAR2) IS
    SELECT regexp_substr(path, '[^\\]+', 1, level) AS res
    FROM dual
    CONNECT BY regexp_substr(path, '[^\\]+', 1, level) IS NOT NULL;

  CURSOR oldTree IS
    SELECT DISTINCT c_fullname
    FROM i2b2metadata.i2b2
    WHERE c_fullname LIKE old_path || '%' ESCAPE '`';

  BEGIN

    --Audit JOB Initialization
    stepCt := 0;

    --Set Audit Parameters
    newJobFlag := 0;
    -- False (Default)
    jobID := currentJobID;

    SELECT sys_context('USERENV', 'CURRENT_SCHEMA')
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

    old_path := trim(old_path_in);
    new_path := trim(new_path_in);

    stepCt := stepCt + 1;
    tText := 'Start I2B2_MOVE_CROSS_BY_PATH from ' || nvl(old_path, '<NULL>') || ' to ' || nvl(new_path, '<NULL>');
    cz_write_audit(jobId, databaseName, procedureName, tText, 0, stepCt, 'Done');

    IF old_path IS NULL OR new_path IS NULL
    THEN
      RAISE empty_paths;
    END IF;
    old_path := regexp_replace('\' || old_path || '\', '\\{2,}', '\');
    new_path := regexp_replace('\' || new_path || '\', '\\{2,}', '\');

    -- check new level need to be added
    SELECT length(old_path) - length(replace(old_path, '\'))
    INTO old_level_num
    FROM dual;
    SELECT length(new_path) - length(replace(new_path, '\'))
    INTO new_level_num
    FROM dual;

    SELECT count(*)
    INTO pCount
    FROM i2b2metadata.i2b2
    WHERE c_fullname LIKE old_path || '%' ESCAPE '`';

    IF new_level_num < 3 AND pCount = 1
    THEN
      RAISE need_set_folder;
    END IF;

    -- check duplicates
    IF old_path = new_path
    THEN
      RAISE duplicated_paths;
    END IF;

    -- check old root node exists
    SELECT count(*)
    INTO rowsExists
    FROM i2b2metadata.i2b2
    WHERE c_fullname = old_path;
    IF rowsExists = 0
    THEN
      RAISE old_study_missed;
    END IF;

    SELECT sourcesystem_cd
    INTO trialId
    FROM i2b2demodata.concept_dimension
    WHERE concept_path = old_path;

    old_parent_path := REGEXP_REPLACE(old_path, '^(.*)\\[^\\]+\\$', '\1');
    new_parent_path := REGEXP_REPLACE(new_path, '^(.*)\\[^\\]+\\$', '\1');
    new_root_node := REGEXP_REPLACE(new_path, '(\\[^\\]*\\).*', '\1');
    new_root_node_name := REGEXP_REPLACE(new_path, '\\([^\\]*)\\.*', '\1');
    new_path_last_node_name := REGEXP_REPLACE(new_path, '.*\\([^\\]*)\\', '\1');

    SELECT min(c_fullname)
    INTO old_study_path
    FROM i2b2metadata.i2b2
    WHERE sourcesystem_cd = trialId;

    is_sub_node := old_path <> old_study_path;

    IF is_sub_node AND (instr(new_path, old_study_path) = 0 OR new_path = old_study_path)
    THEN
      RAISE subfolder_outside_of_study;
    END IF;

    -- check new path exists
    SELECT count(*)
    INTO rowsExists
    FROM i2b2metadata.i2b2
    WHERE c_fullname = new_path;

    SELECT instr(old_path, new_path)
    INTO substringPos
    FROM dual;

    IF rowsExists > 0 AND substringPos = 0
    THEN
      RAISE new_path_exists_exception;
    END IF;

    SELECT count(*)
    INTO rowsExists
    FROM i2b2metadata.i2b2
    WHERE c_fullname = new_root_node;

    IF rowsExists = 0
    THEN
      -- create new root in table_access,
      i2b2_add_root_node(new_root_node_name, jobId);
      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName, 'New root node was added ' || new_root_node_name, SQL%ROWCOUNT,
                     stepCt, 'Done');
      COMMIT;
    END IF;

    -- rename paths in i2b2
    FOR rPath IN oldTree
    LOOP
      SELECT count(*)
      INTO pCount
      FROM i2b2metadata.i2b2
      WHERE c_fullname = replace(rPath.c_fullname, old_path, new_path);
      IF (pCount = 0)
      THEN
        UPDATE i2b2metadata.i2b2
        SET c_fullname = replace(c_fullname, old_path, new_path),
          c_dimcode    = replace(c_dimcode, old_path, new_path),
          c_tooltip    = replace(c_tooltip, old_path, new_path)
        WHERE c_fullname = rPath.c_fullname;

        UPDATE i2b2metadata.i2b2_secure
        SET c_fullname = replace(c_fullname, old_path, new_path),
          c_dimcode    = replace(c_dimcode, old_path, new_path),
          c_tooltip    = replace(c_tooltip, old_path, new_path)
        WHERE c_fullname = rPath.c_fullname;

        stepCt := stepCt + 1;
        cz_write_audit(jobId, databaseName, procedureName, 'Rename path ' || rPath.c_fullname || ' in i2b2', 1, stepCt,
                       'Done');
      END IF;

      SELECT count(*)
      INTO pCount
      FROM i2b2demodata.concept_dimension
      WHERE concept_path = replace(rPath.c_fullname, old_path, new_path);

      IF (pCount = 0)
      THEN
        UPDATE i2b2demodata.concept_dimension
        SET concept_path = replace(rPath.c_fullname, old_path, new_path)
        WHERE concept_path = rPath.c_fullname;

        stepCt := stepCt + 1;

        cz_write_audit(jobId, databaseName, procedureName, 'Update concept_dimension path ' || rPath.c_fullname, 1,
                       stepCt, 'Done');
      END IF;
    END LOOP;
    COMMIT;


    genPath := '';
    FOR x IN r1(new_path)
    LOOP
      genPath := concat(concat(genPath, '\'), x.res);

      SELECT count(*)
      INTO rCount
      FROM i2b2demodata.concept_counts
      WHERE concept_path = (genPath || '\');

      IF rCount = 0
      THEN
        SELECT count(*)
        INTO rCount
        FROM i2b2metadata.i2b2_secure
        WHERE c_fullname = (genPath || '\');
        IF rCount = 0
        THEN
          IF is_sub_node AND length(genPath) > length(old_study_path)
          THEN
            i2b2_add_node(trialId, genPath || '\', x.res, jobId);
          ELSE
            i2b2_add_node(NULL, genPath || '\', x.res, jobId);
          END IF;
        END IF;
      END IF;
    END LOOP;
    IF (NOT is_sub_node)
    THEN
      I2B2_CREATE_CONCEPT_COUNTS(new_path || '\', jobId, 'Y');
    END IF;

    UPDATE i2b2metadata.i2b2
    SET C_HLEVEL = (length(C_FULLNAME) - nvl(length(replace(C_FULLNAME, '\')), 0)) / length('\') - 2
    WHERE c_fullname LIKE new_path || '%';
    COMMIT;


    UPDATE i2b2metadata.i2b2_secure
    SET C_HLEVEL = (length(C_FULLNAME) - nvl(length(replace(C_FULLNAME, '\')), 0)) / length('\') - 2
    WHERE c_fullname LIKE new_path || '%';
    COMMIT;

    -- check new level need to be added
    -- Fill in levels if levels are added
    i2b2_fill_in_tree(CASE WHEN is_sub_node
      THEN trialId
                      ELSE NULL END, new_path, jobID);

    -- Remove empty levels
    i2b2_remove_empty_parent_nodes(old_path, jobID);

    -- Update security data
    i2b2_load_security_data(jobID);

    --Update head node visual attributes

    IF (is_sub_node)
    THEN
      UPDATE i2b2metadata.i2b2
      SET c_visualattributes = 'FAS'
      WHERE c_fullname = old_study_path;
      I2B2_CREATE_CONCEPT_COUNTS(old_study_path, jobId, 'Y');
    END IF;

    EXCEPTION
    WHEN need_set_folder
    THEN
      cz_write_audit(jobId, databasename, procedurename, 'Please choose folder for move', 1, stepCt, 'ERROR');
      cz_error_handler(jobid, procedurename);
      cz_end_audit(jobId, 'FAIL');
      DBMS_OUTPUT.PUT_LINE('need_set_folder');

    WHEN old_study_missed
    THEN
      cz_write_audit(jobId, databasename, procedurename, 'Please select exists study path to move', 1, stepCt, 'ERROR');
      cz_error_handler(jobid, procedurename);
      cz_end_audit(jobId, 'FAIL');
      DBMS_OUTPUT.PUT_LINE('old_study_missed');

    WHEN empty_paths
    THEN
      cz_write_audit(jobId, databasename, procedurename, 'New or old path is empty. Please check input parameters', 1,
                     stepCt,
                     'ERROR');
      cz_error_handler(jobid, procedurename);
      cz_end_audit(jobId, 'FAIL');
      DBMS_OUTPUT.PUT_LINE('empty_paths');

    WHEN duplicated_paths
    THEN
      cz_write_audit(jobId, databasename, procedurename, 'Please select different old and new paths', 1, stepCt,
                     'ERROR');
      cz_error_handler(jobid, procedurename);
      cz_end_audit(jobId, 'FAIL');
      DBMS_OUTPUT.PUT_LINE('duplicated_paths');

    WHEN new_node_root_exception
    THEN
      cz_write_audit(jobId, databasename, procedurename, 'Please select new study target path: it can not be root node',
                     1, stepCt, 'ERROR');
      cz_error_handler(jobid, procedurename);
      cz_end_audit(jobId, 'FAIL');
      DBMS_OUTPUT.PUT_LINE('new_node_root_exception');

    WHEN new_path_exists_exception
    THEN
      cz_write_audit(jobId, databasename, procedurename, 'Please select new study target path', 1, stepCt, 'ERROR');
      cz_error_handler(jobid, procedurename);
      cz_end_audit(jobId, 'FAIL');
      DBMS_OUTPUT.PUT_LINE('new_path_exists_exception');

    WHEN subnode_exists_exception
    THEN
      cz_write_audit(jobId, databasename, procedurename,
                     'Please select new study target path: target path can not be subnode of exists study', 1, stepCt,
                     'ERROR');
      cz_error_handler(jobid, procedurename);
      cz_end_audit(jobId, 'FAIL');
      DBMS_OUTPUT.PUT_LINE('subnode_exists_exception');

    WHEN subfolder_outside_of_study
    THEN
      cz_write_audit(jobId, databasename, procedurename,
                     'Invalid target path: new subfolder path should be inside of study root', 1, stepCt, 'ERROR');
      cz_error_handler(jobid, procedurename);
      cz_end_audit(jobId, 'FAIL');
      DBMS_OUTPUT.PUT_LINE('subfolder_outside_of_study');

    WHEN new_path_is_not_a_study_root
    THEN
      cz_write_audit(jobId, databasename, procedurename,
                     'You can only save security settings when the target is the top node for the study', 1, stepCt,
                     'ERROR');
      cz_error_handler(jobid, procedurename);
      cz_end_audit(jobId, 'FAIL');
      DBMS_OUTPUT.PUT_LINE('new_path_is_not_a_study_root');

    WHEN OTHERS
    THEN
      --Handle errors.
      cz_error_handler(jobID, procedureName);
      --End Proc
      cz_end_audit(jobID, 'FAIL');
  END;
/
