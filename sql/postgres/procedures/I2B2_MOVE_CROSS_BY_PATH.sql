--DROP FUNCTION i2b2_move_study_by_path(character varying,character varying,numeric);

CREATE OR REPLACE
FUNCTION I2B2_MOVE_CROSS_BY_PATH
  (old_path_in  CHARACTER VARYING,
   new_path_in  CHARACTER VARYING,
   saveSecurity CHARACTER VARYING, -- Doesn't use now, but save for future modification
   currentJobID NUMERIC DEFAULT -1
  )
  RETURNS INTEGER AS
$BODY$

DECLARE
  --Audit variables
  newJobFlag    INTEGER;
  databaseName  VARCHAR(100);
  procedureName VARCHAR(100);
  jobID         NUMERIC(18, 0);
  stepCt        NUMERIC(18, 0);
  tText         VARCHAR(2000);
  rtnCd         INTEGER;
  rowCt         NUMERIC(18, 0);
  errorNumber   CHARACTER VARYING;
  errorMessage  CHARACTER VARYING;
  x                  TEXT;
  genPath            TEXT; --VARCHAR(2000);
  rCount             INTEGER;
  trialId            VARCHAR(2000);

  old_path           VARCHAR(2000);
  new_path           VARCHAR(2000);
  old_root_node      VARCHAR(2000);
  old_study_path     VARCHAR(2000);
  new_root_node      VARCHAR(2000);
  new_root_node_name VARCHAR(2000);
  new_path_last_node_name VARCHAR(2000);
  rowsExists              INTEGER;
  counter                 INTEGER;
  substringPos            INTEGER;
  old_level_num           INTEGER;
  new_level_num           INTEGER;
  current_path            TEXT;
  new_paths               TEXT [];
  old_paths               TEXT [];
  is_sub_node             BOOLEAN;

  pCount INTEGER;
  rPath  VARCHAR;

    oldTree CURSOR IS
    SELECT DISTINCT c_fullname
    FROM i2b2metadata.i2b2
    WHERE c_fullname LIKE old_path || '%' ESCAPE '`';
BEGIN

  --Audit JOB Initialization
  stepCt := 0;

  --Set Audit Parameters
  newJobFlag := 0; -- False (Default)

  jobID := currentJobID;
  databaseName := current_schema();
  procedureName := 'I2B2_MOVE_CROSS_BY_PATH';

  --If Job ID does not exist, then this is a single procedure run and we need to create it
  IF (jobID IS NULL OR jobID < 1)
  THEN
    newJobFlag := 1; -- True
    SELECT cz_start_audit(procedureName, databaseName)
    INTO jobID;
  END IF;

  old_path := trim(old_path_in);
  new_path := trim(new_path_in);

  stepCt := 0;
  stepCt := stepCt + 1;
  tText := 'Start I2B2_MOVE_CROSS_BY_PATH from ' || coalesce(old_path, '<NULL>') || ' to ' ||
           coalesce(new_path, '<NULL>');
  SELECT cz_write_audit(jobId, databaseName, procedureName, tText, 0, stepCt, 'Done')
  INTO rtnCd;

  IF old_path IS NULL OR new_path IS NULL
     OR old_path = '' OR new_path = ''
  THEN
    stepCt := stepCt + 1;
    SELECT
      cz_write_audit(jobId, databaseName, procedureName, 'New or old path is empty. Please check input parameters', 0,
                     stepCt, 'Done')
    INTO rtnCd;
    SELECT cz_error_handler(jobID, procedureName, '-1', 'Application raised error')
    INTO rtnCd;
    SELECT cz_end_audit(jobID, 'FAIL')
    INTO rtnCd;
    RETURN -16;
  END IF;

  -- update slashes
  old_path := REGEXP_REPLACE('\' || old_path || '\', '(\\){2,}', '\', 'g');
  new_path := REGEXP_REPLACE('\' || new_path || '\', '(\\){2,}', '\', 'g');

  -- check new level need to be added
  SELECT length(old_path) - length(replace(old_path, '\', ''))
  INTO old_level_num;

  SELECT length(new_path) - length(replace(new_path, '\', ''))
  INTO new_level_num;

  SELECT count(*)
  INTO pCount
  FROM i2b2metadata.i2b2
  WHERE c_fullname LIKE old_path || '%' ESCAPE '`';

  IF new_level_num < 3 AND pCount = 1
  THEN
    stepCt := stepCt + 1;
    SELECT cz_write_audit(jobId, databaseName, procedureName, 'Please choose folder for move', 0, stepCt, 'Done')
    INTO rtnCd;
    SELECT cz_error_handler(jobID, procedureName, '-1', 'Application raised error')
    INTO rtnCd;
    SELECT cz_end_audit(jobID, 'FAIL')
    INTO rtnCd;
    RETURN -16;
  END IF;

  -- check duplicates
  IF old_path = new_path
  THEN
    stepCt := stepCt + 1;
    SELECT
      cz_write_audit(jobId, databaseName, procedureName, 'Please select different old and new paths', 0, stepCt, 'Done')
    INTO rtnCd;
    SELECT cz_error_handler(jobID, procedureName, '-1', 'Application raised error')
    INTO rtnCd;
    SELECT cz_end_audit(jobID, 'FAIL')
    INTO rtnCd;
    RETURN -16;
  END IF;
  -- check old root node exists
  SELECT count(*)
  INTO rowsExists
  FROM i2b2metadata.i2b2
  WHERE c_fullname = old_path;

  IF rowsExists = 0
  THEN
    stepCt := stepCt + 1;
    SELECT
      cz_write_audit(jobId, databaseName, procedureName, 'Please select exists study path to move', 0, stepCt, 'Done')
    INTO rtnCd;
    SELECT cz_error_handler(jobID, procedureName, '-1', 'Application raised error')
    INTO rtnCd;
    SELECT cz_end_audit(jobID, 'FAIL')
    INTO rtnCd;
    RETURN -16;
  END IF;

  old_root_node := REGEXP_REPLACE(old_path, '(\\[^\\]*\\).*', '\1');
  new_root_node := REGEXP_REPLACE(new_path, '(\\[^\\]*\\).*', '\1');
  new_root_node_name := REGEXP_REPLACE(new_path, '\\([^\\]*)\\.*', '\1');
  new_path_last_node_name := REGEXP_REPLACE(new_path, '.*\\([^\\]*)\\', '\1');

  is_sub_node := old_path <> old_study_path;

  -- check new path exists
  SELECT count(*)
  INTO rowsExists
  FROM i2b2metadata.i2b2
  WHERE c_fullname = new_path;

  SELECT position(new_path IN old_path)
  INTO substringPos;

  IF rowsExists > 0 AND substringPos = 0
  THEN
    stepCt := stepCt + 1;
    SELECT cz_write_audit(jobId, databaseName, procedureName, 'Study target path is already exists', 0, stepCt, 'Done')
    INTO rtnCd;
    SELECT cz_error_handler(jobID, procedureName, '-1', 'Application raised error')
    INTO rtnCd;
    SELECT cz_end_audit(jobID, 'FAIL')
    INTO rtnCd;
    RETURN -16;
  END IF;

  -- check new root node exists
  SELECT count(*)
  INTO rowsExists
  FROM i2b2metadata.i2b2
  WHERE c_fullname = new_root_node;

  IF rowsExists = 0
  THEN
    -- create new root in table_access,
    BEGIN
      SELECT i2b2_add_root_node(new_root_node_name, jobID)
      INTO rtnCd;
      GET DIAGNOSTICS rowCt := ROW_COUNT;
      EXCEPTION
      WHEN OTHERS
        THEN
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
          INTO rtnCd;
          --End Proc
          SELECT cz_end_audit(jobID, 'FAIL')
          INTO rtnCd;
          RETURN -16;
    END;
    stepCt := stepCt + 1;
    SELECT cz_write_audit(jobId, databaseName, procedureName, 'New root node was added', rowCt, stepCt, 'Done')
    INTO rtnCd;
  END IF;

  -- rename paths in i2b2
  BEGIN
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
        SELECT cz_write_audit(jobId, databaseName, procedureName, 'Rename path ' || rPath.c_fullname || ' in i2b2', 1,
                              stepCt,
                              'Done')
        INTO rtnCd;
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
        SELECT
          cz_write_audit(jobId, databaseName, procedureName, 'Update concept_dimension path ' || rPath.c_fullname, 1,
                         stepCt,
                         'Done')
        INTO rtnCd;
      END IF;
    END LOOP;

    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;

  -- rename c_name in i2b2
  BEGIN
    UPDATE i2b2metadata.i2b2
    SET c_name = new_path_last_node_name
    WHERE c_fullname = new_path;

    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;

  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Update c_name in i2b2', rowCt, stepCt,
                        'Done')
  INTO rtnCd;

  IF is_sub_node
  THEN
    genPath := '';
    FOR x IN SELECT unnest(string_to_array(new_path, '\', ''))
    LOOP
      IF x IS NOT NULL
      THEN
        genPath := concat(genPath, '\', x);

        SELECT count(*)
        INTO rCount
        FROM i2b2demodata.concept_counts
        WHERE concept_path = (genPath || '\');

        IF rCount = 0
        THEN
          SELECT count(*)
          INTO rCount
          FROM i2b2metadata.i2b2
          WHERE c_fullname = (genPath || '\');
          IF rCount = 0
          THEN
            new_paths := array_append(new_paths, genPath || '\');
            stepCt := stepCt + 1;
          END IF;
        END IF;

      END IF;
    END LOOP;

    IF (array_length(new_paths, 1) > 0)
    THEN
      PERFORM cz_write_audit(jobId, databaseName, procedureName,
                             'i2b2_add_nodes  ' || array_to_string(new_paths, ','), 0, stepCt, 'Done');
      PERFORM i2b2_add_nodes(trialId, new_paths, jobId);
    END IF;
  END IF; -- sub node
  IF (NOT is_sub_node)
  THEN
    PERFORM I2B2_CREATE_CONCEPT_COUNTS(new_path, jobId, 'Y');
  END IF;

  -- Fill in levels if levels are added
  SELECT i2b2_fill_in_tree(CASE WHEN is_sub_node
    THEN trialId
                           ELSE NULL END, new_path, jobID)
  INTO rtnCd;

  -- Remove empty levels
  old_paths := array(
      WITH paths_a AS (
          SELECT string_to_array(substring(old_path FROM 2 FOR char_length(old_path) - 2), '\', '') AS path
      )
      SELECT p.c_fullname :: TEXT
      FROM
        (
          SELECT '\' || array_to_string(paths_a.path [1 :n], '\') || '\' AS c_fullname
          FROM paths_a, generate_series(array_length(paths_a.path, 1), 1, -1) n
        ) p
        INNER JOIN i2b2 i2
          ON p.c_fullname = i2.c_fullname
  );

  FOREACH current_path IN ARRAY old_paths LOOP
    IF NOT EXISTS(SELECT c_fullname
                  FROM i2b2
                  WHERE c_fullname LIKE current_path || '_%' ESCAPE '`')
    THEN
      PERFORM i2b2_delete_1_node(current_path);

      stepCt := stepCt + 1;
      SELECT cz_write_audit(jobId, databaseName, procedureName,
                            'Remove empty level: ' || current_path, rowCt, stepCt,
                            'Done')
      INTO rtnCd;
    END IF;
  END LOOP;

  --where (select count(*) from i2b2 where i2b2.c_fullname like i2.c_fullname || '%' escape '`') <= expected_childs;

  -- Update c_hlevels in i2b2
  BEGIN
    UPDATE i2b2metadata.i2b2
    SET C_HLEVEL = (length(C_FULLNAME) - coalesce(length(replace(C_FULLNAME, '\', '')), 0)) / length('\') - 2
    WHERE c_fullname LIKE new_path || '%' ESCAPE '`';
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;

  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName,
                        'Update levels in i2b2metadata.i2b2', rowCt, stepCt,
                        'Done')
  INTO rtnCd;

  PERFORM i2b2_load_security_data(trialId, jobID);

  IF (is_sub_node)
  THEN
    UPDATE i2b2metadata.i2b2
    SET c_visualattributes = 'FAS'
    WHERE c_fullname = old_study_path;
    PERFORM I2B2_CREATE_CONCEPT_COUNTS(old_study_path, jobId, 'Y');
    stepCt := stepCt + 1;
    SELECT cz_write_audit(jobId, databaseName, procedureName, 'Update visual attributes and concept_count', 0, stepCt,
                          'Done')
    INTO rtnCd;
  END IF;

  IF NOT is_sub_node
  THEN
    WITH paths_a AS (
        SELECT string_to_array(substring(new_path FROM 2 FOR char_length(new_path) - 2), '\', '') AS path
    )
    INSERT INTO i2b2_secure
    (
      c_hlevel
      , c_fullname
      , c_name
      , c_synonym_cd
      , c_visualattributes
      , c_totalnum
      , c_basecode
      , c_metadataxml
      , c_facttablecolumn
      , c_tablename
      , c_columnname
      , c_columndatatype
      , c_operator
      , c_dimcode
      , c_comment
      , c_tooltip
      , update_date
      , download_date
      , import_date
      , sourcesystem_cd
      , valuetype_cd
      , secure_obj_token
    )
      SELECT
        i2.c_hlevel     AS c_hlevel,
        i2.c_fullname   AS c_fullname,
        i2.c_name       AS c_name,
        i2.c_synonym_cd AS c_synonym_cd,
        i2.c_visualattributes,
        i2.c_totalnum,
        i2.c_basecode,
        i2.c_metadataxml,
        i2.c_facttablecolumn,
        i2.c_tablename,
        i2.c_columnname,
        i2.c_columndatatype,
        i2.c_operator,
        i2.c_dimcode,
        i2.c_comment,
        i2.c_tooltip,
        i2.update_date,
        i2.download_date,
        i2.import_date,
        i2.sourcesystem_cd,
        i2.valuetype_cd,
        'EXP:PUBLIC'    AS secure_obj_token
      FROM
        (
          SELECT '\' || array_to_string(paths_a.path [1 :n], '\') || '\' AS c_fullname
          FROM paths_a, generate_series(1, array_length(paths_a.path, 1)) n
        ) p
        INNER JOIN i2b2 i2
          ON p.c_fullname = i2.c_fullname
        LEFT JOIN i2b2_secure i2s
          ON p.c_fullname = i2s.c_fullname
      WHERE i2s.c_fullname IS NULL;
  END IF;

  -- check if old root has another child
  SELECT count(c_fullname)
  INTO counter
  FROM i2b2metadata.i2b2
  WHERE c_fullname LIKE old_root_node || '%' ESCAPE '`' AND
                                                    c_fullname NOT IN
                                                    (SELECT c_fullname
                                                     FROM i2b2metadata.i2b2
                                                     WHERE c_fullname LIKE old_path || '%' ESCAPE '`');

  IF old_root_node <> new_root_node AND counter = 0
  THEN

    -- if has not - remove old root node from i2b2, table_access
    BEGIN
      DELETE FROM i2b2metadata.i2b2
      WHERE c_fullname = old_root_node;
      DELETE FROM i2b2metadata.i2b2_secure
      WHERE c_fullname = old_root_node;
      DELETE FROM i2b2metadata.table_access
      WHERE c_fullname = old_root_node;
      DELETE FROM i2b2demodata.concept_dimension
      WHERE concept_path = old_root_node;

      GET DIAGNOSTICS rowCt := ROW_COUNT;
      EXCEPTION
      WHEN OTHERS
        THEN
          errorNumber := SQLSTATE;
          errorMessage := SQLERRM;
          --Handle errors.
          SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
          INTO rtnCd;
          --End Proc
          SELECT cz_end_audit(jobID, 'FAIL')
          INTO rtnCd;
          RETURN -16;
    END;

    stepCt := stepCt + 1;
    SELECT cz_write_audit(jobId, databaseName, procedureName, 'Remove old root node from i2b2, table_access',
                          rowCt, stepCt, 'Done')
    INTO rtnCd;


  END IF;

  ---Cleanup OVERALL JOB if this proc is being run standalone
  IF newJobFlag = 1
  THEN
    SELECT cz_end_audit(jobID, 'SUCCESS')
    INTO rtnCD;
  END IF;

  RETURN 1;
END;

$BODY$
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;

