CREATE OR REPLACE
PROCEDURE "I2B2_MOVE_STUDY_BY_PATH"
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
  x                       VARCHAR2(700 BYTE);
  genPath                 VARCHAR2(700 BYTE);
  rCount                  INT;
  trialId                 VARCHAR2(700 BYTE);

  old_path                VARCHAR2(700 BYTE);
  new_path                VARCHAR2(700 BYTE);
  old_root_node           VARCHAR2(700 BYTE);
  new_root_node           VARCHAR2(700 BYTE);
  new_root_node_name      VARCHAR2(700 BYTE);
  new_path_last_node_name VARCHAR2(700 BYTE);
  rowsExists              INT;
  counter                 INT;
  substringPos            INT;
  substringPos2         INT;
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
    empty_paths EXCEPTION;
    duplicated_paths EXCEPTION;
    new_node_root_exception EXCEPTION;
    new_path_exists_exception EXCEPTION;
    subnode_exists_exception EXCEPTION;

  cursor r1(path VARCHAR2) is
    select regexp_substr(path,'[^\\]+', 1, level) as res from dual
    connect by regexp_substr(path, '[^\\]+', 1, level) is not null;

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

    old_path := trim(old_path_in);
    new_path := trim(new_path_in);

    stepCt := stepCt + 1;
    tText := 'Start i2b2_move_study_by_path from ' || nvl(old_path, '<NULL>') || ' to ' || nvl(new_path, '<NULL>');
    cz_write_audit(jobId, databaseName, procedureName, tText, 0, stepCt, 'Done');

    IF old_path IS NULL OR new_path IS NULL THEN
      RAISE empty_paths;
    END IF;
    old_path := regexp_replace('\' || old_path || '\', '\\{2,}', '\');
    new_path := regexp_replace('\' || new_path || '\', '\\{2,}', '\');

    select sourcesystem_cd into trialId from i2b2demodata.concept_dimension
    where concept_path = old_path;

     -- check first and last /
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

    -- check duplicates
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
    IF rowsExists = 0
    THEN
      RAISE old_study_missed;
    END IF;

    old_root_node := REGEXP_REPLACE(old_path, '(\\(\w|\s)*\\)(.*)', '\1');
--\Test Studies\
    new_root_node := REGEXP_REPLACE(new_path, '(\\(\w|\s)*\\)(.*)', '\1');
-- '\Test Studies 2\';
    new_root_node_name := REGEXP_REPLACE(new_path, '\\((\w|\s)*)\\(.*)', '\1');
--Test Studies 2
    new_path_last_node_name := REGEXP_REPLACE(new_path, '(.*)\\((\w|\s)*)\\', '\2');
-- ClinicalSample

--check new path is not root node
    IF new_root_node = new_path
    THEN
      RAISE new_node_root_exception;
    END IF;

-- check new path exists
    SELECT
      count(*)
    INTO rowsExists
    FROM i2b2metadata.i2b2
    WHERE c_fullname = new_path;

    SELECT instr(old_path, new_path)
              INTO substringPos FROM dual;

    IF rowsExists > 0 and substringPos = 0
    THEN
       RAISE new_path_exists_exception;
    END IF;

-- check that new path is not subnode of exists study
   -- parse new_path to levels
      /*SELECT
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

      FOR i IN 1 .. paths_tab.COUNT
      LOOP
        current_path := paths_tab(i).path;
        current_path_level := paths_tab(i).lvl - 1;

        IF current_path_level > 0 THEN
          SELECT
                count(*)
                INTO rowsExists
                FROM i2b2metadata.i2b2
                WHERE c_fullname = current_path;

          -- check cases with adding/removing new level /a/b/c/ -> /a/b/ and reverse /a/b/-> /a/b/c/
          SELECT instr (old_path, current_path)
          INTO substringPos FROM dual;

          SELECT instr (current_path, old_path)
          INTO substringPos2 FROM dual;

          IF rowsExists > 0 and substringPos = 0 and substringPos2 = 0
          THEN
             RAISE subnode_exists_exception;
          END IF;
        END IF;

      END LOOP;*/


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
       i2b2_add_root_node(new_root_node_name, jobId);
       stepCt := stepCt + 1;
       cz_write_audit(jobId, databaseName, procedureName, 'New root node was added ' || new_root_node_name, SQL%ROWCOUNT, stepCt, 'Done');
      COMMIT;
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
      DELETE FROM i2b2demodata.concept_dimension
      WHERE concept_path = old_root_node;

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

    genPath := '';
  FOR x IN r1(new_path)
    LOOP
        genPath := concat(concat(genPath, '\'), x.res);

        SELECT count(*) into rCount from i2b2demodata.concept_counts where concept_path = (genPath || '\') ;

        if rCount = 0 THEN
          SELECT count(*) into rCount from i2b2metadata.i2b2_secure where c_fullname = (genPath || '\');
          if rCount = 0 then
            i2b2_add_node(trialId , genPath|| '\', x.res, jobId);
            I2B2_CREATE_CONCEPT_COUNTS(genPath || '\', jobId, 'Y');
            stepCt := stepCt + 1;
            cz_write_audit(jobId, databaseName, procedureName, 'i2b2_add_node genPath ' || genPath, 0, stepCt, 'Done');
          end if;
        end if;

  END LOOP;

-- check new level need to be added
-- Fill in levels if levels are added
    i2b2_fill_in_tree(null, new_path, jobID);

    -- Remove empty levels
    IF new_level_num <= old_level_num
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
          REGEXP_SUBSTR(ltrim(old_path, '\'), '[^\]+', 1, LEVEL) STR
        FROM dual
        CONNECT BY LEVEL <= LENGTH(REGEXP_REPLACE(ltrim(old_path, '\'), '[^\]+'))

      )
      START WITH PARENT_LEVEL = 0
      CONNECT BY PRIOR LVL = PARENT_LEVEL;

-- add new level nodes to i2b2, i2b2_secure, concept_counts, concept_dimension
      FOR i IN REVERSE 1 .. paths_tab.COUNT
      LOOP
        current_path := paths_tab(i).path;
        SELECT count(*) INTO rowsExists FROM i2b2metadata.i2b2
        WHERE c_fullname LIKE current_path || '%';

        IF rowsExists = 1
        THEN
          i2b2_delete_1_node(current_path);
          cz_write_audit(jobId, databaseName, procedureName,
                           'Remove empty level: ' || current_path, SQL%ROWCOUNT, stepCt,
                           'Done');
        END IF;

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

    WHEN empty_paths THEN
    cz_write_audit(jobId, databasename, procedurename, 'New or old path is empty. Please check input parameters', 1, stepCt,
                   'ERROR');
    cz_error_handler(jobid, procedurename);
    cz_end_audit(jobId, 'FAIL');
    DBMS_OUTPUT.PUT_LINE('empty_paths');

    WHEN duplicated_paths THEN
    cz_write_audit(jobId, databasename, procedurename, 'Please select different old and new paths', 1, stepCt, 'ERROR');
    cz_error_handler(jobid, procedurename);
    cz_end_audit(jobId, 'FAIL');
    DBMS_OUTPUT.PUT_LINE('duplicated_paths');

    WHEN new_node_root_exception THEN
    cz_write_audit(jobId, databasename, procedurename, 'Please select new study target path: it can not be root node', 1, stepCt, 'ERROR');
    cz_error_handler(jobid, procedurename);
    cz_end_audit(jobId, 'FAIL');
    DBMS_OUTPUT.PUT_LINE('new_node_root_exception');

    WHEN new_path_exists_exception THEN
    cz_write_audit(jobId, databasename, procedurename, 'Please select new study target path', 1, stepCt, 'ERROR');
    cz_error_handler(jobid, procedurename);
    cz_end_audit(jobId, 'FAIL');
    DBMS_OUTPUT.PUT_LINE('new_path_exists_exception');

    WHEN subnode_exists_exception THEN
    cz_write_audit(jobId, databasename, procedurename, 'Please select new study target path: target path can not be subnode of exists study', 1, stepCt, 'ERROR');
    cz_error_handler(jobid, procedurename);
    cz_end_audit(jobId, 'FAIL');
    DBMS_OUTPUT.PUT_LINE('subnode_exists_exception');

    WHEN OTHERS THEN
--Handle errors.
    cz_error_handler(jobID, procedureName);
--End Proc
    cz_end_audit(jobID, 'FAIL');
END;
/
