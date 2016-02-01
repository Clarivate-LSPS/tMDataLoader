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

  old_path                VARCHAR2(2000 BYTE);
  old_study_path					VARCHAR2(2000 BYTE);
  new_path                VARCHAR2(2000 BYTE);
  old_parent_path         VARCHAR2(2000 BYTE);
  new_parent_path         VARCHAR2(2000 BYTE);
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
  is_sub_node							BOOLEAN;
  tmp                     VARCHAR2(700 BYTE);

	old_study_missed EXCEPTION;
	empty_paths EXCEPTION;
	duplicated_paths EXCEPTION;
	new_node_root_exception EXCEPTION;
	new_path_exists_exception EXCEPTION;
	subnode_exists_exception EXCEPTION;
  subfolder_outside_of_study EXCEPTION;

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

    old_parent_path := REGEXP_REPLACE(old_path, '^(.*)\\[^\\]+\\$', '\1');
    new_parent_path := REGEXP_REPLACE(new_path, '^(.*)\\[^\\]+\\$', '\1');
    new_root_node := REGEXP_REPLACE(new_path, '(\\[^\\]*\\).*', '\1');
    new_root_node_name := REGEXP_REPLACE(new_path, '\\([^\\]*)\\.*', '\1');
    new_path_last_node_name := REGEXP_REPLACE(new_path, '.*\\([^\\]*)\\', '\1');

		--check new path is not root node
    IF new_root_node = new_path
    THEN
      RAISE new_node_root_exception;
    END IF;

    select distinct
    	first_value(c_fullname) over (partition by sourcesystem_cd order by c_fullname) into old_study_path
		from i2b2metadata.i2b2
		where sourcesystem_cd = trialId;

		is_sub_node := old_path <> old_study_path;

    IF is_sub_node and (instr(new_path, old_study_path) = 0 or new_path = old_study_path) THEN
      RAISE subfolder_outside_of_study;
    END IF;


-- check new path exists
    SELECT
      count(*)
    INTO rowsExists
    FROM i2b2metadata.i2b2
    WHERE c_fullname = new_path;

    SELECT instr(old_path, new_path) INTO substringPos FROM dual;

    IF rowsExists > 0 and substringPos = 0
    THEN
       RAISE new_path_exists_exception;
    END IF;

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

-- update concept_counts
    UPDATE i2b2demodata.concept_counts
    SET
    	concept_path=replace(concept_path, old_path, new_path),
      parent_concept_path=replace(parent_concept_path, old_path, new_path)
    WHERE concept_path LIKE old_path || '%';
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Rename paths in concept_counts', SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    select count(*) into rCount from i2b2metadata.i2b2 where c_fullname like old_parent_path || '%' and c_visualattributes = 'FAS';

    if (old_parent_path <> new_parent_path) and (rCount = 1) then
      UPDATE i2b2demodata.concept_counts
      SET
        parent_concept_path=replace(parent_concept_path, old_parent_path, new_parent_path)
      WHERE parent_concept_path = old_parent_path || '\';
      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName, 'Update top parent path in concept_counts ', SQL%ROWCOUNT, stepCt, 'Done');
      COMMIT;
    end if;

    genPath := '';
  FOR x IN r1(new_path)
    LOOP
        genPath := concat(concat(genPath, '\'), x.res);

        SELECT count(*) into rCount from i2b2demodata.concept_counts where concept_path = (genPath || '\') ;

        if rCount = 0 THEN
          SELECT count(*) into rCount from i2b2metadata.i2b2_secure where c_fullname = (genPath || '\');
          if rCount = 0 then
            i2b2_add_node(trialId , genPath|| '\', x.res, jobId);

            select count(*) into rCount from i2b2metadata.i2b2
            where c_fullname like (genPath || '\_%')
                  and C_VISUALATTRIBUTES = 'FAS';

            if rCount = 0 then
              I2B2_CREATE_CONCEPT_COUNTS(genPath || '\', jobId, 'Y');
            end if;
            stepCt := stepCt + 1;
            cz_write_audit(jobId, databaseName, procedureName, 'i2b2_add_node genPath ' || genPath || ' new_path ' || new_path || ' rCount ' || rCount, 0, stepCt, 'Done');
          end if;
        end if;
  	END LOOP;

    /*Checked old path if path isn't head node*/
    select count(*) into rCount from i2b2metadata.i2b2 where c_fullname = old_path and c_visualattributes = 'FAS';
    if (rCount = 0) then
      genPath := '';
      FOR x IN r1(old_path) LOOP
        genPath := concat(concat(genPath, '\'), x.res);

        select count(*) into rCount from i2b2metadata.i2b2
          where c_fullname like (genPath || '\_%')
                and C_VISUALATTRIBUTES = 'FAS';

        if (rCount = 0) then
          I2B2_CREATE_CONCEPT_COUNTS(genPath || '\', jobId, 'Y');
        end if;
        stepCt := stepCt + 1;
        cz_write_audit(jobId, databaseName, procedureName, 'Old path rebuild with' || genPath, 0, stepCt, 'Done');
        exit when rCount = 0;
      END LOOP;
    end if;

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
    i2b2_fill_in_tree(case when is_sub_node then trialId else null end, new_path, jobID);

    -- Remove empty levels
    i2b2_remove_empty_parent_nodes(old_path, jobID);

    -- Update security data
    i2b2_load_security_data(jobID);

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

    WHEN subfolder_outside_of_study THEN
    cz_write_audit(jobId, databasename, procedurename, 'Invalid target path: new subfolder path should be inside of study root', 1, stepCt, 'ERROR');
    cz_error_handler(jobid, procedurename);
    cz_end_audit(jobId, 'FAIL');
    DBMS_OUTPUT.PUT_LINE('subfolder_outside_of_study');

    WHEN OTHERS THEN
--Handle errors.
    cz_error_handler(jobID, procedureName);
--End Proc
    cz_end_audit(jobID, 'FAIL');
END;
/
