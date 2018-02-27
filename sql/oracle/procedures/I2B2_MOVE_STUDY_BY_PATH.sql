CREATE OR REPLACE
PROCEDURE "I2B2_MOVE_STUDY_BY_PATH"
  (old_path_in  VARCHAR2,
   new_path_in  VARCHAR2,
   saveSecurity VARCHAR2,
   currentJobID NUMBER := NULL
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
  trialId2                VARCHAR2(700 BYTE);

  old_path                VARCHAR2(2000 BYTE);
  old_study_path					VARCHAR2(2000 BYTE);
  new_path                VARCHAR2(2000 BYTE);
  old_parent_path         VARCHAR2(2000 BYTE);
  new_parent_path         VARCHAR2(2000 BYTE);
  new_root_node           VARCHAR2(700 BYTE);
  new_root_node_name      VARCHAR2(700 BYTE);
  new_path_last_node_name VARCHAR2(700 BYTE);
  new_study_path          VARCHAR2(2000 BYTE);
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
	new_path_is_not_a_study_root EXCEPTION;
	subnode_exists_exception EXCEPTION;
  subfolder_outside_of_study EXCEPTION;

  accession_old           VARCHAR2(50);
  accession_new           VARCHAR2(50);
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

    select sourcesystem_cd into trialId from i2b2demodata.concept_dimension
     where concept_path = old_path;

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

    select min(c_fullname) into old_study_path
      from i2b2metadata.i2b2
     where sourcesystem_cd = trialId;

    is_sub_node := old_path <> old_study_path;

    IF is_sub_node and (instr(new_path, old_study_path) = 0 or new_path = old_study_path) THEN
      RAISE subfolder_outside_of_study;
    END IF;

    IF (saveSecurity = 'Y') THEN
        begin
            select sourcesystem_cd into trialId2 from i2b2demodata.concept_dimension
             where concept_path = new_path;
        exception
            when no_data_found then null;
        end;
        IF trialId2 is not NULL THEN
            select min(c_fullname) into new_study_path
              from i2b2metadata.i2b2
             where sourcesystem_cd = trialId2;
            IF new_path <> new_study_path THEN
                RAISE new_path_is_not_a_study_root;
            END IF;

            select secure_obj_token INTO accession_new from i2b2metadata.i2b2_secure where c_fullname = new_path;
            select secure_obj_token INTO accession_old from i2b2metadata.i2b2_secure where c_fullname = old_path;
            accession_new := replace(accession_new, 'EXP:', '');
            accession_old := replace(accession_old, 'EXP:', '');

            -- Deleted security configuration from first study
            DELETE FROM i2b2demodata.study WHERE study_id = accession_old;
            DELETE FROM biomart.bio_experiment WHERE accession = accession_old;
            DELETE FROM biomart.bio_data_uid WHERE unique_id = 'EXP:'||accession_old;
            DELETE FROM searchapp.search_secure_object WHERE bio_data_unique_id = 'EXP:'||accession_old;
            COMMIT;
            --Changed accession to new path
            UPDATE biomart.bio_experiment SET accession = accession_old WHERE accession = accession_new;
            UPDATE biomart.bio_data_uid SET unique_id = 'EXP:'||accession_old WHERE unique_id = 'EXP:'||accession_new;
            UPDATE searchapp.search_secure_object SET bio_data_unique_id = 'EXP:'||accession_old WHERE bio_data_unique_id = 'EXP:'||accession_new;
            COMMIT;
            stepCt := stepCt + 1;
            cz_write_audit(jobId,databaseName,procedureName,'Security configuration changed',0,stepCt,'Done');

            I2B2_DELETE_ALL_DATA(null, new_path, jobID);

            stepCt := stepCt + 1;
            cz_write_audit(jobId,databaseName,procedureName,'Study '|| new_path || ' deleted!',0,stepCt,'Done');
        ELSE
            stepCt := stepCt + 1;
            cz_write_audit(jobId,databaseName,procedureName,'No study found with path '||new_path||'. Ignoring save security settings option.',0,stepCt,'Done');
        END IF;
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

    if (old_parent_path <> new_parent_path) then
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
            if is_sub_node and length(genPath) > length(old_study_path) then
              i2b2_add_node(trialId , genPath|| '\', x.res, jobId);
            else
              i2b2_add_node(null , genPath|| '\', x.res, jobId);
            end if;
          end if;
        end if;
  	END LOOP;
    if (not is_sub_node) THEN
      I2B2_CREATE_CONCEPT_COUNTS(new_path||'\', jobId, 'Y');
    END IF ;

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

    --Update head node visual attributes

    if (is_sub_node) THEN
      UPDATE i2b2metadata.i2b2
      SET c_visualattributes = 'FAS'
      WHERE c_fullname = old_study_path;
      I2B2_CREATE_CONCEPT_COUNTS(old_study_path, jobId, 'Y');
    end if;

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

    WHEN new_path_is_not_a_study_root THEN
    cz_write_audit(jobId, databasename, procedurename, 'You can only save security settings when the target is the top node for the study', 1, stepCt, 'ERROR');
    cz_error_handler(jobid, procedurename);
    cz_end_audit(jobId, 'FAIL');
    DBMS_OUTPUT.PUT_LINE('new_path_is_not_a_study_root');

    WHEN OTHERS THEN
--Handle errors.
    cz_error_handler(jobID, procedureName);
--End Proc
    cz_end_audit(jobID, 'FAIL');
END;
/
