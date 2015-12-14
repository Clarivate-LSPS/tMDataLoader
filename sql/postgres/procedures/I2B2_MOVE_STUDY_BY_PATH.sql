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
    x                       text;
    genPath                 text;--VARCHAR(2000);
    rCount                  INTEGER ;
    trialId                 VARCHAR(2000);

    old_path                VARCHAR(2000);
    new_path                VARCHAR(2000);
    old_root_node           VARCHAR(2000);
    new_root_node           VARCHAR(2000);
    new_root_node_name      VARCHAR(2000);
    new_path_last_node_name VARCHAR(2000);
    rowsExists              INTEGER;
    counter                 INTEGER;
    substringPos            INTEGER;
    substringPos2           INTEGER;
    lvl_num_to_remove       INTEGER;
    old_level_num           INTEGER;
    new_level_num           INTEGER;
    parent_path_node        VARCHAR(2000);
    current_path            VARCHAR(2000);
    current_path_level      INTEGER;
    current_path_attr_name  VARCHAR(2000);
    tmp                     VARCHAR(2000);
    new_paths               TEXT[];

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

    old_path := trim(old_path_in);
    new_path := trim(new_path_in);

    stepCt := 0;
    stepCt := stepCt + 1;
    tText := 'Start i2b2_move_study_by_path from ' || coalesce(old_path, '<NULL>') || ' to ' || coalesce(new_path, '<NULL>');
    select cz_write_audit(jobId,databaseName,procedureName,tText,0,stepCt,'Done') into rtnCd;

    IF old_path is null or new_path is null
      or old_path = '' or new_path = ''
    THEN
      stepCt := stepCt + 1;
      select cz_write_audit(jobId,databaseName,procedureName,'New or old path is empty. Please check input parameters',0,stepCt,'Done') into rtnCd;
      select cz_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
      select cz_end_audit (jobID, 'FAIL') into rtnCd;
      return -16;
    END IF;

    -- update slashes
    old_path := REGEXP_REPLACE('\' || old_path || '\','(\\){2,}', '\','g');
    new_path := REGEXP_REPLACE('\' || new_path || '\','(\\){2,}', '\','g');

    select sourcesystem_cd into trialId from i2b2demodata.concept_dimension
    where concept_path = old_path;

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

    old_root_node := REGEXP_REPLACE(old_path, '(\\[^\\]*\\).*', '\1');
    new_root_node := REGEXP_REPLACE(new_path, '(\\[^\\]*\\).*', '\1');
    new_root_node_name := REGEXP_REPLACE(new_path, '\\([^\\]*)\\.*', '\1');
    new_path_last_node_name := REGEXP_REPLACE(new_path, '.*\\([^\\]*)\\', '\1');

-- check new path is not root node
    IF new_root_node = new_path
    THEN
      stepCt := stepCt + 1;
      select cz_write_audit(jobId,databaseName,procedureName,
            'Please select new study target path: it can not be root node',0,stepCt,'Done') into rtnCd;
      select cz_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
      select cz_end_audit (jobID, 'FAIL') into rtnCd;
      return -16;
    END IF;

-- check new path exists
    SELECT
      count(*)
    INTO rowsExists
    FROM i2b2metadata.i2b2
    WHERE c_fullname = new_path;

    SELECT position (new_path in old_path)
              INTO substringPos;

    IF rowsExists > 0 and substringPos = 0
    THEN
      stepCt := stepCt + 1;
      select cz_write_audit(jobId,databaseName,procedureName,'Study target path is already exists',0,stepCt,'Done') into rtnCd;
      select cz_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
      select cz_end_audit (jobID, 'FAIL') into rtnCd;
      return -16;
    END IF;

-- check that new path is not subnode of exists study
        -- parse new_path to levels
        /*DROP TABLE IF EXISTS temp_t CASCADE;
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

        select count(*) INTO counter from temp_t_paths where lvl > 1;
        IF counter > 0
          THEN
          FOR i IN 1..counter LOOP
              SELECT node_path INTO current_path FROM temp_t_paths WHERE lvl=i+1;

              SELECT
                count(*)
              INTO rowsExists
              FROM i2b2metadata.i2b2
              WHERE c_fullname = current_path;

              -- check cases with adding/removing new level /a/b/c/ -> /a/b/ and reverse /a/b/-> /a/b/c/
              SELECT position (current_path in old_path)
              INTO substringPos;

              SELECT position (old_path in current_path)
              INTO substringPos2;

              IF rowsExists > 0 and substringPos = 0 and substringPos2 = 0
              THEN
                stepCt := stepCt + 1;
                select cz_write_audit(jobId,databaseName,procedureName,
                'Please select new study target path: target path can not be subnode of exists study',0,stepCt,'Done') into rtnCd;
                select cz_error_handler (jobID, procedureName, '-1', 'Application raised error') into rtnCd;
                select cz_end_audit (jobID, 'FAIL') into rtnCd;
                return -16;
              END IF;

          END LOOP;
        END IF;*/

-- check new root node exists

    SELECT
      count(*)
    INTO rowsExists
    FROM i2b2metadata.i2b2
    WHERE c_fullname = new_root_node;

    IF rowsExists = 0
    THEN
    -- create new root in table_access,
      BEGIN
      select i2b2_add_root_node(new_root_node_name, jobID) into rtnCd;
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
      END;
      stepCt := stepCt + 1;
		  select cz_write_audit(jobId,databaseName,procedureName,'New root node was added',rowCt,stepCt,'Done') into rtnCd;
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

    genPath := '';
    FOR x IN select unnest(string_to_array(new_path,'\',''))
    LOOP
      if x is not null then
        genPath := concat(genPath, '\', x);

        SELECT count(*) into rCount from i2b2demodata.concept_counts where concept_path = (genPath || '\') ;

        if rCount = 0 THEN
          SELECT count(*) into rCount from i2b2metadata.i2b2_secure where c_fullname = (genPath || '\');
          if rCount = 0 then
            new_paths := array_append(new_paths,genPath || '\');
            stepCt := stepCt + 1;
          end if;
        end if;

      end if;
    END LOOP;

    IF (array_length(new_paths, 1) > 0) THEN
      PERFORM cz_write_audit(jobId, databaseName, procedureName,
                             'i2b2_add_nodes  ' || array_to_string(new_paths, ',') , 0, stepCt, 'Done');
      PERFORM i2b2_add_nodes(trialId , new_paths, jobId, false);

      FOR i IN array_lower(new_paths, 1) .. array_upper(new_paths, 1)
      LOOP
        PERFORM I2B2_CREATE_CONCEPT_COUNTS(new_paths[i], jobId, 'Y');
      END LOOP;
    END IF;

    -- Fill in levels if levels are added
    select i2b2_fill_in_tree(null, new_path, jobID) into rtnCd;

    -- Remove empty levels
    IF new_level_num <= old_level_num
    THEN
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

        FOR i IN REVERSE (select count(*) from temp_t_paths)..1 LOOP
              SELECT node_path INTO current_path FROM temp_t_paths WHERE lvl=i;
              SELECT count(*) INTO rowsExists FROM i2b2metadata.i2b2
              WHERE c_fullname LIKE current_path || '%' ESCAPE '`';

              IF rowsExists = 1
              THEN
                select i2b2_delete_1_node(current_path) into rtnCd;
                select cz_write_audit(jobId, databaseName, procedureName,
                                 'Remove empty level: ' || current_path, rowCt, stepCt,
                                 'Done') into rtnCd;
              END IF;
         END LOOP;

        stepCt := stepCt + 1;
        select cz_write_audit(jobId, databaseName, procedureName,
                                 'Remove empty levels from i2b2', rowCt, stepCt,
                                 'Done') into rtnCd;
    END IF;

    -- Update c_hlevels in i2b2 and i2b2_secure
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
