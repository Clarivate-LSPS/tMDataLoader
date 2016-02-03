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
    old_study_path					VARCHAR(2000);
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
    current_path            TEXT;
    current_path_level      INTEGER;
    current_path_attr_name  VARCHAR(2000);
    tmp                     VARCHAR(2000);
    new_paths               TEXT[];
    old_paths               TEXT[];
    is_sub_node							BOOLEAN;

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

    select distinct
      first_value(c_fullname) over (partition by sourcesystem_cd order by c_fullname) into old_study_path
    from i2b2metadata.i2b2
    where sourcesystem_cd = trialId;

    is_sub_node := old_path <> old_study_path;

    IF is_sub_node and (position(old_study_path in new_path) = 0 or old_study_path = new_path) THEN
      stepCt := stepCt + 1;
      select cz_write_audit(jobId,databaseName,procedureName,
                            'Invalid target path: new subfolder path should be inside of study root',0,stepCt,'Done') into rtnCd;
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

    -- TODO: check that new path is not subnode of exists study

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

    -- if has not - remove old root node from i2b2, table_access
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
      select cz_write_audit(jobId, databaseName, procedureName, 'Remove old root node from i2b2, table_access',
                     rowCt, stepCt, 'Done')  into rtnCd;


    END IF;


-- check new level need to be added
    SELECT
      length(old_path) - length(replace(old_path, '\', ''))
    INTO old_level_num;

    SELECT
      length(new_path) - length(replace(new_path, '\', ''))
    INTO new_level_num;

    -- rename paths in i2b2
    begin
    UPDATE i2b2metadata.i2b2
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
    select cz_write_audit(jobId, databaseName, procedureName, 'Rename paths in i2b2', rowCt, stepCt,
                   'Done') into rtnCd;


-- rename c_name in i2b2
    begin
    UPDATE i2b2metadata.i2b2
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
    select cz_write_audit(jobId, databaseName, procedureName, 'Update c_name in i2b2', rowCt, stepCt,
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

    if is_sub_node then
      genPath := '';
      FOR x IN select unnest(string_to_array(new_path,'\',''))
      LOOP
        if x is not null then
          genPath := concat(genPath, '\', x);

          SELECT count(*) into rCount from i2b2demodata.concept_counts where concept_path = (genPath || '\') ;

          if rCount = 0 THEN
            SELECT count(*) into rCount from i2b2metadata.i2b2 where c_fullname = (genPath || '\');
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
        PERFORM i2b2_add_nodes(trialId , new_paths, jobId);

        FOR i IN array_lower(new_paths, 1) .. array_upper(new_paths, 1)
        LOOP
          PERFORM I2B2_CREATE_CONCEPT_COUNTS(new_paths[i], jobId, 'Y');
        END LOOP;
      END IF;
    end if; -- sub node

    -- Fill in levels if levels are added
    select i2b2_fill_in_tree(case when is_sub_node then trialId else null end, new_path, jobID) into rtnCd;

    -- Remove empty levels
    old_paths := array(
        with paths_a as (
          select string_to_array(substring(old_path from 2 for char_length(old_path) - 2), '\', '') as path
        )
        select
          p.c_fullname::text
        from
          (
             select
               '\' || array_to_string(paths_a.path[1:n], '\') || '\' as c_fullname
             from paths_a, generate_series(array_length(paths_a.path, 1), 1, -1) n
           ) p
          inner join i2b2 i2
            on p.c_fullname = i2.c_fullname
    );

    FOREACH current_path IN ARRAY old_paths LOOP
      IF NOT EXISTS(SELECT c_fullname FROM i2b2 WHERE c_fullname LIKE current_path || '_%' ESCAPE '`') THEN
        PERFORM i2b2_delete_1_node(current_path);

        stepCt := stepCt + 1;
        select cz_write_audit(jobId, databaseName, procedureName,
                              'Remove empty level: ' || current_path, rowCt, stepCt,
                              'Done') into rtnCd;
      END IF;
    END LOOP;

      --where (select count(*) from i2b2 where i2b2.c_fullname like i2.c_fullname || '%' escape '`') <= expected_childs;

    -- Update c_hlevels in i2b2
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

    perform i2b2_load_security_data(trialId, jobID);

    if (is_sub_node) THEN
      UPDATE i2b2metadata.i2b2
      SET c_visualattributes = 'FAS'
      WHERE c_fullname = old_study_path;
      PERFORM I2B2_CREATE_CONCEPT_COUNTS(old_study_path, jobId, 'Y');
      stepCt := stepCt + 1;
      select cz_write_audit(jobId,databaseName,procedureName,'Update visual attributes and concept_count',0,stepCt,'Done') into rtnCd;
    end if;

    if not is_sub_node then
      with paths_a as (
          select string_to_array(substring(new_path from 2 for char_length(new_path) - 2), '\', '') as path
      )
      insert into i2b2_secure
      (
        c_hlevel
        ,c_fullname
        ,c_name
        ,c_synonym_cd
        ,c_visualattributes
        ,c_totalnum
        ,c_basecode
        ,c_metadataxml
        ,c_facttablecolumn
        ,c_tablename
        ,c_columnname
        ,c_columndatatype
        ,c_operator
        ,c_dimcode
        ,c_comment
        ,c_tooltip
        ,update_date
        ,download_date
        ,import_date
        ,sourcesystem_cd
        ,valuetype_cd
        ,secure_obj_token
      )
      select
         i2.c_hlevel as c_hlevel
        ,i2.c_fullname as c_fullname
        ,i2.c_name as c_name
        ,i2.c_synonym_cd as c_synonym_cd
        ,i2.c_visualattributes
        ,i2.c_totalnum
        ,i2.c_basecode
        ,i2.c_metadataxml
        ,i2.c_facttablecolumn
        ,i2.c_tablename
        ,i2.c_columnname
        ,i2.c_columndatatype
        ,i2.c_operator
        ,i2.c_dimcode
        ,i2.c_comment
        ,i2.c_tooltip
        ,i2.update_date
        ,i2.download_date
        ,i2.import_date
        ,i2.sourcesystem_cd
        ,i2.valuetype_cd
        ,'EXP:PUBLIC' as secure_obj_token
      from
        (
          select '\' || array_to_string(paths_a.path[1:n], '\') || '\' as c_fullname
          from paths_a, generate_series(1, array_length(paths_a.path, 1)) n
        ) p
        inner join i2b2 i2
          on p.c_fullname = i2.c_fullname
        left join i2b2_secure i2s
          on p.c_fullname = i2s.c_fullname
      where i2s.c_fullname is null;
    end if;


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
