CREATE OR REPLACE PROCEDURE i2b2_add_nodes (
    Trial_id VARCHAR2,
    New_paths string_table_t,
    Job_id NUMBER := NULL
)
AS
    -------------------------------------------------------------
    -- Add tree nodes in I2B2. Uses bulk processing.
    -------------------------------------------------------------
    new_nodes string_table_t;
    root_node VARCHAR2(4000);
    root_level INTEGER;
  
    --Audit variables
    job_was_created boolean;
    current_schema_name VARCHAR2(32);
    procedure_name VARCHAR2(32);
    l_job_id INTEGER;
    step INTEGER;
BEGIN
    IF New_paths.COUNT = 0 THEN
        RETURN;
    END IF;

    --Set Audit Parameters
    step := 0;
    l_job_id := Job_id;
    job_was_created := false;
    SELECT sys_context('USERENV', 'CURRENT_SCHEMA') INTO current_schema_name FROM dual;
    procedure_name := $$PLSQL_UNIT;
  
    -- Audit JOB Initialization
    -- If Job ID does not exist, then this is a single procedure run and we need to create it
    IF l_job_id IS NULL OR l_job_id < 1 THEN
        job_was_created := true;
        cz_start_audit(procedure_name, current_schema_name, l_job_id);
    END IF;

    -- Get c_hlevel value. Just refactored code from I2B2_ADD_NODE.
    -- TODO: what is this, why do we need it, and what if there is no matching record in table 'table_access'?
    FOR i IN New_paths.FIRST .. New_paths.LAST LOOP
        root_node := parse_nth_value(New_paths(i), 2, '\');
        EXIT WHEN root_node IS NOT NULL;
    END LOOP;
    IF root_node IS NULL THEN
        step := step + 1;
        cz_write_audit(l_job_id, current_schema_name, procedure_name, 'No paths to add', 0, step, 'Done');
        GOTO cleanup;
    END IF;
    SELECT c_hlevel INTO root_level FROM table_access WHERE c_name = root_node;

    -- Delete existing data
    FORALL i IN New_paths.FIRST .. New_paths.LAST
        DELETE FROM observation_fact WHERE concept_cd IN (SELECT c_basecode FROM i2b2 WHERE c_fullname = New_paths(i));
    step := step + 1;
    cz_write_audit(l_job_id, current_schema_name, procedure_name, 'Deleted any concepts for path from I2B2DEMODATA observation_fact', SQL%ROWCOUNT, step, 'Done');
    COMMIT;

    FORALL i IN New_paths.FIRST .. New_paths.LAST
        DELETE FROM concept_dimension WHERE concept_path = New_paths(i);
    step := step + 1;
    cz_write_audit(l_job_id, current_schema_name, procedure_name, 'Deleted any concepts for path from I2B2DEMODATA concept_dimension', SQL%ROWCOUNT, step, 'Done');
    COMMIT;
    
    FORALL i IN New_paths.FIRST .. New_paths.LAST
        DELETE FROM i2b2 WHERE c_fullname = New_paths(i);
    step := step + 1;
    cz_write_audit(l_job_id, current_schema_name, procedure_name, 'Deleted path from I2B2METADATA i2b2', SQL%ROWCOUNT, step, 'Done');
    COMMIT;
    
    -- Populate node names table
    new_nodes := string_table_t();
    new_nodes.EXTEND(New_paths.LAST);
    FOR i IN New_paths.FIRST .. New_paths.LAST LOOP
        new_nodes(i) := trim('\' FROM SUBSTR(New_paths(i), INSTR(New_paths(i), '\', -2)));
        IF new_nodes(i) IS NULL THEN
            new_nodes.DELETE(i);
        END IF;
    END LOOP;
  
    -- Insert data
    FORALL i IN INDICES OF new_nodes
        INSERT /*+APPEND*/ INTO concept_dimension (concept_cd, concept_path, name_char,  update_date,  download_date, import_date, sourcesystem_cd, table_name)
            VALUES (concept_id.nextval, New_paths(i), new_nodes(i), sysdate, sysdate, sysdate, Trial_id, 'CONCEPT_DIMENSION');
    step := step + 1;
    cz_write_audit(l_job_id, current_schema_name, procedure_name, 'Inserted concept for paths into I2B2DEMODATA concept_dimension', SQL%ROWCOUNT, step, 'Done');
    COMMIT;
    
    FORALL i IN INDICES OF new_nodes
        INSERT /*+ APPEND */ INTO i2b2 (c_hlevel, c_fullname, c_name, c_visualattributes, c_synonym_cd, c_facttablecolumn, c_tablename, c_columnname, c_dimcode,
            c_tooltip, update_date, download_date, import_date, sourcesystem_cd, c_basecode, c_operator, c_columndatatype, c_comment, i2b2_id, m_applied_path)
        SELECT (LENGTH(concept_path) - NVL(LENGTH(REPLACE(concept_path, '\')), 0)) / LENGTH('\') - 2 + root_level,
               concept_path,
               name_char,
               'FA',
               'N',
               'CONCEPT_CD',
               'CONCEPT_DIMENSION',
               'CONCEPT_PATH',
               concept_path,
               concept_path,
               sysdate,
               sysdate,
               sysdate,
               sourcesystem_cd,
               concept_cd,
               'LIKE',
               'T',
               DECODE(Trial_id, NULL, NULL, 'trial:' || Trial_id),
               i2b2_id_seq.nextval,
               '@'
          FROM concept_dimension WHERE concept_path = New_paths(i);
    step := step + 1;
    cz_write_audit(l_job_id, current_schema_name, procedure_name, 'Inserted paths into I2B2METADATA i2b2', SQL%ROWCOUNT, step, 'Done');
    COMMIT;

    FORALL i IN INDICES OF new_nodes
        INSERT INTO i2b2_secure (c_hlevel, c_fullname, c_name, c_visualattributes, c_synonym_cd, c_facttablecolumn, c_tablename, c_columnname, c_dimcode,
            c_tooltip, update_date, download_date, import_date, sourcesystem_cd, c_basecode, c_operator, c_columndatatype, c_comment, m_applied_path, secure_obj_token)
        SELECT (LENGTH(concept_path) - NVL(LENGTH(REPLACE(concept_path, '\')), 0)) / LENGTH('\') - 2 + root_level,
               concept_path,
               name_char,
               'FA',
               'N',
               'CONCEPT_CD',
               'CONCEPT_DIMENSION',
               'CONCEPT_PATH',
               concept_path,
               concept_path,
               sysdate,
               sysdate,
               sysdate,
               sourcesystem_cd,
               concept_cd,
               'LIKE',
               'T',
               DECODE(Trial_id, NULL, NULL, 'trial:' || Trial_id),
               '@',
               'EXP:PUBLIC'
          FROM concept_dimension WHERE concept_path = New_paths(i);
    step := step + 1;
    cz_write_audit(l_job_id, current_schema_name, procedure_name, 'Inserted paths into I2B2METADATA i2b2_secure', SQL%ROWCOUNT, step, 'Done');
    COMMIT;

    ---Cleanup OVERALL JOB if this proc is being run standalone
    <<cleanup>>
    IF job_was_created THEN
        cz_end_audit(l_job_id, 'SUCCESS');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
      cz_error_handler(l_job_id, procedure_name);
      cz_end_audit(l_job_id, 'FAIL');
END;
/ 
