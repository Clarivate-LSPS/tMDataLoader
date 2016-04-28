CREATE OR REPLACE PROCEDURE analyze_table (p_owner VARCHAR2, p_table VARCHAR2, p_job_id NUMBER := NULL)
AS
    stats_locked exception;
    pragma exception_init(stats_locked, -20005);
    stats_locked2 exception;
    pragma exception_init(stats_locked2, -38029);

    --Audit variables
    job_was_created boolean;
    current_schema_name VARCHAR2(32);
    procedure_name VARCHAR2(32);
    l_job_id INTEGER;
    step INTEGER;
BEGIN
    --Set Audit Parameters
    step := 0;
    l_job_id := p_job_id;
    job_was_created := false;
    SELECT sys_context('USERENV', 'CURRENT_SCHEMA') INTO current_schema_name FROM dual;
    procedure_name := $$PLSQL_UNIT;

    -- Audit JOB Initialization
    -- If Job ID does not exist, then this is a single procedure run and we need to create it
    IF l_job_id IS NULL OR l_job_id < 1 THEN
        job_was_created := true;
        CZ_START_AUDIT(procedure_name, current_schema_name, l_job_id);
    END IF;

    BEGIN
        dbms_stats.gather_table_stats(p_owner, p_table, cascade => true);
        step := step + 1;
        cz_write_audit(l_job_id, current_schema_name, procedure_name, 'Analyzed table '||p_owner||'.'||p_table, 0, step, 'Done');
    EXCEPTION
        WHEN stats_locked OR stats_locked2 THEN
            step := step + 1;
            cz_write_audit(l_job_id, current_schema_name, procedure_name, 'Statistics is locked for table '||p_owner||'.'||p_table, 0, step, 'Done');
    END;

    IF job_was_created THEN
        cz_end_audit(l_job_id, 'SUCCESS');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        IF job_was_created THEN
            cz_error_handler(l_job_id, procedure_name);
            cz_end_audit(l_job_id, 'FAIL');
        END IF;
        RAISE;
END;
/
