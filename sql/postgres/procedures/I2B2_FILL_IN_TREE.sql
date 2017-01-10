--
-- Name: i2b2_fill_in_tree(character varying, character varying, numeric); Type: FUNCTION; Schema: tm_dataloader; Owner: -
-- Purpose: add missing intermediate tree nodes into table i2b2metadata.i2b2.
--
CREATE OR REPLACE FUNCTION i2b2_fill_in_tree(trial_id varchar, path varchar, current_job_id numeric DEFAULT (-1)) RETURNS numeric
    LANGUAGE plpgsql SECURITY DEFINER SET search_path FROM CURRENT
    AS $$
/*************************************************************************
* Copyright 2008-2012 Janssen Research & Development, LLC.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
******************************************************************/
DECLARE
    user_name        CONSTANT varchar := current_user;
    function_name    CONSTANT varchar := 'I2B2_FILL_IN_TREE';
    new_job_flag     boolean;
    escaped_path     varchar;
    max_path_len     numeric;
    dir_path         varchar;
    node_name        varchar;
    full_path        varchar;
    v_count          numeric;
    job_id           numeric;
    step             numeric;
    error_number     varchar;
    error_message    varchar;
    return_code      numeric;
    new_paths        varchar[];
    -- audit_text       varchar;
BEGIN
    step := 0;
    --Set Audit Parameters
    new_job_flag := FALSE;
    job_id := current_job_id;
    -- Audit JOB Initialization
    -- If Job ID does not exist, then this is a single procedure run and we need to create it
    IF job_id IS NULL OR job_id < 1 THEN
        new_job_flag := TRUE;
        select cz_start_audit(function_name, user_name) into job_id;
    END IF;

    escaped_path := regexp_replace(path, '([*%_])', '*\1', 'g');

    drop table if exists tmp_i2b2_paths;
    -- Removing last node using regexp_replace because that node is already in table i2b2.
    create temporary table tmp_i2b2_paths (node_path) without oids on commit drop
        as select string_to_array(txt_path, '\') from (
            select regexp_replace(c_fullname, '\\[^\\]+\\$', '') as txt_path
              from i2b2metadata.i2b2
             where c_fullname like escaped_path || '%' escape '*'
            union
            select regexp_replace(path, '\\$', '') where not exists(select 1 from i2b2metadata.i2b2 where c_fullname = path)
        ) foo where length(txt_path) > 0;

    select max(array_length(node_path, 1)) into max_path_len from tmp_i2b2_paths;
    IF max_path_len IS NULL THEN
        step := step + 1;
        PERFORM cz_write_audit(job_id, current_schema()::varchar, function_name,
                               'No nodes found by path ''' || COALESCE(path, '<<null>>') || ''' found',1,step,'ERROR');
        PERFORM cz_error_handler(job_id, function_name, '-1', 'Application raised error');
        PERFORM cz_end_audit(job_id,'FAIL');
        return -16;
    END IF;
    FOR path_len IN 2 .. max_path_len LOOP
        FOR dir_path, node_name IN
            select distinct
                   array_to_string(node_path[1:path_len-1], '\'),
                   node_path[path_len]
              from tmp_i2b2_paths
             where array_length(node_path, 1) >= path_len
             order by 1, 2
        LOOP
            full_path := dir_path || '\' || node_name || '\';
            --Check if node exists.
            select count(*) into v_count
              from i2b2metadata.i2b2
             where c_fullname = full_path;

            --If it doesn't exist, add it
            IF v_count = 0 THEN
                -- audit_text := 'Inserting ' || full_path;
                -- step := step + 1;
                -- select cz_write_audit(job_id, user_name, function_name, audit_text, 0, step, 'Done') into return_code;
                new_paths := array_append(new_paths, full_path);
            END IF;
        END LOOP;
    END LOOP;

    SELECT i2b2_add_nodes(trial_id, new_paths::text[], job_id) INTO return_code;
    IF return_code <> 1 THEN
        RETURN return_code;
    END IF;

    ---Cleanup OVERALL JOB if this proc is being run standalone
    IF new_job_flag THEN
        select cz_end_audit (job_id, 'SUCCESS') into return_code;
    END IF;

    return 1;

EXCEPTION
    WHEN OTHERS THEN
        error_number := SQLSTATE;
        error_message := SQLERRM;
        --Handle errors.
        select cz_error_handler (job_id, function_name, error_number, error_message) into return_code;
        --End Proc
        select cz_end_audit (job_id, 'FAIL') into return_code;
        RETURN -16;
END;
$$;

