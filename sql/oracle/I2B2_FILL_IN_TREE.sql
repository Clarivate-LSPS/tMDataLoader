--
-- Type: PROCEDURE; Owner: TM_CZ; Name: I2B2_FILL_IN_TREE
--
CREATE OR REPLACE PROCEDURE I2B2_FILL_IN_TREE (
    Trial_id VARCHAR2,
    Path VARCHAR2,
    CurrentJobID NUMBER := null
)
AS
/*****************************************************************************
* Copyright 2008-2012 Janssen Research and Development, LLC.
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
*****************************************************************************/

    ----------------------------------------------
    --Goal: To fill out an I2B2 Tree node
    --Steps. Walk backwards through an i2b2 tree and fill in all missing nodes.
    --\1\2\3\4\5\6\
    --Will check that \1\, \1\2\, etc..all exist.
    ----------------------------------------------
  
    TYPE paths_hash_t IS TABLE OF CHAR(1) INDEX BY VARCHAR2(4000);
    paths_hash paths_hash_t;
    new_paths string_table_t;
    escaped_path VARCHAR2(4000);
    node_path VARCHAR2(4000);
    existing_path VARCHAR2(4000);

    --Audit variables
    job_was_created boolean;
    current_schema_name VARCHAR(32);
    procedure_name VARCHAR(32);
    audit_text VARCHAR2(4000);
    job_id INTEGER;
    step INTEGER;
BEGIN
    -- Set Audit Parameters
    step := 0;
    job_was_created := false;
    job_id := CurrentJobID;
    current_schema_name := sys_context('USERENV', 'CURRENT_SCHEMA');
    procedure_name := $$PLSQL_UNIT;

    -- Audit JOB Initialization
    -- If Job ID does not exist, then this is a single procedure run and we need to create it
    IF job_id IS NULL OR job_id < 1 THEN
        job_was_created := true;
        cz_start_audit(procedure_name, current_schema_name, job_id);
    END IF;
  
    -- Get the nodes
    escaped_path := regexp_replace(Path, '([*%_])', '*\1', 1, 0);
    FOR i IN (select c_fullname from i2b2 where c_fullname like escaped_path || '%' escape '*') LOOP
        paths_hash(i.c_fullname) := null;
    END LOOP;
    audit_text := 'Got all study nodes';
    step := step + 1;
    cz_write_audit(job_id, current_schema_name, procedure_name, audit_text, paths_hash.COUNT, step, 'Done');

    -- Iterate through each node
    new_paths := string_table_t();
    existing_path := paths_hash.FIRST;
    LOOP
        node_path := existing_path;
        LOOP
            node_path := SUBSTR(node_path, 1, INSTR(node_path, '\', -2));
            EXIT WHEN paths_hash.EXISTS(node_path) OR INSTR(node_path, '\', 1, 3) < 1 OR node_path IS NULL;
            new_paths.EXTEND;
            new_paths(new_paths.LAST) := node_path;
            paths_hash(node_path) := null;
        END LOOP;
        existing_path := paths_hash.NEXT(existing_path);
        EXIT WHEN existing_path IS NULL;
    END LOOP;
    audit_text := 'Found missing nodes';
    step := step + 1;
    cz_write_audit(job_id, current_schema_name, procedure_name, audit_text, new_paths.COUNT, step, 'Done');

    -- Add missing nodes
    i2b2_add_nodes(Trial_id, new_paths, job_id);
  
    --- Cleanup OVERALL JOB if this proc is being run standalone
    IF job_was_created THEN
        cz_end_audit(job_id, 'SUCCESS');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        cz_error_handler(job_id, procedure_name);
        cz_end_audit(job_id, 'FAIL');
END;
/
 
