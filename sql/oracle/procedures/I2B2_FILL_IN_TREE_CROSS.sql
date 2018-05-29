--
-- Type: PROCEDURE; Owner: TM_DATALOADER; Name: I2B2_FILL_IN_TREE
--
CREATE OR REPLACE PROCEDURE I2B2_FILL_IN_TREE_CROSS(
  Path         VARCHAR2,
  CurrentJobID NUMBER := NULL
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

  TYPE PATHS_HASH_T IS TABLE OF CHAR(1) INDEX BY VARCHAR2 (4000);
  paths_hash          PATHS_HASH_T;
  new_paths           STRING_TABLE_T;
  escaped_path        VARCHAR2(4000);
  node_path           VARCHAR2(4000);
  existing_path       VARCHAR2(4000);
  name_char           VARCHAR2(2000);
    n_nodes             NUMBER;

  --Audit variables
  job_was_created     BOOLEAN;
  current_schema_name VARCHAR(32);
  procedure_name      VARCHAR(32);
  audit_text VARCHAR2(4000);
  job_id     INTEGER;
  step       INTEGER;
  BEGIN
    -- Set Audit Parameters
    step := 0;
    job_was_created := FALSE;
    job_id := CurrentJobID;
    current_schema_name := sys_context('USERENV', 'CURRENT_SCHEMA');
    procedure_name := $$PLSQL_UNIT;

    -- Audit JOB Initialization
    -- If Job ID does not exist, then this is a single procedure run and we need to create it
    IF job_id IS NULL OR job_id < 1
    THEN
      job_was_created := TRUE;
      cz_start_audit(procedure_name, current_schema_name, job_id);
    END IF;

    -- Get the nodes
    escaped_path := regexp_replace(Path, '([*%_])', '*\1', 1, 0);
    FOR i IN (SELECT modifier_path
              FROM modifier_dimension
              WHERE modifier_path LIKE escaped_path || '%' ESCAPE '*') LOOP
      paths_hash(i.modifier_path) := NULL;
    END LOOP;
    audit_text := 'Got all study nodes';
    step := step + 1;
    cz_write_audit(job_id, current_schema_name, procedure_name, audit_text, paths_hash.COUNT, step, 'Done');

    -- Iterate through each node
    new_paths := string_table_t();
    existing_path := paths_hash.FIRST;

    WHILE existing_path IS NOT NULL LOOP
      node_path := existing_path;
      LOOP
        node_path := SUBSTR(node_path, 1, INSTR(node_path, '\', -2));
        step := step + 1;
        cz_write_audit(job_id, current_schema_name, procedure_name, 'node_path ' || node_path, 0, step, 'Done');

        EXIT WHEN paths_hash.EXISTS(node_path) OR node_path IS NULL OR node_path = '\';
        new_paths.EXTEND;
        new_paths(new_paths.LAST) := node_path;
        paths_hash(node_path) := NULL;

        step := step + 1;
        cz_write_audit(job_id, current_schema_name, procedure_name, 'node_path ' || node_path, 0, step, 'Done');
        SELECT count(*)
        INTO n_nodes
        FROM modifier_dimension
        WHERE modifier_path = node_path;

        step := step + 1;
        cz_write_audit(job_id, current_schema_name, procedure_name, 'n_nodes ' || n_nodes, 0, step, 'Done');

        IF n_nodes = 0
        THEN
          new_paths.EXTEND;
          new_paths(new_paths.LAST) := node_path;
          name_char := SUBSTR(node_path, INSTR(node_path, '\', -2) + 1);
          name_char := SUBSTR(name_char, 1, LENGTH(name_char)-1);

          -- TODO: maybe here need to use MERGE
          INSERT INTO i2b2demodata.modifier_dimension (
            modifier_path,
            modifier_cd,
            name_char,
            modifier_level,
            modifier_node_type
          ) VALUES (
            node_path,
            tm_dataloader.modifier_dimension_seq.nextval,
            name_char,
            length(node_path)-length(replace(node_path, '\', '')) - 2,
            'F'
          );
          audit_text := 'Insert node ' || node_path;
          step := step + 1;
          cz_write_audit(job_id, current_schema_name, procedure_name, audit_text, 1, step, 'Done');

        END IF;

      END LOOP;
      existing_path := paths_hash.NEXT(existing_path);
    END LOOP;
    --- Cleanup OVERALL JOB if this proc is being run standalone
    IF job_was_created
    THEN
      cz_end_audit(job_id, 'SUCCESS');
    END IF;

    EXCEPTION
    WHEN OTHERS
    THEN
      cz_error_handler(job_id, procedure_name);
      cz_end_audit(job_id, 'FAIL');
  END;
/

