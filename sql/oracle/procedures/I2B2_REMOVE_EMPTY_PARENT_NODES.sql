CREATE OR REPLACE PROCEDURE "I2B2_REMOVE_EMPTY_PARENT_NODES"(
	node_path VARCHAR2,
	job_id    NUMBER := -1
)
AS
	-- Helper procedure for removing empty parent nodes for node_path
	-- Used in i2b2_move_study ad in i2b2_delete_all_data
	newJobFlag    INTEGER(1);

	databaseName  VARCHAR(100);
	procedureName VARCHAR(100);
	jobID         NUMBER(18, 0);
	stepCt        NUMBER(18, 0);

	-------------------------------
	-- Actual variables
	nodePath      VARCHAR2(2000);

	current_path  VARCHAR2(2000 BYTE);
	rowsExists    INT;

	TYPE row_type IS RECORD
	( path VARCHAR2(700 BYTE),
		lvl NUMBER,
		attr_name VARCHAR2(700 BYTE)
	);

	TYPE paths_tab_type IS TABLE OF row_type;
	paths_tab     paths_tab_type := paths_tab_type();

	BEGIN
		stepCt := 0;

		--Set Audit Parameters
		newJobFlag := 0; -- False (Default)
		jobID := job_id;
		SELECT sys_context('USERENV', 'CURRENT_SCHEMA')
		INTO databaseName
		FROM dual;
		procedureName := $$PLSQL_UNIT;

		stepCt := 0;

		IF (jobID IS NULL OR jobID < 1)
		THEN
			newJobFlag := 1; -- True
			cz_start_audit(procedureName, databaseName, jobID);

			stepCt := stepCt + 1;
			cz_write_audit(jobId, databaseName, procedureName, 'Starting ' || procedureName, 0, stepCt, 'Done');
			COMMIT;
		END IF;

		-- Start actual procedure body
		------------------------------

		nodePath := REGEXP_REPLACE('\' || node_path || '\', '(\\){2,}', '\');

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
				REGEXP_SUBSTR(ltrim(nodePath, '\'), '[^\]+', 1, LEVEL) STR
			FROM dual
			CONNECT BY LEVEL <= LENGTH(REGEXP_REPLACE(ltrim(nodePath, '\'), '[^\]+'))
		)
		START WITH PARENT_LEVEL = 0
		CONNECT BY PRIOR LVL = PARENT_LEVEL;

		-- add new level nodes to i2b2, i2b2_secure, concept_counts, concept_dimension
		FOR i IN REVERSE 1 .. paths_tab.COUNT
		LOOP
			current_path := paths_tab(i).path;
			SELECT count(*)
			INTO rowsExists
			FROM i2b2metadata.i2b2
			WHERE c_fullname LIKE current_path || '%';

			IF rowsExists = 1
			THEN
				I2B2_DELETE_1_NODE(current_path, jobID);
				cz_write_audit(jobId, databaseName, procedureName,
											 'Remove empty level: ' || current_path, 0, stepCt,
											 'Done');
				IF paths_tab(i).lvl = 1 THEN
					delete from table_access where c_fullname = current_path;
				END IF;
			END IF;
		END LOOP;

		------------------------------
		-- End actual procedure body


		IF newJobFlag = 1
		THEN
			stepCt := stepCt + 1;
			cz_write_audit(jobId, databaseName, procedureName, 'End ' || procedureName, 0, stepCt, 'Done');
			COMMIT;

			cz_end_audit(jobID, 'SUCCESS');
		END IF;

		EXCEPTION
		WHEN OTHERS THEN
		--Handle errors.
		cz_error_handler(jobID, procedureName);
		--End Proc
		cz_end_audit(jobID, 'FAIL');
	END;
/
