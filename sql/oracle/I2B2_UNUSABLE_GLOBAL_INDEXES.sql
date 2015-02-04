CREATE OR REPLACE PROCEDURE "I2B2_UNUSABLE_GLOBAL_INDEXES"(
	tbl_owner       VARCHAR2,
	tbl_name        VARCHAR2,
	job_id          NUMBER := -1)
AS
-- Rebuild all global indexes in UNUSABLE state
-- Params:
--   tbl_owner 	- owner of table, schema
--   tbl_name 	- table name
--	 job_id			- current job id (by default starts new job)

	newJobFlag      INTEGER(1);

	databaseName    VARCHAR(100);
	procedureName   VARCHAR(100);
	jobID           NUMBER(18, 0);
	stepCt          NUMBER(18, 0);

	qualifiedName   VARCHAR2(1024);
	sqlText         VARCHAR2(1000);
	n               NUMBER(18, 0);
	cnt             NUMBER(18, 0);

	BEGIN
		stepCt := 0;

--Set Audit Parameters
		newJobFlag := 0;
-- False (Default)
		jobID := job_id;
		SELECT sys_context('USERENV', 'CURRENT_SCHEMA')
		INTO databaseName
		FROM dual;
		procedureName := $$PLSQL_UNIT;

		stepCt := 0;

		IF (jobID IS NULL OR jobID < 1)
		THEN
			newJobFlag := 1;
-- True
			cz_start_audit(procedureName, databaseName, jobID);

			stepCt := stepCt + 1;
			cz_write_audit(jobId, databaseName, procedureName, 'Starting ' || procedureName, 0, stepCt, 'Done');
			COMMIT;
		END IF;

		qualifiedName := '"' || tbl_owner || '"."' || tbl_name || '"';

		n := 0;
		SELECT count(*)
		INTO cnt
		FROM all_indexes
		WHERE table_name = tbl_name AND table_owner = tbl_owner AND status = 'VALID';

		FOR idx IN ( SELECT
									 '"' || "OWNER" || '"."' || index_name || '"' AS index_name,
									 partitioned
								 FROM all_indexes
								 WHERE table_name = tbl_name AND table_owner = tbl_owner AND status = 'VALID')
		LOOP
			n := n + 1;

			sqlText := 'ALTER INDEX ' || idx.index_name || ' UNUSABLE';
			EXECUTE IMMEDIATE sqlText;

			stepCt := stepCt + 1;
			cz_write_audit(jobId, databaseName, procedureName,
										 'Unusable global index (' || n || ' of ' || cnt || ') - ' || idx.index_name || ' for ' || qualifiedName, 0,
										 stepCt, 'Done');
			COMMIT;
		END LOOP;

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

