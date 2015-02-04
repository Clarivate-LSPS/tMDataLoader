CREATE OR REPLACE PROCEDURE "I2B2_DELETE_LV_PARTITION"(
	tbl_owner       VARCHAR2,
	tbl_name        VARCHAR2,
  part_col_name   VARCHAR2,
	val             VARCHAR2,
	part_name       VARCHAR2 := NULL,
  drop_partition  INTEGER := 0,
	rebuild_indexes INTEGER := 1,
	job_id          NUMBER := -1,
	ret_code OUT    NUMBER)
AS
-- Helper procedure for list value partition adding
-- It drop/truncate partition if partitioning enabled and partition exists
-- If partitioning is not enabled then it used DELETE operator and part_col_name for filtering
-- Params:
--   tbl_owner 	- owner of table, schema
--   tbl_name 	- table name
--   part_col_name - partitioned column name (used in case if partitioning is not enabled as filter column for delete operation)
--	 val				- value for partition
--	 part_name	- partition name (default: val)
--   drop_partition  - 1: drop partition, 0: truncate only (default: 0)
--	 rebuild_indexes - 1: rebuild indexes for partition after adding, 0: do not rebuild (default: 1)
--	 job_id			- current job id (by default starts new job)
-- Return codes (ret_code):
--  -1	- table without partitioning (use DELETE operator)
--   0	- partition for this value doesn't exists (do nothing)
--	 1  - partition droped/truncated

	newJobFlag      INTEGER(1);

	databaseName    VARCHAR(100);
	procedureName   VARCHAR(100);
	jobID           NUMBER(18, 0);
	stepCt          NUMBER(18, 0);

	qualifiedName   VARCHAR2(1024);
	sqlText         VARCHAR2(1000);
	partitionName   VARCHAR(512);
	n               NUMBER(18, 0);
	cnt             NUMBER(18, 0);

	dataPartitioned NUMBER;
	partitionExists NUMBER;

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
		partitionName := nvl(part_name, val);

		SELECT count(*)
		INTO dataPartitioned
		FROM all_tables
		WHERE table_name = tbl_name
					AND owner = tbl_owner
					AND partitioned = 'YES';
		IF dataPartitioned = 0
		THEN
      sqlText := 'DELETE FROM ' || qualifiedName || ' WHERE "' || part_col_name || '" = ''' || REPLACE(val, '''', '''''') || '''';
      EXECUTE IMMEDIATE 'BEGIN ' || sqlText || '; :x := SQL%ROWCOUNT; END;' USING OUT cnt;
    
			stepCt := stepCt + 1;
			cz_write_audit(jobId, databaseName, procedureName,
										 'Partitioning is not available for ' || qualifiedName || '. Use DELETE operator.', cnt,
										 stepCt, 'Done');
			COMMIT;

			ret_code := -1;
		END IF;

		SELECT count(*)
		INTO partitionExists
		FROM all_tab_partitions
		WHERE table_name = tbl_name
					AND table_owner = tbl_owner
					AND partition_name = partitionName;            

		IF partitionExists = 0
		THEN
			stepCt := stepCt + 1;
			cz_write_audit(jobId, databaseName, procedureName,
										 'Partition by ' || partitionName || ' doesn''t exists for ' || qualifiedName || '. Skip.', 0,
										 stepCt, 'Done');
			COMMIT;

			ret_code := 0;
		END IF;

		IF dataPartitioned <> 0 AND partitionExists <> 0
		THEN
			sqlText := 'ALTER TABLE ' || qualifiedName || ' ' || CASE WHEN drop_partition <> 0 THEN 'DROP' ELSE 'TRUNCATE' END || ' PARTITION "' || partitionName || '"';
			EXECUTE IMMEDIATE (sqlText);

			stepCt := stepCt + 1;
			cz_write_audit(jobId, databaseName, procedureName, CASE WHEN drop_partition <> 0 THEN 'Drop' ELSE 'Truncate' END || ' partition for ' || qualifiedName, 0, stepCt, 'Done');
			
			IF rebuild_indexes <> 0
			THEN
        n := 0;
        SELECT count(*)
        INTO cnt
        FROM all_indexes
        WHERE table_name = tbl_name AND table_owner = tbl_owner AND status = 'UNUSABLE';

      	-- Update only global indexes, local will be updated automatically
				FOR idx IN ( SELECT '"' || "OWNER" || '"."' || index_name || '"' AS index_name
										 FROM all_indexes
										 WHERE table_name = tbl_name AND table_owner = tbl_owner AND status = 'UNUSABLE')
				LOOP
					n := n + 1;

					sqlText := 'ALTER INDEX ' || idx.index_name || ' REBUILD';
					EXECUTE IMMEDIATE sqlText;

					stepCt := stepCt + 1;
					cz_write_audit(jobId, databaseName, procedureName,
												 'Rebuild index ' || n || ' of ' || cnt || ' ' || qualifiedName || ' partition indexes', 0,
												 stepCt, 'Done');
					COMMIT;
				END LOOP;
			END IF;

			ret_code := 1;
		END IF;

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

