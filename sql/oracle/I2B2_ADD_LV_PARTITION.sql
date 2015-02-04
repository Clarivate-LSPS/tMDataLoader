CREATE OR REPLACE PROCEDURE "I2B2_ADD_LV_PARTITION"(
	tbl_owner       VARCHAR2,
	tbl_name        VARCHAR2,
	val             VARCHAR2,
	part_name       VARCHAR2 := NULL,
	rebuild_indexes INTEGER := 1,
	job_id          NUMBER := -1,
	ret_code OUT    NUMBER)
AS
-- Helper procedure for list value partition adding
-- It add partition only if partitioning enabled and partition not yet exists
-- Params:
--   tbl_owner 	- owner of table, schema
--   tbl_name 	- table name
--	 val				- value for partition
--	 part_name	- partition name (default: val)
--	 rebuild_indexes - 1: rebuild indexes for partition after adding, 0: do not rebuild (default: 1)
--	 job_id			- current job id (by default starts new job)
-- Return codes (ret_code):
--   -1	- table without partitioning (do nothing)
--   0	- partition for this value already exists (do nothing)
--	 1  - partition created

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
	useCompression  NUMBER;

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
			stepCt := stepCt + 1;
			cz_write_audit(jobId, databaseName, procedureName,
										 'Partitioning is not available for ' || qualifiedName || '. Skip.', 0,
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

		IF partitionExists <> 0
		THEN
			stepCt := stepCt + 1;
			cz_write_audit(jobId, databaseName, procedureName,
										 'Partition by ' || partitionName || ' already exists for ' || qualifiedName || '. Skip.', 0,
										 stepCt, 'Done');
			COMMIT;

			ret_code := 0;
		END IF;

		IF dataPartitioned <> 0 AND partitionExists = 0
		THEN
			useCompression := 0;

			SELECT 0
			INTO useCompression
			FROM all_tab_partitions
			WHERE table_name = 'DE_SNP_CALLS_BY_GSM'
						AND table_OWNER = 'DEAPP'
						AND COMPRESSION = 'DISABLED'
						AND ROWNUM = 1;

			sqlText := 'ALTER TABLE ' || qualifiedName || ' ADD PARTITION "' || partitionName || '" ' ||
								 'VALUES (''' || REPLACE(val, '''', '''''') || ''') ' ||
								 'NOLOGGING' || (CASE WHEN useCompression = 1 THEN ' COMPRESS'
																 ELSE '' END);
			EXECUTE IMMEDIATE (sqlText);

			stepCt := stepCt + 1;
			cz_write_audit(jobId, databaseName, procedureName, 'Add partition "' || partitionName || '" to ' || qualifiedName, 0, stepCt, 'Done');

			IF rebuild_indexes <> 0
			THEN
				n := 0;
				SELECT count(*)
				INTO cnt
				FROM all_indexes
				WHERE table_name = tbl_name AND table_owner = tbl_owner AND status <> 'VALID';

				FOR idx IN ( SELECT
											 '"' || "OWNER" || '"."' || index_name || '"' AS index_name,
											 partitioned
										 FROM all_indexes
										 WHERE table_name = tbl_name AND table_owner = tbl_owner AND status <> 'VALID')
				LOOP
					n := n + 1;

					IF idx.partitioned = 'YES'
					THEN
						sqlText := 'ALTER INDEX ' || idx.index_name || ' REBUILD PARTITION "' || partitionName || '"';
					ELSE
						sqlText := 'ALTER INDEX ' || idx.index_name || ' REBUILD';
					END IF;
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

