CREATE OR REPLACE PROCEDURE I2B2_MRNA_INDEX_MAINT(
    run_type        VARCHAR2 := 'DROP',
    tablespace_name VARCHAR2 := 'INDX',
    currentJobID    NUMBER := NULL,
    partitionName   VARCHAR2 := NULL
) AS
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

    tableName VARCHAR2(32) := 'DE_SUBJECT_MICROARRAY_DATA';
    deappSchema VARCHAR2(32) := 'DEAPP';
    runType VARCHAR2(100);
    idxExists NUMBER;
    pExists NUMBER;
    localVar VARCHAR2(20);
    bitmapVar VARCHAR2(20);
    bitmapCompress VARCHAR2(20);
    tableSpace VARCHAR2(50);

    --Audit variables
    newJobFlag BOOLEAN := FALSE;
    databaseName VARCHAR2(100);
    procedureName VARCHAR2(100);
    jobID NUMBER(18, 0);
    stepCt NUMBER(18, 0);
    sqltext VARCHAR2(200);

    FUNCTION index_exists(iName VARCHAR2)
    RETURN BOOLEAN AS
        nIndexes NUMBER;
    BEGIN
        select count(*) into nIndexes
          from all_indexes
         where     index_name = iName
               and owner = deappSchema;
        return nIndexes > 0;
    END;
    PROCEDURE drop_index(iName VARCHAR2) IS
    BEGIN
        IF index_exists(iName) THEN
            execute immediate('drop index '||deappSchema||'.'||iName);
            stepCt := stepCt + 1;
            cz_write_audit(jobId, databaseName, procedureName, 'Droped index '||deappSchema||'.'||iName, 0, stepCt, 'Done');
        END IF;
    END;
    PROCEDURE create_index(iName VARCHAR2, colList VARCHAR2, isBitmap BOOLEAN := FALSE) IS
    BEGIN
        IF index_exists(iName) THEN
            RETURN;
        END IF;
        IF isBitmap THEN
            execute immediate('create '||bitmapVar||' index '||deappSchema||'.'||iName||' on '||deappSchema||'.'||tableName||'('||colList||') '||localVar||' nologging '||bitmapCompress||' tablespace "'||tableSpace||'"');
        ELSE
            execute immediate('create index '||deappSchema||'.'||iName||' on '||deappSchema||'.'||tableName||'('||colList||') '||localVar||' nologging compress tablespace "'||tableSpace||'"');
        END IF;
        stepCt := stepCt + 1;
        cz_write_audit(jobId, databaseName, procedureName, 'Created index '||deappSchema||'.'||iName, 0, stepCt, 'Done');
    END;
BEGIN

    runType := upper(run_type);
    tableSpace := upper(nvl(tablespace_name, 'INDX'));
    -- Set Audit Parameters
    newJobFlag := FALSE;
    jobID := currentJobID;

    databaseName := sys_context('USERENV', 'CURRENT_SCHEMA');
    procedureName := $$PLSQL_UNIT;

    -- Audit JOB Initialization
    -- If Job ID does not exist, then this is a single procedure run and we need to create it
    IF jobID IS NULL or jobID < 1 THEN
        newJobFlag := TRUE;
        cz_start_audit(procedureName, databaseName, jobID);
    END IF;

    stepCt := 0;

    -- Determine if table is partitioned
    select count(*) into pExists
      from all_tables
     where     table_name = tableName
           and owner = deappSchema
           and partitioned = 'YES';

    IF runType = 'DROP' THEN
        stepCt := stepCt + 1;
        cz_write_audit(jobId, databaseName, procedureName, 'Start '||tableName||' index drop', 0, stepCt, 'Done');

        IF pExists = 0 or partitionName is NULL THEN
            drop_index('DE_MICROARRAY_DATA_IDX1');
            drop_index('DE_MICROARRAY_DATA_IDX2');
            drop_index('DE_MICROARRAY_DATA_IDX3');
            drop_index('DE_MICROARRAY_DATA_IDX4');
            drop_index('DE_MICROARRAY_DATA_IDX5');
            drop_index('DE_MICROARRAY_DATA_IDX10');
        ELSE
            -- All indexes MUST be locally partitioned.
            FOR i IN (
                select index_owner, index_name, partition_name
                  from all_ind_partitions
                 where     status= 'USABLE'
                       and partition_name= partitionName
                       and (index_owner, index_name) in (select owner, index_name from all_indexes
                           where table_owner= deappSchema and table_name= tableName)
            ) LOOP
                execute immediate 'alter index '||i.index_owner||'.'||i.index_name||' modify partition "'||i.partition_name||'" unusable';
                stepCt := stepCt + 1;
                cz_write_audit(jobId, databaseName, procedureName, 'Made partition '||i.index_owner||'.'||i.index_name||':"'||i.partition_name||'" unusable', 0, stepCt, 'Done');
            END LOOP;
        END IF;
    ELSIF runType = 'ADD' THEN
        stepCt := stepCt + 1;
        cz_write_audit(jobId, databaseName, procedureName, 'Start '||tableName||' index create', 0, stepCt, 'Done');

        IF pExists = 0 THEN
            localVar := null;
            bitmapVar := null;
            bitmapCompress := 'compress';
        ELSE
            localVar := 'local';
            bitmapVar := 'bitmap';
            bitmapCompress := null;
        END IF;

        IF pExists = 0 or partitionName is NULL THEN
            create_index('DE_MICROARRAY_DATA_IDX1', 'trial_name, assay_id, probeset_id');
            create_index('DE_MICROARRAY_DATA_IDX2', 'assay_id, probeset_id');
            create_index('DE_MICROARRAY_DATA_IDX3', 'assay_id', TRUE);
            create_index('DE_MICROARRAY_DATA_IDX4', 'probeset_id', TRUE);
            -- Only create this index if the table is not partitioned.  This is the column that the table would be partitioned on.
            IF pExists = 0 THEN
                create_index('DE_MICROARRAY_DATA_IDX5', 'trial_source');
            END IF;
        ELSE
            FOR i IN (
                select index_owner, index_name, partition_name
                  from all_ind_partitions
                 where     status= 'UNUSABLE'
                       and partition_name= partitionName
                       and (index_owner, index_name) in (select owner, index_name from all_indexes
                           where table_owner= 'DEAPP' and table_name= 'DE_SUBJECT_MICROARRAY_DATA')
            ) LOOP
                execute immediate 'alter index '||i.index_owner||'.'||i.index_name||' rebuild partition "'||i.partition_name||'"';
                stepCt := stepCt + 1;
                cz_write_audit(jobId, databaseName, procedureName, 'Rebuilt partition '||i.index_owner||'.'||i.index_name||':"'||i.partition_name||'"', 0, stepCt, 'Done');
            END LOOP;
        END IF;
    ELSE
        raise VALUE_ERROR;
    END IF;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'End procedure'||procedureName, 0, stepCt, 'Done');
    commit;

    ---Cleanup OVERALL JOB if this proc is being run standalone
    IF newJobFlag THEN
        cz_end_audit(jobID, 'SUCCESS');
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        IF newJobFlag THEN
            cz_error_handler(jobID, procedureName);
            cz_end_audit(jobID, 'FAIL');
        END IF;
        RAISE;
END;
/
