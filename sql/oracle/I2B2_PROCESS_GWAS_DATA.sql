CREATE OR REPLACE
PROCEDURE I2B2_PROCESS_GWAS_DATA
  (
    trial_id     IN VARCHAR2,
    top_node     IN VARCHAR2,
    secure_study IN VARCHAR2 := 'N',
    currentJobID IN NUMBER := NULL
  )
AS

  topNode       VARCHAR2(2000);
  topLevel      NUMBER(10, 0);
  root_node     VARCHAR2(2000);
  root_level    INT;
  study_name    VARCHAR2(2000);
  TrialID       VARCHAR2(100);
  secureStudy   VARCHAR2(200);
  etlDate       DATE;
  tPath         VARCHAR2(2000);
  pCount        INT;
  pExists       INT;
  rtnCode       INT;
  tText         VARCHAR2(2000);
  pathRegexp    VARCHAR2(2000);
  updatedPath   VARCHAR2(2000);

  --Audit variables
  newJobFlag    INTEGER(1);
  databaseName  VARCHAR(100);
  procedureName VARCHAR(100);
  jobID         NUMBER(18, 0);
  stepCt        NUMBER(18, 0);

    duplicate_values EXCEPTION;
    invalid_topNode EXCEPTION;
    MULTIPLE_VISIT_NAMES EXCEPTION;
    INDEX_NOT_EXISTS EXCEPTION;
PRAGMA EXCEPTION_INIT (index_not_exists, -1418);

  CURSOR delNodes IS
    SELECT DISTINCT c_fullname
    FROM i2b2
    WHERE c_fullname LIKE topNode || '%'
          AND substr(c_visualattributes, 2, 1) = 'H';

  --	cursor to determine if any leaf nodes exist in i2b2 that are not used in this reload (node changes from text to numeric or numeric to text)

  CURSOR delUnusedLeaf IS
    SELECT l.c_fullname
    FROM i2b2 l
    WHERE l.c_visualattributes LIKE 'L%'
          AND l.c_fullname LIKE topNode || '%'
          --and l.c_fullname not in
          AND NOT exists
    (SELECT t.leaf_node
     FROM wt_trial_nodes t
     WHERE t.leaf_node = l.c_fullname
     UNION ALL
     SELECT m.c_fullname
     FROM de_subject_sample_mapping sm
       , i2b2 m
     WHERE sm.trial_name = TrialId
           AND sm.concept_code = m.c_basecode
           AND m.c_visualattributes LIKE 'L%' AND m.c_fullname = l.c_fullname);
  BEGIN

    TrialID := upper(trial_id);
    secureStudy := upper(secure_study);

    --Set Audit Parameters
    newJobFlag := 0; -- False (Default)
    jobID := currentJobID;

    SELECT sys_context('USERENV', 'CURRENT_SCHEMA')
    INTO databaseName
    FROM dual;
    procedureName := $$PLSQL_UNIT;

    SELECT sysdate
    INTO etlDate
    FROM dual;

    --Audit JOB Initialization
    --If Job ID does not exist, then this is a single procedure run and we need to create it
    IF (jobID IS NULL OR jobID < 1)
    THEN
      newJobFlag := 1; -- True
      cz_start_audit(procedureName, databaseName, jobID);
    END IF;

    stepCt := 0;

    stepCt := stepCt + 1;
    tText := 'Start I2B2_PROCESS_GWAS_DATA for ' || TrialId;
    cz_write_audit(jobId, databaseName, procedureName, tText, 0, stepCt, 'Done');

    IF (secureStudy NOT IN ('Y', 'N'))
    THEN
      secureStudy := 'Y';
    END IF;

    -- added by Eugr: enable parallel queries
    EXECUTE IMMEDIATE 'alter session enable parallel dml';

    topNode := REGEXP_REPLACE('\' || top_node || '\', '(\\){2,}', '\');

    --	figure out how many nodes (folders) are at study name and above
    --	\Public Studies\Clinical Studies\Pancreatic_Cancer_Smith_GSE22780\: topLevel = 4, so there are 3 nodes
    --	\Public Studies\GSE12345\: topLevel = 3, so there are 2 nodes

    SELECT length(topNode) - length(replace(topNode, '\', ''))
    INTO topLevel
    FROM dual;

    IF topLevel < 3
    THEN
      RAISE invalid_topNode;
    END IF;


    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Validate secure params ', SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    EXECUTE IMMEDIATE ('truncate table TM_WZ.wrk_clinical_data');
    BEGIN
      EXECUTE IMMEDIATE ('drop index TM_WZ.IDX_WRK_CD');
      EXCEPTION
      WHEN index_not_exists THEN NULL;
    END;

    --	insert data from lt_src_clinical_data to wrk_clinical_data
    -- Optimization: do not insert null data_Value
    INSERT /*+ APPEND */ INTO wrk_clinical_data nologging
    (study_id
      , site_id
      , subject_id
      , visit_name
      , data_label
      , data_value
      , category_cd
      , ctrl_vocab_code
      , category_path
      , usubjid
      , data_type
      , valuetype_cd
    )
      SELECT
        study_id,
        NULL,
        subject_id,
        NULL,
        data_label,
        ' ',
        category_cd,
        NULL,
        regexp_replace(regexp_replace(replace(replace(category_cd,'_',' '),'+','\'),'\$\$\d*[A-Z]\{([^}]+)\}','\1'),'\$\$\d*[A-Z]','')
        ,TrialID || ':' || subject_id
        ,'T'
        ,NULL
      FROM tm_cz.lt_src_gwas_data;
    COMMIT;
    EXECUTE IMMEDIATE ('CREATE INDEX TM_WZ.IDX_WRK_CD ON TM_WZ.WRK_CLINICAL_DATA (DATA_TYPE ASC, DATA_VALUE ASC, VISIT_NAME ASC, DATA_LABEL ASC, CATEGORY_CD ASC, USUBJID ASC)');

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Load lt_src_gwas_data to work table', SQL%ROWCOUNT, stepCt, 'Done');

    -- Get root_node from topNode

    SELECT parse_nth_value(topNode, 2, '\')
    INTO root_node
    FROM dual;

    SELECT count(*)
    INTO pExists
    FROM table_access
    WHERE c_name = root_node;

    SELECT count(*)
    INTO pCount
    FROM i2b2
    WHERE c_name = root_node;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Load lt_src_gwas_data to work table', SQL%ROWCOUNT, stepCt, 'Done');

    IF pExists = 0 OR pCount = 0
    THEN
      i2b2_add_root_node(root_node, jobId);
    END IF;

    SELECT c_hlevel
    INTO root_level
    FROM table_access
    WHERE c_name = root_node;

    -- Get study name from topNode

    SELECT parse_nth_value(topNode, topLevel, '\')
    INTO study_name
    FROM dual;

    --	Add any upper level nodes as needed

    tPath := REGEXP_REPLACE(replace(topNode, study_name, NULL), '(\\){2,}', '\');
    SELECT length(tPath) - length(replace(tPath, '\', NULL))
    INTO pCount
    FROM dual;

    IF pCount > 2
    THEN
      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName, 'Adding upper-level nodes', 0, stepCt, 'Done');
      i2b2_fill_in_tree(NULL, tPath, jobId);
    END IF;

    SELECT count(*)
    INTO pExists
    FROM i2b2
    WHERE c_fullname = topNode;

    --	add top node for study

    IF pExists = 0
    THEN
      i2b2_add_node(TrialId, topNode, study_name, jobId);
    END IF;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'add top node for study', SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;
    --	Set data_type, category_path, and usubjid 

    --Remove Invalid pipes in the data values.
    --RULE: If Pipe is last or first, delete it
    --If it is in the middle replace with a dash

    UPDATE wrk_clinical_data
    SET data_value = replace(trim('|' FROM data_value), '|', '-')
    WHERE data_value LIKE '%|%';
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Remove pipes in data_value', SQL%ROWCOUNT, stepCt, 'Done');

    COMMIT;

    --Remove invalid Parens in the data
    --They have appeared as empty pairs or only single ones.

    UPDATE wrk_clinical_data
    SET data_value = replace(data_value, '(', '')
    WHERE data_value LIKE '%()%'
          OR data_value LIKE '%( )%'
          OR (data_value LIKE '%(%' AND data_value NOT LIKE '%)%');
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Remove empty parentheses 1', SQL%ROWCOUNT, stepCt, 'Done');

    UPDATE wrk_clinical_data
    SET data_value = replace(data_value, ')', '')
    WHERE data_value LIKE '%()%'
          OR data_value LIKE '%( )%'
          OR (data_value LIKE '%)%' AND data_value NOT LIKE '%(%');
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Remove empty parentheses 2', SQL%ROWCOUNT, stepCt, 'Done');

    COMMIT;

    --Replace the Pipes with Commas in the data_label column
    UPDATE wrk_clinical_data
    SET data_label = replace(data_label, '|', ',')
    WHERE data_label LIKE '%|%';
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Replace pipes with comma in data_label', SQL%ROWCOUNT, stepCt,
                   'Done');

    COMMIT;

    --	set visit_name to null when there's only a single visit_name for the category_cd
    UPDATE wrk_clinical_data tpm
    SET visit_name = NULL
    WHERE (regexp_replace(tpm.category_cd, '\$\$(\d*[A-Z])(\{[^}]+\}|[^+]+)', '\$\$\1')) IN
          (SELECT regexp_replace(x.category_cd, '\$\$(\d*[A-Z])(\{[^}]+\}|[^+]+)', '\$\$\1')
           FROM wrk_clinical_data x
           -- all tag values started with $$ ($$ will be removed from concept_path),
           -- concept_cd with different tags should be in same group, so we just replace tag with $$ for grouping
           GROUP BY regexp_replace(x.category_cd, '\$\$(\d*[A-Z])(\{[^}]+\}|[^+]+)', '\$\$\1')
           HAVING count(DISTINCT upper(x.visit_name)) = 1);

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Set single visit_name to null', SQL%ROWCOUNT, stepCt, 'Done');

    UPDATE wrk_clinical_data tmp
    SET
      category_cd = regexp_replace(regexp_replace(tmp.category_cd, '\$\$\d*[A-Z]\{([^}]+)\}', '\1'), '\$\$\d*[A-Z]', '')
    WHERE tmp.category_cd LIKE '%$$%';

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Remove tag markers', SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    --	set data_label to null when it duplicates the last part of the category_path
    --	Remove data_label from last part of category_path when they are the same
    UPDATE wrk_clinical_data tpm
    --set data_label = null
    SET category_path = substr(tpm.category_path, 1, instr(tpm.category_path, '\', -2) - 1)
      , category_cd   = substr(tpm.category_cd, 1, instr(tpm.category_cd, '+', -2) - 1)
    WHERE (tpm.category_cd, tpm.data_label) IN
          (SELECT DISTINCT
             t.category_cd,
             t.data_label
           FROM wrk_clinical_data t
           WHERE upper(substr(t.category_path, instr(t.category_path, '\', -1) + 1,
                              length(t.category_path) - instr(t.category_path, '\', -1)))
                 = upper(t.data_label)
                 AND t.data_label IS NOT NULL)
          AND tpm.data_label IS NOT NULL;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Set data_label to null when found in category_path',
                   SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    UPDATE /*+ parallel(4) */ wrk_clinical_data
    SET data_label    = trim(TRAILING ',' FROM trim(replace(replace(/**/
                                                                replace(replace(replace(replace(replace(data_label, '%',
                                                                                                        ' Pct'), '&',
                                                                                                ' and '), '+', ' and '),
                                                                                '_', ' '), '(plus)', '+')
                                                                /**/, '  ', ' '), ' ,', ',')))
      , data_value    = trim(TRAILING ',' FROM trim(replace(replace(replace(/**/replace(replace(replace(data_value, '%',
                                                                                                        ' Pct'), '&',
                                                                                                ' and '), '+', ' and ')
                                                                                /**/, '  ', ' '), ' ,', ','), '(plus)',
                                                            '+')))
      , visit_name    = trim(TRAILING ',' FROM trim(replace(replace(visit_name, '  ', ' '), ' ,', ',')))
      , category_cd   = replace(replace(category_cd, '%', ' Pct'), '&', ' and ')
      , category_path = replace(replace(replace(category_path, '%', ' Pct'), '&', ' and '), '(plus)', '+');

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Remove leading, trailing, double spaces', SQL%ROWCOUNT, stepCt,
                   'Done');

    COMMIT;

    -- set visit_name and data_label to null if it is not in path. Avoids duplicates for wt_trial_nodes
    -- we should clear these field before detecting data type
    UPDATE wrk_clinical_data t
    SET visit_name = NULL
    WHERE category_path LIKE '%\$' AND category_path NOT LIKE '%VISITNAME%';

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Set visit_name to null if VISITNAME not in category_path',
                   SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    UPDATE wrk_clinical_data t
    SET data_label = NULL
    WHERE category_path LIKE '%\$' AND category_path NOT LIKE '%DATALABEL%';

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Set data_label to null if DATALABEL not in category_path',
                   SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    --	set visit_name to null if same as data_label

    UPDATE wrk_clinical_data t
    SET visit_name = NULL
    WHERE (t.category_cd, t.visit_name, t.data_label) IN
          (SELECT DISTINCT
             tpm.category_cd,
             tpm.visit_name,
             tpm.data_label
           FROM wrk_clinical_data tpm
           WHERE tpm.visit_name = tpm.data_label);

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Set visit_name to null when found in data_label', SQL%ROWCOUNT,
                   stepCt, 'Done');

    COMMIT;

    UPDATE wrk_clinical_data t
    SET visit_name = NULL
    WHERE (t.category_cd, t.visit_name, t.data_value) IN
          (SELECT DISTINCT
             tpm.category_cd,
             tpm.visit_name,
             tpm.data_value
           FROM wrk_clinical_data tpm
           WHERE tpm.visit_name = tpm.data_value);

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Set visit_name to null when found in data_value', SQL%ROWCOUNT,
                   stepCt, 'Done');

    COMMIT;

    -- determine numeric data types

    EXECUTE IMMEDIATE ('truncate table TM_WZ.wt_num_data_types');

    COMMIT;

    UPDATE wrk_clinical_data t
    SET data_type = 'N'
    WHERE exists
    (SELECT 1
     FROM wt_num_data_types x
     WHERE nvl(t.category_cd, '@') = nvl(x.category_cd, '@')
           AND nvl(t.data_label, '**NULL**') = nvl(x.data_label, '**NULL**')
           AND nvl(t.visit_name, '**NULL**') = nvl(x.visit_name, '**NULL**')
    );
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Updated data_type flag for numeric data_types', SQL%ROWCOUNT,
                   stepCt, 'Done');

    COMMIT;


    UPDATE /*+ parallel(4) */ wrk_clinical_data
    SET category_path =
    CASE
    -- Path with terminator, don't change, just remove terminator
    WHEN category_path LIKE '%\$'
      THEN substr(category_path, 1, length(category_path) - 2)
    -- Add missing fields to concept_path
    ELSE
      CASE
      WHEN category_path LIKE '%\VISITNFST'
        THEN replace(category_path, '\VISITNFST', '')
      ELSE category_path
      END ||
      CASE
      WHEN category_path NOT LIKE '%DATALABEL%'
        THEN '\DATALABEL'
      ELSE ''
      END ||
      CASE
      WHEN category_path LIKE '%\VISITNFST'
        THEN '\VISITNAME'
      ELSE ''
      END ||
      CASE
      WHEN data_type = 'T' AND category_path NOT LIKE '%DATAVALUE%'
        THEN '\DATAVALUE'
      ELSE ''
      END ||
      CASE
      WHEN category_path NOT LIKE '%\VISITNFST' AND category_path NOT LIKE '%VISITNAME%'
        THEN '\VISITNAME'
      ELSE ''
      END
    END;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName,
                   'Add if missing DATALABEL, VISITNAME and DATAVALUE to category_path', SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    -- Remove duplicates
    DELETE FROM /*+ parallel(4) */ wrk_clinical_data
    WHERE rowid IN (
      SELECT rid
      FROM (
        SELECT
          rowid rid,
          row_number()
          OVER (
            PARTITION BY subject_id, visit_name, data_label, category_cd, data_value
            ORDER BY rowid
          )     rn
        FROM wrk_clinical_data
      )
      WHERE rn <> 1
    );

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Remove duplicates from wrk_clinical_data', SQL%ROWCOUNT, stepCt,
                   'Done');

    COMMIT;

    --	Check if any duplicate records of key columns (site_id, subject_id, visit_name, data_label, category_cd) for numeric data
    --	exist.  Raise error if yes

    EXECUTE IMMEDIATE ('truncate table TM_WZ.wt_clinical_data_dups');

    INSERT INTO wt_clinical_data_dups
    (site_id
      , subject_id
      , visit_name
      , data_label
      , category_cd)
      SELECT
        w.site_id,
        w.subject_id,
        w.visit_name,
        w.data_label,
        w.category_cd
      FROM wrk_clinical_data w
      WHERE exists
      (SELECT 1
       FROM wt_num_data_types t
       WHERE coalesce(w.category_cd, '@') = coalesce(t.category_cd, '@')
             AND coalesce(w.data_label, '@') = coalesce(t.data_label, '@')
             AND coalesce(w.visit_name, '@') = coalesce(t.visit_name, '@')
      )
      GROUP BY w.site_id, w.subject_id, w.visit_name, w.data_label, w.category_cd
      HAVING count(*) > 1;

    pCount := SQL%ROWCOUNT;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Check for duplicate key columns', pCount, stepCt, 'Done');

    IF pCount > 0
    THEN
      RAISE duplicate_values;
    END IF;

    -- Build all needed leaf nodes in one pass for both numeric and text nodes

    EXECUTE IMMEDIATE ('truncate table TM_WZ.wt_trial_nodes');

    INSERT /*+ APPEND parallel(wt_trial_nodes, 4) */ INTO wt_trial_nodes nologging
    (leaf_node
      , category_cd
      , visit_name
      , data_label
     --,node_name
      , data_value
      , data_type
      , valuetype_cd
    )
      SELECT
        /*+ parallel(a, 4) */  DISTINCT
        CASE
        --	Text data_type (default node)
        WHEN a.data_type = 'T'
          THEN regexp_replace(topNode || replace(
              replace(replace(a.category_path, 'DATALABEL', a.data_label), 'VISITNAME', a.visit_name), 'DATAVALUE',
              a.data_value) || '\', '(\\){2,}', '\')
        --	else is numeric data_type and default_node
        ELSE regexp_replace(
            topNode || replace(replace(a.category_path, 'DATALABEL', a.data_label), 'VISITNAME', a.visit_name) || '\',
            '(\\){2,}', '\')
        END                                          AS leaf_node,
        a.category_cd,
        a.visit_name,
        a.data_label,
        decode(a.data_type, 'T', a.data_value, NULL) AS data_value,
        a.data_type,
        a.valuetype_cd
      FROM wrk_clinical_data a;
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Create leaf nodes for trial', SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    --	set node_name

    UPDATE wt_trial_nodes
    SET node_name = parse_nth_value(leaf_node, length(leaf_node) - length(replace(leaf_node, '\', NULL)), '\');
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Updated node name for leaf nodes', SQL%ROWCOUNT, stepCt,
                   'Done');
    COMMIT;

    -- execute immediate('analyze table wt_trial_nodes compute statistics');

    --	insert subjects into patient_dimension if needed

    EXECUTE IMMEDIATE ('truncate table tmp_subject_info');

    INSERT INTO tmp_subject_info
    (usubjid,
     age_in_years_num,
     sex_cd,
     race_cd
    )
      SELECT
        a.usubjid,
        nvl(max(CASE WHEN upper(a.data_label) = 'AGE'
          THEN CASE WHEN is_number(a.data_value) = 1
            THEN 0
               ELSE floor(to_number(a.data_value)) END
                WHEN upper(a.data_label) LIKE '%(AGE)'
                  THEN CASE WHEN is_number(a.data_value) = 1
                    THEN 0
                       ELSE floor(to_number(a.data_value)) END
                ELSE NULL END), 0)         AS age,
        nvl(max(CASE WHEN upper(a.data_label) = 'SEX'
          THEN a.data_value
                WHEN upper(a.data_label) LIKE '%(SEX)'
                  THEN a.data_value
                WHEN upper(a.data_label) = 'GENDER'
                  THEN a.data_value
                ELSE NULL END), 'Unknown') AS sex,
        max(CASE WHEN upper(a.data_label) = 'RACE'
          THEN a.data_value
            WHEN upper(a.data_label) LIKE '%(RACE)'
              THEN a.data_value
            ELSE NULL END)                 AS race
      FROM wrk_clinical_data a
      GROUP BY a.usubjid;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Insert subject information into temp table', SQL%ROWCOUNT,
                   stepCt, 'Done');

    COMMIT;

    --	Delete dropped subjects from patient_dimension if they do not exist in de_subject_sample_mapping
    DELETE /*+ parallel(patient_dimension, 8) */ patient_dimension
    WHERE sourcesystem_cd IN
          (SELECT DISTINCT pd.sourcesystem_cd
           FROM patient_dimension pd
           WHERE pd.sourcesystem_cd LIKE TrialId || ':%'
           MINUS
           SELECT DISTINCT cd.usubjid
           FROM wrk_clinical_data cd)
          AND patient_num NOT IN
              (SELECT DISTINCT sm.patient_id
               FROM de_subject_sample_mapping sm);

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Delete dropped subjects from patient_dimension', SQL%ROWCOUNT,
                   stepCt, 'Done');

    COMMIT;

    --	update patients with changed information

    UPDATE /*+ parallel(patient_dimension, 8) */ patient_dimension pd
    SET (SEX_CD, AGE_IN_YEARS_NUM, RACE_CD, UPDATE_DATE) =
    (SELECT
       /*+ parallel(tmp_subject_info, 8) */
       nvl(t.sex_cd, pd.sex_cd),
       t.age_in_years_num,
       nvl(t.race_cd, pd.race_cd),
       sysdate
     FROM tmp_subject_info t
     WHERE t.usubjid = pd.sourcesystem_cd
           AND (coalesce(pd.sex_cd, '@') != t.sex_cd OR
                pd.age_in_years_num != t.age_in_years_num OR
                coalesce(pd.race_cd, '@') != t.race_cd)
    )
    WHERE exists
    (SELECT
       /*+ parallel(tmp_subject_info, 8) */ 1
     FROM tmp_subject_info x
     WHERE pd.sourcesystem_cd = x.usubjid
           AND (coalesce(pd.sex_cd, '@') != x.sex_cd OR
                pd.age_in_years_num != x.age_in_years_num OR
                coalesce(pd.race_cd, '@') != x.race_cd)
    );

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Update subjects with changed demographics in patient_dimension',
                   SQL%ROWCOUNT, stepCt, 'Done');

    COMMIT;

    --	insert new subjects into patient_dimension

    INSERT /*+ parallel(patient_dimension, 8) */ INTO patient_dimension
    (patient_num,
     sex_cd,
     age_in_years_num,
     race_cd,
     update_date,
     download_date,
     import_date,
     sourcesystem_cd
    )
      SELECT
        /*+ parallel(tmp_subject_info, 8) */
        seq_patient_num.nextval,
        t.sex_cd,
        t.age_in_years_num,
        t.race_cd,
        sysdate,
        sysdate,
        sysdate,
        t.usubjid
      FROM tmp_subject_info t
      WHERE t.usubjid IN
            (SELECT DISTINCT cd.usubjid
             FROM tmp_subject_info cd
             MINUS
             SELECT DISTINCT pd.sourcesystem_cd
             FROM patient_dimension pd
             WHERE pd.sourcesystem_cd LIKE TrialId || '%');

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Insert new subjects into patient_dimension', SQL%ROWCOUNT,
                   stepCt, 'Done');

    COMMIT;

    --	delete leaf nodes that will not be reused, if any

    FOR r_delUnusedLeaf IN delUnusedLeaf LOOP

      --	deletes unused leaf nodes for a trial one at a time

      i2b2_delete_1_node(r_delUnusedLeaf.c_fullname);
      stepCt := stepCt + 1;
      cz_write_audit(jobId, databaseName, procedureName, 'Deleted unused node: ' || r_delUnusedLeaf.c_fullname,
                     SQL%ROWCOUNT, stepCt, 'Done');

    END LOOP;

    --	bulk insert leaf nodes

    UPDATE /*+ parallel(cd, 4) */ concept_dimension cd
    SET name_char = (SELECT
                       /*+ parallel(t, 4) */ t.node_name
                     FROM wt_trial_nodes t
                     WHERE cd.concept_path = t.leaf_node
                           AND cd.name_char != t.node_name)
    WHERE exists(SELECT
                   /*+ parallel(x, 4) */ 1
                 FROM wt_trial_nodes x
                 WHERE cd.concept_path = x.leaf_node
                       AND cd.name_char != x.node_name);

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Update name_char in concept_dimension for changed names',
                   SQL%ROWCOUNT, stepCt, 'Done');

    COMMIT;


    INSERT /*+ parallel(concept_dimension, 8) */ INTO concept_dimension
    (concept_cd
      , concept_path
      , name_char
      , update_date
      , download_date
      , import_date
      , sourcesystem_cd
      , table_name
    )
      SELECT
        /*+ parallel(8) */
        concept_id.nextval,
        x.leaf_node,
        x.node_name,
        sysdate,
        sysdate,
        sysdate,
        TrialId,
        'CONCEPT_DIMENSION'
      FROM (SELECT DISTINCT
              c.leaf_node,
              to_char(c.node_name) AS node_name
            FROM wt_trial_nodes c
            WHERE NOT exists
            (SELECT 1
             FROM concept_dimension x
             WHERE c.leaf_node = x.concept_path)
           ) x;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Inserted new leaf nodes into I2B2DEMODATA concept_dimension',
                   SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    --	update i2b2 to pick up change in name, data_type for leaf nodes
    MERGE /*+ parallel(i2b2, 8) */ INTO i2b2 b
    USING (
            SELECT DISTINCT
              c.concept_path,
              c.name_char,
              c.sourcesystem_cd,
              c.concept_cd,
              t.data_type,
              t.valuetype_cd
            FROM concept_dimension c
              , wt_trial_nodes t
            WHERE c.concept_path = t.leaf_node
          ) c
    ON (b.c_fullname = c.concept_path)
    WHEN MATCHED THEN
    UPDATE SET
      c_name = c.name_char,
      c_columndatatype = 'T',
      c_metadataxml = I2B2_BUILD_METADATA_XML(c.name_char, c.data_type, c.valuetype_cd)
    WHEN NOT MATCHED THEN
    INSERT (
      c_hlevel
      , c_fullname
      , c_name
      , c_visualattributes
      , c_synonym_cd
      , c_facttablecolumn
      , c_tablename
      , c_columnname
      , c_dimcode
      , c_tooltip
      , update_date
      , download_date
      , import_date
      , sourcesystem_cd
      , c_basecode
      , c_operator
      , c_columndatatype
      , c_comment
      , i2b2_id
      , c_metadataxml
    )
    VALUES (
      (length(c.concept_path) - nvl(length(replace(c.concept_path, '\')), 0)) / length('\') - 2 + root_level
      , c.concept_path
      , c.name_char
      , 'LA'
      , 'N'
      , 'CONCEPT_CD'
      , 'CONCEPT_DIMENSION'
      , 'CONCEPT_PATH'
      , c.concept_path
      , c.concept_path
      , sysdate
      , sysdate
      , sysdate
      , c.sourcesystem_cd
      , c.concept_cd
      , 'LIKE'
      , 'T'    -- if i2b2 gets fixed to respect c_columndatatype then change to t.data_type
      , 'trial:' || TrialID
      , i2b2_id_seq.nextval
      , I2B2_BUILD_METADATA_XML(c.name_char, c.data_type, c.valuetype_cd)
    );

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Inserted leaf nodes into I2B2METADATA i2b2', SQL%ROWCOUNT,
                   stepCt, 'Done');
    COMMIT;

    i2b2_fill_in_tree(TrialId, topNode, jobID);

    COMMIT;

    --21 July 2013. Performace fix by TR. Drop complicated index before data manipulation
    BEGIN
      BEGIN
        EXECUTE IMMEDIATE ('DROP INDEX "I2B2DEMODATA"."OB_FACT_PK"');
        EXCEPTION
        WHEN index_not_exists THEN NULL;
      END;
      BEGIN
        EXECUTE IMMEDIATE ('DROP INDEX "I2B2DEMODATA"."IDX_OB_FACT_1"');
        EXCEPTION
        WHEN index_not_exists THEN NULL;
      END;
      BEGIN
        EXECUTE IMMEDIATE ('DROP INDEX "I2B2DEMODATA"."IDX_OB_FACT_2"');
        EXCEPTION
        WHEN index_not_exists THEN NULL;
      END;
      BEGIN
        EXECUTE IMMEDIATE ('DROP INDEX "I2B2DEMODATA"."IDX_OB_FACT_PATIENT_NUMBER"');
        EXCEPTION
        WHEN index_not_exists THEN NULL;
      END;
    END;

    --execute immediate('DROP INDEX "I2B2DEMODATA"."OF_CTX_BLOB"'); 

    --	delete from observation_fact all concept_cds for trial that are clinical data, exclude concept_cds from biomarker data
    DELETE /*+ parallel(observation_fact, 4) */ FROM OBSERVATION_FACT F
    WHERE f.modifier_cd = TrialId
          AND F.CONCEPT_CD NOT IN
              (SELECT
                 /*+ parallel(de_subject_sample_mapping, 4) */ DISTINCT concept_code AS concept_cd
               FROM de_subject_sample_mapping
               WHERE trial_name = TrialId
                     AND concept_code IS NOT NULL
               UNION
               SELECT
                 /*+ parallel(de_subject_sample_mapping, 4) */ DISTINCT platform_cd AS concept_cd
               FROM de_subject_sample_mapping
               WHERE trial_name = TrialId
                     AND platform_cd IS NOT NULL
               UNION
               SELECT
                 /*+ parallel(de_subject_sample_mapping, 4) */ DISTINCT sample_type_cd AS concept_cd
               FROM de_subject_sample_mapping
               WHERE trial_name = TrialId
                     AND sample_type_cd IS NOT NULL
               UNION
               SELECT
                 /*+ parallel(de_subject_sample_mapping, 4) */ DISTINCT tissue_type_cd AS concept_cd
               FROM de_subject_sample_mapping
               WHERE trial_name = TrialId
                     AND tissue_type_cd IS NOT NULL
               UNION
               SELECT
                 /*+ parallel(de_subject_sample_mapping, 4) */ DISTINCT timepoint_cd AS concept_cd
               FROM de_subject_sample_mapping
               WHERE trial_name = TrialId
                     AND timepoint_cd IS NOT NULL
               UNION
               SELECT
                 /*+ parallel(de_subject_sample_mapping, 4) */ DISTINCT concept_cd AS concept_cd
               FROM de_subject_snp_dataset
               WHERE trial_name = TrialId
                     AND concept_cd IS NOT NULL);

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Delete clinical data for study from observation_fact',
                   SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    analyze_table('TM_WZ', 'WRK_CLINICAL_DATA', jobId);
    analyze_table('TM_WZ', 'WT_TRIAL_NODES', jobId);
    analyze_table('I2B2METADATA', 'I2B2', jobId);
    analyze_table('I2B2DEMODATA', 'PATIENT_DIMENSION', jobId);

    --Insert into observation_fact
    --22 July 2013. Performace fix by TR. Set nologging.
    INSERT /*+ APPEND */ INTO observation_fact nologging
    (patient_num,
     concept_cd,
     modifier_cd,
     valtype_cd,
     tval_char,
     nval_num,
     sourcesystem_cd,
     import_date,
     valueflag_cd,
     PROVIDER_ID,
     location_cd,
     instance_num
    )
      SELECT
        /*+opt_param('_optimizer_cartesian_enabled','false')*/ DISTINCT
        c.patient_num,
        i.c_basecode,
        a.study_id,
        a.data_type,
        CASE WHEN a.data_type = 'T'
          THEN a.data_value
        ELSE 'E' --Stands for Equals for numeric types
        END,
        CASE WHEN a.data_type = 'N'
          THEN a.data_value
        ELSE NULL --Null for text types
        END,
        c.sourcesystem_cd,
        sysdate,
        '@',
        '@',
        '@',
        1
      FROM wrk_clinical_data a
        , patient_dimension c
        , wt_trial_nodes t
        , i2b2 i
      WHERE a.usubjid = c.sourcesystem_cd
            AND nvl(a.category_cd, '@') = nvl(t.category_cd, '@')
            AND nvl(a.data_label, '**NULL**') = nvl(t.data_label, '**NULL**')
            AND nvl(a.visit_name, '**NULL**') = nvl(t.visit_name, '**NULL**')
            AND t.leaf_node = i.c_fullname
            AND NOT exists-- don't insert if lower level node exists
      (SELECT 1
       FROM wt_trial_nodes x
       --where x.leaf_node like t.leaf_node || '%_'
       --Jule 2013. Performance fix by TR. Find if any leaf parent node is current
       WHERE (SUBSTR(x.leaf_node, 1, INSTR(x.leaf_node, '\', -2))) = t.leaf_node

      );

    -- Performace fix. re create dropped index
    EXECUTE IMMEDIATE ('CREATE UNIQUE INDEX "I2B2DEMODATA"."OB_FACT_PK" ON "I2B2DEMODATA"."OBSERVATION_FACT" ("ENCOUNTER_NUM", "PATIENT_NUM", "CONCEPT_CD", "PROVIDER_ID", "START_DATE", "MODIFIER_CD")');
    EXECUTE IMMEDIATE ('CREATE INDEX "I2B2DEMODATA"."IDX_OB_FACT_1" ON "I2B2DEMODATA"."OBSERVATION_FACT" ( "CONCEPT_CD" )');
    EXECUTE IMMEDIATE ('CREATE INDEX "I2B2DEMODATA"."IDX_OB_FACT_2" ON "I2B2DEMODATA"."OBSERVATION_FACT" ("CONCEPT_CD", "PATIENT_NUM", "ENCOUNTER_NUM")');
    EXECUTE IMMEDIATE ('CREATE INDEX "I2B2DEMODATA"."IDX_OB_FACT_PATIENT_NUMBER" ON "I2B2DEMODATA"."OBSERVATION_FACT" ("PATIENT_NUM", "CONCEPT_CD")');


    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Insert trial into I2B2DEMODATA observation_fact', SQL%ROWCOUNT,
                   stepCt, 'Done');

    COMMIT;

    --July 2013. Performance fix by TR. Prepare precompute tree

    I2B2_CREATE_FULL_TREE(topNode, jobId);


    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Create i2b2 full tree', SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    --July 2013. Performance fix by TR.
    EXECUTE IMMEDIATE ('truncate table TM_WZ.I2B2_LOAD_PATH_WITH_COUNT');

    INSERT INTO i2b2_load_path_with_count
      SELECT
        /*+ parallel(4) */
        p.c_fullname,
        count(*)
      FROM i2b2 p
        --,i2b2 c
        , I2B2_LOAD_TREE_FULL tree
      WHERE p.c_fullname LIKE topNode || '%'
            --and c.c_fullname like p.c_fullname || '%'
            AND p.rowid = tree.IDROOT
      --and c.rowid = tree.IDCHILD
      GROUP BY P.C_FULLNAME;

    COMMIT;

    analyze_table('TM_WZ', 'I2B2_LOAD_PATH_WITH_COUNT', jobId);

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Create i2b2 load path with count', SQL%ROWCOUNT, stepCt,
                   'Done');
    COMMIT;

    --	update c_visualattributes for all nodes in study, done to pick up node that changed from leaf/numeric to folder/text
    --July 2013. Performance fix by TR. join by precompute tree
    UPDATE /*+ parallel(i2b2, 4) */ i2b2 a
    SET C_VISUALATTRIBUTES = (
      SELECT CASE WHEN u.nbr_children = 1
        THEN 'L' || substr(a.c_visualattributes, 2, 2)
             ELSE 'F' || substr(a.c_visualattributes, 2, 1) ||
                  CASE WHEN u.c_fullname = topNode
                    THEN 'S'
                  ELSE '' END
             END
      FROM i2b2_load_path_with_count u
      WHERE a.c_fullname = u.c_fullname)
    WHERE EXISTS
    (SELECT 1
     FROM i2b2 x
     WHERE x.c_fullname LIKE topNode || '%' AND a.c_fullname = x.c_fullname);

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Update c_visualattributes for study', SQL%ROWCOUNT, stepCt,
                   'Done');

    COMMIT;

    UPDATE i2b2 a
    SET c_visualattributes = 'FAS'
    WHERE a.c_fullname = topNode;

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Update visual attributes for study nodes in I2B2METADATA i2b2',
                   SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    update i2b2metadata.i2b2 t
    set c_columndatatype = 'T'
      ,c_metadataxml = null
      ,c_visualattributes='LAH'
    where t.c_fullname in (
      select leaf_node from tm_cz.wt_trial_nodes
    );
    cz_write_audit(jobId, databaseName, procedureName, 'Update visual attributes for GWAS nodes in I2B2METADATA i2b2',
                   SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;


    UPDATE i2b2 b
    SET sourcesystem_cd = NULL, c_comment = NULL
    WHERE b.sourcesystem_cd = TrialId
          AND length(b.c_fullname) < length(topNode);

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Set sourcesystem_cd to null for added upper-level nodes',
                   SQL%ROWCOUNT, stepCt, 'Done');
    COMMIT;

    i2b2_create_concept_counts(topNode, jobID, 'N');

    --	delete each node that is hidden after create concept counts
    FOR r_delNodes IN delNodes LOOP

      --	deletes hidden nodes for a trial one at a time

      i2b2_delete_1_node(r_delNodes.c_fullname);
      stepCt := stepCt + 1;
      stepCt := stepCt + 1;
      tText := 'Deleted node: ' || r_delNodes.c_fullname;

      cz_write_audit(jobId, databaseName, procedureName, tText, SQL%ROWCOUNT, stepCt, 'Done');

    END LOOP;

    i2b2_create_security_for_trial(TrialId, secureStudy, jobID);
    i2b2_load_security_data(jobID);

    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'End I2B2_PROCESS_GWAS_DATA', 0, stepCt, 'Done');

    ---Cleanup OVERALL JOB if this proc is being run standalone
    IF newJobFlag = 1
    THEN
      cz_end_audit(jobID, 'SUCCESS');
    END IF;

    rtnCode := 0;

    EXCEPTION
    WHEN duplicate_values THEN
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Duplicate values found in key columns', 0, stepCt, 'Done');
    cz_error_handler(jobID, procedureName);
    cz_end_audit(jobID, 'FAIL');
    rtnCode := 16;
    WHEN invalid_topNode THEN
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName, 'Path specified in top_node must contain at least 2 nodes', 0,
                   stepCt, 'Done');
    cz_error_handler(jobID, procedureName);
    cz_end_audit(jobID, 'FAIL');
    rtnCode := 16;
    WHEN multiple_visit_names THEN
    stepCt := stepCt + 1;
    cz_write_audit(jobId, databaseName, procedureName,
                   'Not for all subject_id/category/label/value visit names specified. Visit names should be all empty or specified for all records.',
                   0, stepCt, 'Done');
    cz_error_handler(jobID, procedureName);
    cz_end_audit(jobID, 'FAIL');
    rtnCode := 16;
    WHEN OTHERS THEN
    --Handle errors.
    cz_error_handler(jobID, procedureName);
    --End Proc
    cz_end_audit(jobID, 'FAIL');
    rtnCode := 16;

  END;
/
