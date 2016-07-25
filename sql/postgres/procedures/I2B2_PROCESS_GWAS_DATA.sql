-- Function: tm_dataloader.i2b2_process_gwas_data(character varying, character varying, character varying, numeric)

-- DROP FUNCTION tm_dataloader.i2b2_process_gwas_data(character varying, character varying, character varying, numeric);

CREATE OR REPLACE FUNCTION tm_dataloader.i2b2_process_gwas_data(
  trial_id     CHARACTER VARYING,
  top_node     CHARACTER VARYING,
  secure_study CHARACTER VARYING DEFAULT 'N' :: CHARACTER VARYING,
  currentjobid NUMERIC DEFAULT (-1))
  RETURNS NUMERIC AS
$BODY$
DECLARE

  --Audit variables
  databaseName         VARCHAR(100);
  procedureName        VARCHAR(100);
  jobID                NUMERIC(18, 0);
  stepCt               NUMERIC(18, 0);
  rowCt                NUMERIC(18, 0);
  errorNumber          CHARACTER VARYING;
  errorMessage         CHARACTER VARYING;
  rtnCd                INTEGER;

  topNode              VARCHAR(2000);
  topLevel             NUMERIC(10, 0);
  root_node            VARCHAR(2000);
  root_level           INTEGER;
  study_name           VARCHAR(2000);
  TrialID              VARCHAR(100);
  secureStudy          VARCHAR(200);
  etlDate              TIMESTAMP;
  tPath                VARCHAR(2000);
  pCount               INTEGER;
  pExists              INTEGER;
  rtnCode              INTEGER;
  tText                VARCHAR(2000);
  recreateIndexes      BOOLEAN;
  recreateIndexesSql   TEXT;
  leaf_fullname        VARCHAR(700);
  updated_patient_nums INTEGER [];
  pathRegexp           VARCHAR(2000);
  updatedPath          VARCHAR(2000);
  cur_row              RECORD;
  pathCount            INTEGER;

    addNodes CURSOR IS
    SELECT DISTINCT
      leaf_node,
      node_name
    FROM wt_trial_nodes a;

  --	cursor to define the path for delete_one_node  this will delete any nodes that are hidden after i2b2_create_concept_counts

    delNodes CURSOR IS
    SELECT DISTINCT c_fullname
    FROM i2b2metadata.i2b2
    WHERE c_fullname LIKE topNode || '%' ESCAPE '`'
                                                AND substr(c_visualattributes, 2, 1) = 'H';

BEGIN

  TrialID := upper(trial_id);
  secureStudy := upper(secure_study);

  databaseName := current_schema();
  procedureName := 'i2b2_process_gwas_data';

  --Audit JOB Initialization
  --If Job ID does not exist, then this is a single procedure run and we need to create it
  SELECT CASE WHEN coalesce(currentjobid, -1) < 1
    THEN cz_start_audit(procedureName, databaseName)
         ELSE currentjobid END
  INTO jobId;

  stepCt := 0;
  stepCt := stepCt + 1;
  tText := 'Start i2b2_process_gwas_data for ' || TrialId || ' topNode ' || topNode;
  SELECT cz_write_audit(jobId, databaseName, procedureName, tText, 0, stepCt, 'Done')
  INTO rtnCd;

  IF (secureStudy NOT IN ('Y', 'N'))
  THEN
    secureStudy := 'Y';
  END IF;

  topNode := REGEXP_REPLACE('\' || top_node || '\', '(\\){2,}', '\', 'g');

  --	figure out how many nodes (folders) are at study name and above
  --	\Public Studies\Clinical Studies\Pancreatic_Cancer_Smith_GSE22780\: topLevel = 4, so there are 3 nodes
  --	\Public Studies\GSE12345\: topLevel = 3, so there are 2 nodes

  SELECT length(topNode) - length(replace(topNode, '\', ''))
  INTO topLevel;

  IF topLevel < 3
  THEN
    stepCt := stepCt + 1;
    SELECT
      cz_write_audit(jobId, databaseName, procedureName, 'Path specified in top_node must contain at least 2 nodes', 0,
                     stepCt, 'Done')
    INTO rtnCd;
    SELECT cz_error_handler(jobID, procedureName, '-1', 'Application raised error')
    INTO rtnCd;
    SELECT cz_end_audit(jobID, 'FAIL')
    INTO rtnCd;
    RETURN -16;
  END IF;

  EXECUTE ('truncate table wrk_clinical_data');

  BEGIN
    INSERT INTO wrk_clinical_data
    (study_id
      , site_id
      , subject_id
      , visit_name
      , data_label
      , data_value
      , category_cd
      , ctrl_vocab_code
      , sample_cd
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
        NULL,
        NULL
      FROM lt_src_gwas_data;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  GET DIAGNOSTICS rowCt := ROW_COUNT;
  stepCt := stepCt + 1;
  SELECT
    cz_write_audit(jobId, databaseName, procedureName, 'Load lt_src_gwas_data to work table', rowCt, stepCt, 'Done')
  INTO rtnCd;

  -- Get root_node from topNode

  SELECT parse_nth_value(topNode, 2, '\')
  INTO root_node;

  SELECT count(*)
  INTO pExists
  FROM i2b2metadata.table_access
  WHERE c_name = root_node;

  SELECT count(*)
  INTO pCount
  FROM i2b2metadata.i2b2
  WHERE c_name = root_node;

  IF pExists = 0 OR pCount = 0
  THEN
    SELECT i2b2_add_root_node(root_node, jobId)
    INTO rtnCd;
  END IF;

  SELECT c_hlevel
  INTO root_level
  FROM i2b2metadata.table_access
  WHERE c_name = root_node;

  -- Get study name from topNode

  SELECT parse_nth_value(topNode, topLevel, '\')
  INTO study_name;

  --	Add any upper level nodes as needed

  tPath := REGEXP_REPLACE(replace(topNode, study_name, ''), '(\\){2,}', '\', 'g');
  SELECT length(tPath) - length(replace(tPath, '\', ''))
  INTO pCount;

  IF pCount > 2
  THEN
    stepCt := stepCt + 1;
    SELECT
      cz_write_audit(jobId, databaseName, procedureName, 'Adding upper-level nodes for "' || tPath || '"', 0, stepCt,
                     'Done')
    INTO rtnCd;
    SELECT i2b2_fill_in_tree(NULL, tPath, jobId)
    INTO rtnCd;
  END IF;

  SELECT count(*)
  INTO pExists
  FROM i2b2metadata.i2b2
  WHERE c_fullname = topNode;

  --	add top node for study

  IF pExists = 0
  THEN
    SELECT i2b2_add_node(TrialId, topNode, study_name, jobId)
    INTO rtnCd;
  END IF;

  --	Set data_type, category_path, and usubjid

  UPDATE wrk_clinical_data
  SET data_type     = 'T'
    -- All tag values prefixed with $$, so we should remove prefixes in category_path
    , category_path = regexp_replace(
      regexp_replace(replace(replace(category_cd, '_', ' '), '+', '\'), '\$\$\d*[A-Z]\{([^}]+)\}', '\1', 'g'),
      '\$\$\d*[A-Z]', '', 'g')
    , usubjid       = REGEXP_REPLACE(TrialID || ':' || coalesce(site_id, '') || ':' || subject_id,
                                     '(::){1,}', ':', 'g');
  GET DIAGNOSTICS rowCt := ROW_COUNT;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Set columns in wrk_clinical_data', rowCt, stepCt, 'Done')
  INTO rtnCd;

  BEGIN
    DELETE FROM i2b2demodata.observation_fact f
    WHERE f.modifier_cd = TrialId
          AND f.concept_cd NOT IN
              (SELECT DISTINCT concept_code AS concept_cd
               FROM deapp.de_subject_sample_mapping
               WHERE trial_name = TrialId
                     AND concept_code IS NOT NULL
               UNION
               SELECT DISTINCT platform_cd AS concept_cd
               FROM deapp.de_subject_sample_mapping
               WHERE trial_name = TrialId
                     AND platform_cd IS NOT NULL
               UNION
               SELECT DISTINCT sample_type_cd AS concept_cd
               FROM deapp.de_subject_sample_mapping
               WHERE trial_name = TrialId
                     AND sample_type_cd IS NOT NULL
               UNION
               SELECT DISTINCT tissue_type_cd AS concept_cd
               FROM deapp.de_subject_sample_mapping
               WHERE trial_name = TrialId
                     AND tissue_type_cd IS NOT NULL
               UNION
               SELECT DISTINCT timepoint_cd AS concept_cd
               FROM deapp.de_subject_sample_mapping
               WHERE trial_name = TrialId
                     AND timepoint_cd IS NOT NULL
               UNION
               SELECT DISTINCT concept_cd AS concept_cd
               FROM deapp.de_subject_snp_dataset
               WHERE trial_name = TrialId
                     AND concept_cd IS NOT NULL);
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT
    cz_write_audit(jobId, databaseName, procedureName, 'Delete clinical data for study from observation_fact', rowCt,
                   stepCt, 'Done')
  INTO rtnCd;
  --Remove Invalid pipes in the data values.
  --RULE: If Pipe is last or first, delete it
  --If it is in the middle replace with a dash

  BEGIN
    UPDATE wrk_clinical_data
    SET data_value = replace(trim('|' FROM data_value), '|', '-')
    WHERE data_value LIKE '%|%';
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  GET DIAGNOSTICS rowCt := ROW_COUNT;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Remove pipes in data_value', rowCt, stepCt, 'Done')
  INTO rtnCd;

  --Remove invalid Parens in the data
  --They have appeared as empty pairs or only single ones.

  BEGIN
    UPDATE wrk_clinical_data
    SET data_value = replace(data_value, '(', '')
    WHERE data_value LIKE '%()%'
          OR data_value LIKE '%( )%'
          OR (data_value LIKE '%(%' AND data_value NOT LIKE '%)%');
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Remove empty parentheses 1', rowCt, stepCt, 'Done')
  INTO rtnCd;

  BEGIN
    UPDATE wrk_clinical_data
    SET data_value = replace(data_value, ')', '')
    WHERE data_value LIKE '%()%'
          OR data_value LIKE '%( )%'
          OR (data_value LIKE '%)%' AND data_value NOT LIKE '%(%');
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Remove empty parentheses 2', rowCt, stepCt, 'Done')
  INTO rtnCd;

  --Replace the Pipes with Commas in the data_label column
  BEGIN
    UPDATE wrk_clinical_data
    SET data_label = replace(data_label, '|', ',')
    WHERE data_label LIKE '%|%';
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT
    cz_write_audit(jobId, databaseName, procedureName, 'Replace pipes with comma in data_label', rowCt, stepCt, 'Done')
  INTO rtnCd;

  --	set data_label to null when it duplicates the last part of the category_path
  --	Remove data_label from last part of category_path when they are the same

  UPDATE wrk_clinical_data tmp
  SET
    category_cd = regexp_replace(regexp_replace(tmp.category_cd, '\$\$\d*[A-Z]\{([^}]+)\}', '\1', 'g'), '\$\$\d*[A-Z]',
                                 '', 'g')
  WHERE tmp.category_cd LIKE '%$$%';

  stepCt := stepCt + 1;
  GET DIAGNOSTICS rowCt := ROW_COUNT;
  PERFORM cz_write_audit(jobId, databaseName, procedureName, 'Remove tag markers', rowCt, stepCt, 'Done');

  BEGIN
    UPDATE wrk_clinical_data tpm
    --set data_label = null
    SET category_path = substr(tpm.category_path, 1, instr(tpm.category_path, '\', -2, 1) - 1)
      , category_cd   = substr(tpm.category_cd, 1, instr(tpm.category_cd, '+', -2, 1) - 1)
    WHERE (tpm.category_cd, tpm.data_label) IN
          (SELECT DISTINCT
             t.category_cd,
             t.data_label
           FROM wrk_clinical_data t
           WHERE upper(substr(t.category_path, instr(t.category_path, '\', -1, 1) + 1,
                              length(t.category_path) - instr(t.category_path, '\', -1, 1)))
                 = upper(t.data_label)
                 AND t.data_label IS NOT NULL)
          AND tpm.data_label IS NOT NULL AND instr(tpm.category_path, '\', -2, 1) > 0 AND
          instr(tpm.category_cd, '+', -2, 1) > 0;
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Set data_label to null when found in category_path', rowCt,
                        stepCt, 'Done')
  INTO rtnCd;

  BEGIN
    UPDATE wrk_clinical_data
    SET data_label    = replace(
        replace(replace(replace(replace(data_label, '%', ' Pct'), '&', ' and '), '+', ' and '), '_', ' '), '(plus)',
        '+')
      , data_value    = replace(replace(replace(replace(data_value, '%', ' Pct'), '&', ' and '), '+', ' and '),
                                '(plus)', '+')
      , category_cd   = replace(replace(category_cd, '%', ' Pct'), '&', ' and ')
      , category_path = replace(replace(replace(category_path, '%', ' Pct'), '&', ' and '), '(plus)', '+');

    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;

  --Trim trailing and leadling spaces as well as remove any double spaces, remove space from before comma, remove trailing comma
  BEGIN
    UPDATE wrk_clinical_data
    SET data_label = trim(TRAILING ',' FROM trim(replace(replace(data_label, '  ', ' '), ' ,', ','))),
      data_value   = trim(TRAILING ',' FROM trim(replace(replace(data_value, '  ', ' '), ' ,', ','))),
      --		sample_type = trim(trailing ',' from trim(replace(replace(sample_type,'  ', ' '),' ,',','))),
      visit_name   = trim(TRAILING ',' FROM trim(replace(replace(visit_name, '  ', ' '), ' ,', ',')));
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT
    cz_write_audit(jobId, databaseName, procedureName, 'Remove leading, trailing, double spaces', rowCt, stepCt, 'Done')
  INTO rtnCd;

  BEGIN
    UPDATE wrk_clinical_data t
    SET visit_name = NULL
    WHERE (t.category_cd, t.visit_name, t.data_value) IN
          (SELECT DISTINCT
             tpm.category_cd,
             tpm.visit_name,
             tpm.data_value
           FROM wrk_clinical_data tpm
           WHERE tpm.visit_name = tpm.data_value);
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT
    cz_write_audit(jobId, databaseName, procedureName, 'Set visit_name to null when found in data_value', rowCt, stepCt,
                   'Done')
  INTO rtnCd;

  --1. DETERMINE THE DATA_TYPES OF THE FIELDS
  --	replaced cursor with update, used temp table to store category_cd/data_label because correlated subquery ran too long

  EXECUTE ('truncate table wt_num_data_types');

  BEGIN
    INSERT INTO wt_num_data_types
    (category_cd
      , data_label
      , visit_name
    )
      SELECT
        category_cd,
        data_label,
        visit_name
      FROM wrk_clinical_data
      WHERE data_value IS NOT NULL
      GROUP BY category_cd
        , data_label
        , visit_name
      HAVING sum(is_numeric(data_value)) = 0;
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT
    cz_write_audit(jobId, databaseName, procedureName, 'Insert numeric data into WZ wt_num_data_types', rowCt, stepCt,
                   'Done')
  INTO rtnCd;

  BEGIN
    UPDATE wrk_clinical_data t
    SET data_type = 'N'
    WHERE exists
    (SELECT 1
     FROM wt_num_data_types x
     WHERE coalesce(t.category_cd, '@') = coalesce(x.category_cd, '@')
           AND coalesce(t.data_label, '**NULL**') = coalesce(x.data_label, '**NULL**')
           AND coalesce(t.visit_name, '**NULL**') = coalesce(x.visit_name, '**NULL**')
    );
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT
    cz_write_audit(jobId, databaseName, procedureName, 'Updated data_type flag for numeric data_types', rowCt, stepCt,
                   'Done')
  INTO rtnCd;

  EXECUTE ('truncate table wt_clinical_data_dups');

  BEGIN
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
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Check for duplicate key columns', rowCt, stepCt, 'Done')
  INTO rtnCd;

  IF rowCt > 0
  THEN
    stepCt := stepCt + 1;
    SELECT
      cz_write_audit(jobId, databaseName, procedureName, 'Duplicate values found in key columns', 0, stepCt, 'Done')
    INTO rtnCd;
    SELECT cz_error_handler(jobID, procedureName, '-1', 'Application raised error')
    INTO rtnCd;
    SELECT cz_end_audit(jobID, 'FAIL')
    INTO rtnCd;
    RETURN -16;
  END IF;

  -- Build all needed leaf nodes in one pass for both numeric and text nodes
  EXECUTE ('truncate table wt_trial_nodes');

  BEGIN
    INSERT INTO wt_trial_nodes
    (leaf_node
      , category_cd
      , visit_name
      , data_label
      , data_value
      , data_type
      , valuetype_cd
    )
      SELECT DISTINCT
        CASE
        --	Text data_type (default node)
        WHEN a.data_type = 'T'
          THEN regexp_replace(topNode || replace(
              replace(replace(coalesce(a.category_path, ''), 'DATALABEL', coalesce(a.data_label, '')), 'VISITNAME',
                      coalesce(a.visit_name, '')), 'DATAVALUE', coalesce(a.data_value, '')) || '\', '(\\){2,}', '\',
                              'g')
        --	else is numeric data_type and default_node
        ELSE regexp_replace(topNode ||
                            replace(replace(coalesce(a.category_path, ''), 'DATALABEL', coalesce(a.data_label, '')),
                                    'VISITNAME', coalesce(a.visit_name, '')) || '\', '(\\){2,}', '\', 'g')
        END           AS leaf_node,
        a.category_cd,
        a.visit_name,
        a.data_label,
        CASE WHEN a.data_type = 'T'
          THEN a.data_value
        ELSE NULL END AS data_value,
        a.data_type,
        a.valuetype_cd
      FROM wrk_clinical_data a;
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Create leaf nodes for trial', rowCt, stepCt, 'Done')
  INTO rtnCd;

  --	set node_name

  BEGIN
    UPDATE wt_trial_nodes
    SET node_name = parse_nth_value(leaf_node, length(leaf_node) - length(replace(leaf_node, '\', '')), '\');
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Updated node name for leaf nodes', rowCt, stepCt, 'Done')
  INTO rtnCd;

  --	insert subjects into patient_dimension if needed

  EXECUTE ('truncate table wt_subject_info');

  BEGIN
    INSERT INTO wt_subject_info
    (usubjid,
     age_in_years_num,
     sex_cd,
     race_cd
    )
      SELECT
        a.usubjid,
        coalesce(max(CASE WHEN upper(a.data_label) = 'AGE'
          THEN CASE WHEN is_numeric(a.data_value) = 1
            THEN 0
               ELSE floor(a.data_value :: NUMERIC) END
                     WHEN upper(a.data_label) LIKE '%(AGE)'
                       THEN CASE WHEN is_numeric(a.data_value) = 1
                         THEN 0
                            ELSE floor(a.data_value :: NUMERIC) END
                     ELSE NULL END), 0)         AS age,
        coalesce(max(CASE WHEN upper(a.data_label) = 'SEX'
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
            ELSE NULL END)                      AS race
      FROM wrk_clinical_data a
      GROUP BY a.usubjid;
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Insert subject information into temp table', rowCt, stepCt,
                        'Done')
  INTO rtnCd;

  updated_patient_nums := array(
      SELECT pat.patient_num
      FROM wt_subject_info si, patient_dimension pat
      WHERE si.usubjid = pat.sourcesystem_cd
  );

  --	Delete dropped subjects from patient_dimension if they do not exist in de_subject_sample_mapping
  BEGIN
    DELETE FROM i2b2demodata.patient_dimension
    WHERE sourcesystem_cd IN
          (SELECT DISTINCT pd.sourcesystem_cd
           FROM i2b2demodata.patient_dimension pd
           WHERE pd.sourcesystem_cd LIKE TrialId || ':%'
           EXCEPT
           SELECT DISTINCT cd.usubjid
           FROM wrk_clinical_data cd)
          AND patient_num NOT IN
              (SELECT DISTINCT sm.patient_id
               FROM deapp.de_subject_sample_mapping sm);
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  --	update patients with changed information
  BEGIN
    WITH nsi AS (SELECT
                   t.usubjid,
                   t.sex_cd,
                   t.age_in_years_num,
                   t.race_cd
                 FROM wt_subject_info t)
    UPDATE i2b2demodata.patient_dimension
    SET sex_cd           = nsi.sex_cd
      , age_in_years_num = nsi.age_in_years_num
      , race_cd          = nsi.race_cd
    FROM nsi
    WHERE sourcesystem_cd = nsi.usubjid;
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName,
                        'Update subjects with changed demographics in patient_dimension', rowCt, stepCt, 'Done')
  INTO rtnCd;

  --	insert new subjects into patient_dimension

  BEGIN
    INSERT INTO i2b2demodata.patient_dimension
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
        nextval('i2b2demodata.seq_patient_num'),
        t.sex_cd,
        t.age_in_years_num,
        t.race_cd,
        current_timestamp,
        current_timestamp,
        current_timestamp,
        t.usubjid
      FROM wt_subject_info t
      WHERE t.usubjid IN
            (SELECT DISTINCT cd.usubjid
             FROM wt_subject_info cd
             EXCEPT
             SELECT DISTINCT pd.sourcesystem_cd
             FROM i2b2demodata.patient_dimension pd
             WHERE pd.sourcesystem_cd LIKE TrialId || ':%');
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Insert new subjects into patient_dimension', rowCt, stepCt,
                        'Done')
  INTO rtnCd;

  BEGIN
    INSERT INTO i2b2demodata.concept_dimension
    (concept_cd
      , concept_path
      , name_char
      , update_date
      , download_date
      , import_date
      , sourcesystem_cd
    )
      SELECT
        nextval('i2b2demodata.concept_id'),
        x.leaf_node,
        x.node_name,
        current_timestamp,
        current_timestamp,
        current_timestamp,
        TrialId
      FROM (SELECT DISTINCT
              c.leaf_node,
              c.node_name :: TEXT AS node_name
            FROM wt_trial_nodes c
            WHERE NOT exists
            (SELECT 1
             FROM i2b2demodata.concept_dimension x
             WHERE c.leaf_node = x.concept_path)
           ) x;
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName,
                        'Inserted new leaf nodes into I2B2DEMODATA concept_dimension', rowCt, stepCt, 'Done')
  INTO rtnCd;

  --	update i2b2 with name, data_type and xml for leaf nodes
  BEGIN
    UPDATE i2b2metadata.i2b2
    SET c_name           = ncd.node_name
      , c_columndatatype = 'T'
      , c_metadataxml    = null
    FROM wt_trial_nodes ncd
    WHERE c_fullname = ncd.leaf_node;
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Updated name and data type in i2b2 if changed', rowCt,
                        stepCt, 'Done')
  INTO rtnCd;

  BEGIN
    INSERT INTO i2b2metadata.i2b2
    (c_hlevel
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
      , m_applied_path
      , c_metadataxml
    )
      SELECT DISTINCT
        (length(c.concept_path) - coalesce(length(replace(c.concept_path, '\', '')), 0)) / length('\') - 2 +
        root_level,
        c.concept_path,
        c.name_char,
        'LA',
        'N',
        'CONCEPT_CD',
        'CONCEPT_DIMENSION',
        'CONCEPT_PATH',
        c.concept_path,
        c.concept_path,
        current_timestamp,
        current_timestamp,
        current_timestamp,
        c.sourcesystem_cd,
        c.concept_cd,
        'LIKE',
        'T',
        'trial:' || TrialID,
        '@',
        null
      FROM i2b2demodata.concept_dimension c
        , wt_trial_nodes t
      WHERE c.concept_path = t.leaf_node
            AND NOT exists
      (SELECT 1
       FROM i2b2metadata.i2b2 x
       WHERE c.concept_path = x.c_fullname);
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Inserted leaf nodes into I2B2METADATA i2b2', rowCt, stepCt,
                        'Done')
  INTO rtnCd;

  --New place form fill_in_tree
  SELECT i2b2_fill_in_tree(TrialId, topNode, jobID)
  INTO rtnCd;

  ANALYZE wrk_clinical_data;
  ANALYZE wt_trial_nodes;

  BEGIN
    SET enable_mergejoin = f;
    CREATE TEMPORARY TABLE tmp_observation_facts WITHOUT OIDS AS
      SELECT DISTINCT
        c.patient_num     AS encounter_num,
        c.patient_num,
        i.c_basecode,
        current_timestamp AS start_date,
        a.study_id,
        a.data_type,
        CASE WHEN a.data_type = 'T'
          THEN a.data_value
        ELSE 'E' --Stands for Equals for numeric types
        END               AS tval_char,
        CASE WHEN a.data_type = 'N'
          THEN a.data_value :: NUMERIC
        ELSE NULL --Null for text types
        END               AS nval_num,
        a.study_id        AS sourcesystem_cd,
        current_timestamp AS import_date,
        '@'               AS valueflag_cd,
        '@'               AS provider_cd,
        '@'               AS location_cd,
        0                 AS instance_num,
        sample_cd         AS sample_cd
      FROM wrk_clinical_data a
        , i2b2demodata.patient_dimension c
        , wt_trial_nodes t
        , i2b2metadata.i2b2 i
      WHERE a.usubjid = c.sourcesystem_cd
            AND coalesce(a.category_cd, '@') = coalesce(t.category_cd, '@')
            AND coalesce(a.data_label, '**NULL**') = coalesce(t.data_label, '**NULL**')
            AND coalesce(a.visit_name, '**NULL**') = coalesce(t.visit_name, '**NULL**')
            AND CASE WHEN a.data_type = 'T'
        THEN a.data_value
                ELSE '**NULL**' END = coalesce(t.data_value, '**NULL**')
            AND t.leaf_node = i.c_fullname
            --	  and not exists		-- don't insert if lower level node exists
            --		 (select 1 from wt_trial_nodes x
            --		  where x.leaf_node like t.leaf_node || '%_' escape '`')
            --	  and a.data_value is not null;
            AND NOT exists-- don't insert if lower level node exists
      (
          SELECT 1
          FROM wt_trial_nodes x
          WHERE regexp_replace(x.leaf_node, '[^\\]+\\$', '') = t.leaf_node
      )
            AND a.data_value IS NOT NULL
            AND NOT (a.data_type = 'N' AND a.data_value = '');
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    stepCt := stepCt + 1;
    SET enable_mergejoin TO DEFAULT;
    SELECT cz_write_audit(jobId, databaseName, procedureName, 'Collect observation facts', rowCt, stepCt, 'Done')
    INTO rtnCd;

    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        SET enable_mergejoin TO DEFAULT;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;

  recreateIndexes := TRUE;
  IF rowCt < 200
  THEN
    recreateIndexes := FALSE;
  END IF;

  IF recreateIndexes = TRUE
  THEN
    SELECT DROP_ALL_INDEXES('i2b2demodata', 'observation_fact')
    INTO recreateIndexesSql;
    stepCt := stepCt + 1;
    SELECT cz_write_audit(jobId, databaseName, procedureName, 'Drop observation facts indexes', 0, stepCt, 'Done')
    INTO rtnCd;
  END IF;

  --Insert into observation_fact
  BEGIN
    INSERT INTO i2b2demodata.observation_fact
    (encounter_num,
     patient_num,
     concept_cd,
     start_date,
     modifier_cd,
     valtype_cd,
     tval_char,
     nval_num,
     sourcesystem_cd,
     import_date,
     valueflag_cd,
     provider_id,
     location_cd,
     instance_num,
     sample_cd
    )
      SELECT *
      FROM tmp_observation_facts;

    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Insert trial into I2B2DEMODATA observation_fact', rowCt,
                        stepCt, 'Done')
  INTO rtnCd;

  --July 2013. Performance fix by TR. Prepare precompute tree
  SELECT I2B2_CREATE_FULL_TREE(topNode, jobId)
  INTO rtnCd;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Create i2b2 full tree', 0, stepCt, 'Done')
  INTO rtnCd;

  IF recreateIndexes = TRUE
  THEN
    EXECUTE (recreateIndexesSql);
    stepCt := stepCt + 1;
    SELECT cz_write_audit(jobId, databaseName, procedureName, 'Create observation facts index', 0, stepCt, 'Done')
    INTO rtnCd;
  END IF;


  DELETE FROM i2b2_load_path_with_count;

  INSERT INTO i2b2_load_path_with_count
    SELECT
      p.c_fullname,
      count(*)
    FROM i2b2metadata.i2b2 p
      --,i2b2metadata.i2b2 c
      , I2B2_LOAD_TREE_FULL tree
    WHERE p.c_fullname LIKE topNode || '%' ESCAPE '`'
                                                  --and c.c_fullname like p.c_fullname || '%'
                                                  AND p.RECORD_ID = tree.IDROOT
    --and c.rowid = tree.IDCHILD
    GROUP BY P.C_FULLNAME;

  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Create i2b2 full tree counts', 0, stepCt, 'Done')
  INTO rtnCd;

  --	update c_visualattributes for all nodes in study, done to pick up node that changed c_columndatatype
  BEGIN

    UPDATE i2b2metadata.i2b2 b
    SET c_visualattributes = CASE WHEN u.nbr_children = 1
      THEN 'L' || substr(b.c_visualattributes, 2, 2)
                             ELSE 'F' || substr(b.c_visualattributes, 2, 1) ||
                                  CASE WHEN u.c_fullname = topNode
                                    THEN 'S'
                                  ELSE '' END
                             END
      , c_columndatatype   = CASE WHEN u.nbr_children > 1
      THEN 'T'
                             ELSE b.c_columndatatype END
    FROM i2b2_load_path_with_count u
    WHERE b.c_fullname = u.c_fullname
          AND b.c_fullname IN
              (SELECT x.c_fullname
               FROM i2b2metadata.i2b2 x
               WHERE x.c_fullname LIKE topNode || '%' ESCAPE '`');
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Set c_visualattributes in i2b2', rowCt, stepCt, 'Done')
  INTO rtnCd;

  begin
    with upd as (select x.leaf_node from wt_trial_nodes x group by x.leaf_node)
    update i2b2metadata.i2b2 t
    set c_columndatatype = 'T'
      ,c_metadataxml = null
      ,c_visualattributes='LAH'
    from upd
    where t.c_fullname = upd.leaf_node;
    get diagnostics rowCt := ROW_COUNT;
    exception
    when others then
      errorNumber := SQLSTATE;
      errorMessage := SQLERRM;
      --Handle errors.
      select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
      --End Proc
      select cz_end_audit (jobID, 'FAIL') into rtnCd;
      return -16;
  end;
  stepCt := stepCt + 1;
  select cz_write_audit(jobId,databaseName,procedureName,'Initialize data_type, visualattributes and xml in i2b2',rowCt,stepCt,'Done') into rtnCd;


  -- final procs
  --moved earlier
  --select i2b2_fill_in_tree(TrialId, topNode, jobID) into rtnCd;

  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Finish fill in tree', 0, stepCt, 'Done')
  INTO rtnCd;

  --	set sourcesystem_cd, c_comment to null if any added upper-level nodes

  BEGIN
    UPDATE i2b2metadata.i2b2 b
    SET sourcesystem_cd = NULL, c_comment = NULL
    WHERE b.sourcesystem_cd = TrialId
          AND length(b.c_fullname) < length(topNode);
    GET DIAGNOSTICS rowCt := ROW_COUNT;
    EXCEPTION
    WHEN OTHERS
      THEN
        errorNumber := SQLSTATE;
        errorMessage := SQLERRM;
        --Handle errors.
        SELECT cz_error_handler(jobID, procedureName, errorNumber, errorMessage)
        INTO rtnCd;
        --End Proc
        SELECT cz_end_audit(jobID, 'FAIL')
        INTO rtnCd;
        RETURN -16;
  END;
  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'Set sourcesystem_cd to null for added upper-level nodes',
                        rowCt, stepCt, 'Done')
  INTO rtnCd;

  SELECT i2b2_create_concept_counts(topNode, jobID, 'N')
  INTO rtnCd;

  --	delete each node that is hidden after create concept counts

  FOR r_delNodes IN delNodes LOOP

    --	deletes hidden nodes for a trial one at a time

    SELECT i2b2_delete_1_node(r_delNodes.c_fullname)
    INTO rtnCd;
    stepCt := stepCt + 1;
    tText := 'Deleted node: ' || r_delNodes.c_fullname;
    SELECT cz_write_audit(jobId, databaseName, procedureName, tText, rowCt, stepCt, 'Done')
    INTO rtnCd;

  END LOOP;

  SELECT i2b2_create_security_for_trial(TrialId, secureStudy, jobID)
  INTO rtnCd;
  SELECT i2b2_load_security_data(TrialId, jobID)
  INTO rtnCd;

  stepCt := stepCt + 1;
  SELECT cz_write_audit(jobId, databaseName, procedureName, 'End i2b2_process_gwas_data', 0, stepCt, 'Done')
  INTO rtnCd;

  ---Cleanup OVERALL JOB if this proc is being run standalone
  PERFORM cz_end_audit(jobID, 'SUCCESS')
  WHERE coalesce(currentJobId, -1) <> jobId;

  RETURN 1;

END;

$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET search_path FROM CURRENT
COST 100;

ALTER FUNCTION tm_dataloader.i2b2_process_gwas_data( CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, NUMERIC )
SET search_path = tm_dataloader, tm_cz, tm_lz, tm_wz, i2b2demodata, i2b2metadata, deapp, fmapp, amapp, pg_temp;

ALTER FUNCTION tm_dataloader.i2b2_process_gwas_data( CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, NUMERIC )
OWNER TO postgres;


ALTER FUNCTION tm_dataloader.i2b2_process_gwas_data( CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, NUMERIC )
SET search_path = tm_dataloader, tm_cz, tm_lz, tm_wz, i2b2demodata, i2b2metadata, deapp, fmapp, amapp, pg_temp;

ALTER FUNCTION tm_dataloader.i2b2_process_gwas_data( CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING, NUMERIC )
OWNER TO postgres;
GRANT EXECUTE ON FUNCTION tm_dataloader.i2b2_process_gwas_data(CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING,
                                                               NUMERIC) TO PUBLIC;
GRANT EXECUTE ON FUNCTION tm_dataloader.i2b2_process_gwas_data(CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING,
                                                               NUMERIC) TO postgres;
GRANT EXECUTE ON FUNCTION tm_dataloader.i2b2_process_gwas_data(CHARACTER VARYING, CHARACTER VARYING, CHARACTER VARYING,
                                                               NUMERIC) TO tm_dataloader;
