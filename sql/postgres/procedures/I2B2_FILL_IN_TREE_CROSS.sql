--
-- Name: i2b2_fill_in_tree(character varying, character varying, numeric); Type: FUNCTION; Schema: tm_dataloader; Owner: -
-- Purpose: add missing intermediate tree nodes into table i2b2metadata.i2b2.
--
CREATE OR REPLACE FUNCTION i2b2_fill_in_tree_cross(path varchar, current_job_id numeric DEFAULT (-1))
  RETURNS numeric
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path FROM CURRENT
AS $$
DECLARE
  user_name CONSTANT     varchar := current_user;
  function_name CONSTANT varchar := 'I2B2_FILL_IN_TREE_CROSS';
  new_job_flag           boolean;

  job_id                 numeric;
  step                   numeric;
  error_number           varchar;
  error_message          varchar;
  return_code            numeric;

  new_paths              varchar [];
  array_len              numeric;
  v_count numeric;
  iPath   varchar;

BEGIN
  step := 0;
  --Set Audit Parameters
  new_job_flag := FALSE;
  job_id := current_job_id;
  -- Audit JOB Initialization
  -- If Job ID does not exist, then this is a single procedure run and we need to create it
  IF job_id IS NULL OR job_id < 1
  THEN
    new_job_flag := TRUE;
    select cz_start_audit(function_name, user_name)
    into job_id;
  END IF;

  new_paths := regexp_split_to_array(path, '\\');
  array_len := array_length(new_paths, 1);
  iPath := '';

  for nPath in 2..array_len - 1 LOOP
    iPath := iPath || '\' || new_paths[nPath];
    --       Check if node exists.
    select count(*)
    into v_count
    from i2b2demodata.modifier_dimension
    where modifier_path = iPath || '\';

    --If it doesn't exist, add it
    IF v_count = 0
    THEN
      INSERT INTO i2b2demodata.modifier_dimension (
        modifier_path,
        modifier_cd,
        name_char,
        modifier_level,
        modifier_node_type
      ) VALUES (
        iPath || '\',
        nextval('tm_dataloader.modifier_dimension_seq'),
        new_paths[nPath],
        nPath - 2,
        'F'
      );

      step := step + 1;
      select cz_write_audit(job_id, user_name, function_name, 'Inserting ' || iPath, 0, step, 'Done')
      into return_code;
    END IF;

  end loop;

  IF return_code <> 1
  THEN
    RETURN return_code;
  END IF;

  ---Cleanup OVERALL JOB if this proc is being run standalone
  IF new_job_flag
  THEN
    select cz_end_audit(job_id, 'SUCCESS')
    into return_code;
  END IF;

  return 1;

  EXCEPTION
  WHEN OTHERS
    THEN
      error_number := SQLSTATE;
      error_message := SQLERRM;
      --Handle errors.
      select cz_error_handler(job_id, function_name, error_number, error_message)
      into return_code;
      --End Proc
      select cz_end_audit(job_id, 'FAIL')
      into return_code;
      RETURN -16;
END;
$$;

