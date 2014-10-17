CREATE OR REPLACE FUNCTION i2b2_delete_partition(
   trialID   varchar(100),
   removed_platform character varying,
   partition_table_name character varying,
   partition_schema character varying,
   sourceCd  character varying,
   currentjobid numeric)

  RETURNS integer
  AS $BODY$
DECLARE
	--Audit variables
	newjobflag		  integer;
	databasename 	  varchar(100);
	procedurename  	varchar(100);
	jobid 			    numeric(18,0);
	stepct 			    numeric(18,0);
	rowct			      numeric(18,0);

	sqlText			  varchar(1000);
	pExists			  numeric;
	partitioniD		numeric(18,0);
  errorNumber		character varying;
	errorMessage	character varying;
	rtnCd			    integer;

BEGIN
  stepCt := 0;
	--Set Audit Parameters
	newJobFlag := 0; -- False (Default)
	jobID := currentJobID;

	databaseName := current_schema();
	procedureName := 'I2B2_DELETE_PARTITION';

	--Audit JOB Initialization
	--If Job ID does not exist, then this is a single procedure run and we need to create it
	IF(jobID IS NULL or jobID < 1)
	THEN
		newJobFlag := 1; -- True
		select cz_start_audit (procedureName, databaseName) into jobId;
	END IF;

	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Start ' || procedureName,0,stepCt,'Done') into rtnCd;

  removed_platform =  coalesce(removed_platform, '');
  partition_table_name = coalesce(partition_table_name, '');
  partition_schema = coalesce(partition_schema, '');
  sourceCd = coalesce(sourceCd, 'STD');

	if removed_platform = '' or partition_table_name = '' or partition_schema = ''
  then
      stepCt := stepCt + 1;
      select cz_write_audit(jobId,databasename,procedurename, 'Empty platform or partition table name or schema',1,stepCt,'ERROR') into rtnCd;
      select cz_error_handler(jobid, procedurename, '-1', 'Application raised error') into rtnCd;
      select cz_end_audit (jobId,'FAIL') into rtnCd;
      return -16;
  end if;


  select partition_id into partitionId
  from deapp.de_subject_sample_mapping
  where trial_name = trialId and platform = removed_platform
  and coalesce(source_cd,'STD') = sourceCd
  group by partition_id limit 1;

  select count(*) into pExists
  from information_schema.tables
  where table_name = partition_table_name ||'_' || partitionId::text;

  if pExists > 0 then
    select count(*) into pExists
    from deapp.de_subject_sample_mapping
    where trial_name <> trialID and partition_id = partitioniD
          and platform = removed_platform;

    if pExists = 0 then
      sqlText := 'drop table '|| partition_schema ||'.' || partition_table_name ||'_' || partitionId::text;
      raise notice 'sqlText= %', sqlText;
      execute sqlText;
      stepCt := stepCt + 1;
      select cz_write_audit(jobId,databaseName,procedureName,
                  'Drop partition '|| partition_schema ||'.' || partition_table_name ||'_' || partitionId::text,1,stepCt,'Done') into rtnCd;
    end if;
  end if;


	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'End ' || procedureName,0,stepCt,'Done') into rtnCD;

	--Cleanup OVERALL JOB if this proc is being run standalone
	IF newJobFlag = 1
	THEN
		select cz_end_audit (jobID, 'SUCCESS') into rtnCd;
	END IF;

	return 1;

	EXCEPTION
	WHEN OTHERS THEN
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		--End Proc
		select cz_end_audit (jobID, 'FAIL') into rtnCd;
		return -16;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
	SET search_path FROM CURRENT
  COST 100;

ALTER FUNCTION i2b2_delete_partition(varchar(100), character varying, character varying, character varying, character varying, numeric)
  OWNER TO postgres;
