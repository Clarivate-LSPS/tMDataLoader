CREATE OR REPLACE FUNCTION i2b2_rna_seq_annotation(currentjobid numeric DEFAULT NULL::numeric)
  RETURNS numeric AS
  $BODY$
DECLARE

	gpl_rtn bigint;
	newJobFlag numeric(1);
	databaseName character varying(100);
	procedureName character varying(100);
	jobID bigint;
	errorNumber		character varying;
	errorMessage	character varying;
	rtnCd			integer;
	rowCt			numeric(18,0);
	stepCt bigint;
BEGIN

	stepCt := 0;

	--Set Audit Parameters
	newJobFlag := 0; -- False (Default)
	jobID := currentJobID;

	databaseName := current_schema();
	procedureName := 'I2B2_RNA_SEQ_ANNOTATION';

	--Audit JOB Initialization
	--If Job ID does not exist, then this is a single procedure run and we need to create it
	IF(coalesce(jobID::text, '') = '' or jobID < 1)
	THEN
		newJobFlag := 1; -- True
		select cz_start_audit (procedureName, databaseName, jobID) into jobId;
	END IF;

	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Starting i2b2_rna_seq_annotation',0,stepCt,'Done') into rtnCd;

	select count(platform) into gpl_rtn from deapp.de_gpl_info where marker_type='RNASEQ' and (platform IS NOT NULL AND platform::text <> '');
	if gpl_rtn=0 then
		select cz_write_audit(jobId,databasename,procedurename,'Platform data missing from DEAPP.DE_GPL_INFO',1,stepCt,'ERROR') into rtnCd;
		select cz_error_handler (jobID, procedureName, errorNumber, errorMessage) into rtnCd;
		select cz_end_audit (jobId,'FAIL') into rtnCd;
		return 161;
	end if;

	begin
		insert into deapp.DE_RNASEQ_ANNOTATION
		(
			GPL_ID
			,TRANSCRIPT_ID
			,GENE_SYMBOL
			,GENE_ID
			,ORGANISM
			,PROBESET_ID
		)
			select g.platform
				,a.transcript_id
				,a.gene_symbol
				,b.bio_marker_id
				,a.organism
				,pd.probeset_id
			from LT_RNASEQ_ANNOTATION a
				,(select platform from deapp.de_gpl_info where marker_type='RNASEQ') as g
				,biomart.bio_marker b
				,probeset_deapp pd
			where b.bio_marker_name=a.gene_symbol
						and a.transcript_id =pd.probeset;
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
	select cz_write_audit(jobId,databaseName,procedureName,'Insert new probesets into antigen_deapp',rowCt,stepCt,'Done') into rtnCd;

	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'End i2b2_rna_seq_annotation',0,stepCt,'Done') into rtnCd;

       ---Cleanup OVERALL JOB if this proc is being run standalone
  IF newJobFlag = 1
  THEN
    select cz_end_audit (jobID, 'SUCCESS') into rtnCd;
  END IF;

  return 0;

END;

$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER
SET SEARCH_PATH FROM CURRENT
COST 100;

ALTER FUNCTION i2b2_rna_seq_annotation(NUMERIC)
OWNER TO postgres;
