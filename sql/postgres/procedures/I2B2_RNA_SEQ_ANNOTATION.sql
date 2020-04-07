CREATE OR REPLACE FUNCTION i2b2_rna_seq_annotation(gpl_id character varying, currentjobid int DEFAULT NULL::int)
  RETURNS numeric AS
  $BODY$
DECLARE

	gpl_rtn int;
	newJobFlag numeric(1);
	databaseName character varying(100);
	procedureName character varying(100);
	jobID int;
	errorNumber		character varying;
	errorMessage	character varying;
	rtnCd			integer;
	rowCt			numeric(18,0);
	stepCt int;
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
		INSERT INTO deapp.DE_RNASEQ_ANNOTATION
		(
			TRANSCRIPT_ID
			, GPL_ID
			, GENE_SYMBOL
			, GENE_ID
			, ORGANISM
		)
			SELECT DISTINCT
				(a.transcript_id),
				gpl_id,
				a.gene_symbol,
				NULL,
				a.organism
			FROM LT_RNASEQ_ANNOTATION a
			WHERE
				a.transcript_id NOT IN (SELECT DISTINCT transcript_id
																FROM deapp.DE_RNASEQ_ANNOTATION);
		---update gene_id from bio_marker  table
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

  begin

    update deapp.DE_RNASEQ_ANNOTATION a set GENE_ID=(select primary_external_id from biomart.bio_marker b where
     b.bio_marker_name=a.gene_symbol limit 1)
                                   where a.GENE_ID is null;
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

