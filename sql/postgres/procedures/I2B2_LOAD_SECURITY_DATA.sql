-- Function: i2b2_load_security_data(numeric)

-- DROP FUNCTION i2b2_load_security_data(numeric);

CREATE OR REPLACE FUNCTION i2b2_load_security_data(currentjobid numeric DEFAULT (-1))
  RETURNS numeric AS
$BODY$
/*************************************************************************
* Copyright 2008-2012 Janssen Research & Development, LLC.
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
******************************************************************/
Declare

	--Audit variables
	databaseName 	VARCHAR(100);
	procedureName 	VARCHAR(100);
	jobID 			numeric(18,0);
	stepCt 			numeric(18,0);
	rowCt			numeric(18,0);
	errorNumber		character varying;
	errorMessage	character varying;
	rtnCd			numeric;

BEGIN

	--Set Audit Parameters
	databaseName := current_schema();
	procedureName := 'I2B2_LOAD_SECURITY';

	--Audit JOB Initialization
	--If Job ID does not exist, then this is a single procedure run and we need to create it
	select case when coalesce(currentjobid, -1) < 1 then cz_start_audit(procedureName, databaseName) else currentjobid end into jobId;

	truncate table I2B2METADATA.i2b2_secure;

	stepCt := stepCt + 1;
	select cz_write_audit(jobId,databaseName,procedureName,'Truncate I2B2METADATA i2b2_secure',0,stepCt,'Done') into rtnCd;

	insert into I2B2METADATA.i2b2_secure
	(C_HLEVEL,
     C_FULLNAME,
     C_NAME,
     C_SYNONYM_CD,
     C_VISUALATTRIBUTES,
     C_TOTALNUM,
     C_BASECODE,
     C_METADATAXML,
     C_FACTTABLECOLUMN,
     C_TABLENAME,
     C_COLUMNNAME,
     C_COLUMNDATATYPE,
     C_OPERATOR,
     C_DIMCODE,
     C_COMMENT,
     C_TOOLTIP,
     UPDATE_DATE,
     DOWNLOAD_DATE,
     IMPORT_DATE,
     SOURCESYSTEM_CD,
     VALUETYPE_CD,
	 secure_obj_token)
  select
    b.C_HLEVEL,
    b.C_FULLNAME,
    b.C_NAME,
    b.C_SYNONYM_CD,
    b.C_VISUALATTRIBUTES,
    b.C_TOTALNUM,
    b.C_BASECODE,
    b.C_METADATAXML,
    b.C_FACTTABLECOLUMN,
    b.C_TABLENAME,
    b.C_COLUMNNAME,
    b.C_COLUMNDATATYPE,
    b.C_OPERATOR,
    b.C_DIMCODE,
    b.C_COMMENT,
    b.C_TOOLTIP,
    b.UPDATE_DATE,
    b.DOWNLOAD_DATE,
    b.IMPORT_DATE,
    b.SOURCESYSTEM_CD,
    b.VALUETYPE_CD,
	coalesce(f.tval_char,'EXP:PUBLIC')
    from I2B2METADATA.I2B2 b
	left outer join (select distinct modifier_cd, tval_char from i2b2demodata.observation_fact where concept_cd = 'SECURITY') f
		 on b.sourcesystem_cd = f.modifier_cd;
	get diagnostics rowCt := ROW_COUNT;
    stepCt := stepCt + 1;
    select cz_write_audit(jobId,databaseName,procedureName,'Insert security data into I2B2METADATA i2b2_secure',rowCt,stepCt,'Done') into rtnCd;

    ---Cleanup OVERALL JOB if this proc is being run standalone
	perform cz_end_audit (jobID, 'SUCCESS') where coalesce(currentJobId, -1) <> jobId;

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

CREATE OR REPLACE FUNCTION i2b2_load_security_data(sourcesystemCd varchar(50), currentjobid numeric DEFAULT (-1))
  RETURNS numeric AS
$BODY$
/*************************************************************************
* Copyright 2008-2012 Janssen Research & Development, LLC.
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
******************************************************************/
Declare

	--Audit variables
	databaseName 	VARCHAR(100);
	procedureName 	VARCHAR(100);
	jobID 			numeric(18,0);
	stepCt 			numeric(18,0);
	rowCt			numeric(18,0);
	errorNumber		character varying;
	errorMessage	character varying;

BEGIN
	--Set Audit Parameters
	databaseName := current_schema();
	procedureName := 'I2B2_LOAD_SECURITY';

	select case when coalesce(currentjobid, -1) < 1 then cz_start_audit(procedureName, databaseName) else currentjobid end into jobId;

	delete from I2B2METADATA.i2b2_secure where sourcesystem_cd = sourcesystemCd;

  get diagnostics rowCt := ROW_COUNT;
	stepCt := stepCt + 1;
	perform cz_write_audit(jobId,databaseName,procedureName,'Clean-up I2B2METADATA i2b2_secure',rowCt,stepCt,'Done');

	insert into I2B2METADATA.i2b2_secure
	(C_HLEVEL,
     C_FULLNAME,
     C_NAME,
     C_SYNONYM_CD,
     C_VISUALATTRIBUTES,
     C_TOTALNUM,
     C_BASECODE,
     C_METADATAXML,
     C_FACTTABLECOLUMN,
     C_TABLENAME,
     C_COLUMNNAME,
     C_COLUMNDATATYPE,
     C_OPERATOR,
     C_DIMCODE,
     C_COMMENT,
     C_TOOLTIP,
     UPDATE_DATE,
     DOWNLOAD_DATE,
     IMPORT_DATE,
     SOURCESYSTEM_CD,
     VALUETYPE_CD,
	 secure_obj_token)
  select
    b.C_HLEVEL,
    b.C_FULLNAME,
    b.C_NAME,
    b.C_SYNONYM_CD,
    b.C_VISUALATTRIBUTES,
    b.C_TOTALNUM,
    b.C_BASECODE,
    b.C_METADATAXML,
    b.C_FACTTABLECOLUMN,
    b.C_TABLENAME,
    b.C_COLUMNNAME,
    b.C_COLUMNDATATYPE,
    b.C_OPERATOR,
    b.C_DIMCODE,
    b.C_COMMENT,
    b.C_TOOLTIP,
    b.UPDATE_DATE,
    b.DOWNLOAD_DATE,
    b.IMPORT_DATE,
    b.SOURCESYSTEM_CD,
    b.VALUETYPE_CD,
	coalesce(f.tval_char,'EXP:PUBLIC')
    from I2B2METADATA.I2B2 b
	left outer join (select distinct modifier_cd, tval_char from i2b2demodata.observation_fact where concept_cd = 'SECURITY') f
		 on b.sourcesystem_cd = f.modifier_cd
	where b.sourcesystem_cd = sourcesystemCd;

	get diagnostics rowCt := ROW_COUNT;
  stepCt := stepCt + 1;
  perform cz_write_audit(jobId,databaseName,procedureName,'Insert security data into I2B2METADATA i2b2_secure',rowCt,stepCt,'Done');

    ---Cleanup OVERALL JOB if this proc is being run standalone
	perform cz_end_audit (jobID, 'SUCCESS') where coalesce(currentJobId, -1) <> jobId;

	return 1;

	EXCEPTION
	WHEN OTHERS THEN
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		--Handle errors.
		perform cz_error_handler (jobID, procedureName, errorNumber, errorMessage);
		--End Proc
		perform cz_end_audit (jobID, 'FAIL');
		return -16;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  SET search_path FROM CURRENT
  COST 100;
