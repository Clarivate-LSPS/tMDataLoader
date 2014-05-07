-- Function: tm_cz.cz_error_handler(numeric, character varying, character varying, character varying)

-- DROP FUNCTION tm_cz.cz_error_handler(numeric, character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION tm_cz.cz_error_handler(jobid numeric, procedurename character varying, errornumber character varying, errormessage character varying)
  RETURNS integer AS
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
	databaseName VARCHAR(100);
	--errorNumber		character varying;	--	PostgreSQL SQLSTATE is alphanumeric
	--errorNumber NUMBER(18,0);
	--errorMessage VARCHAR(1000);
	errorStack VARCHAR(4000);
	errorBackTrace VARCHAR(4000);
	stepNo numeric(18,0);

	rtnCd	integer;

BEGIN
	--Get DB Name
	select database_name INTO databaseName
	from tm_cz.cz_job_master
	where job_id=jobID;

	--Get Latest Step
	select max(step_number) into stepNo from tm_cz.cz_job_audit where job_id = jobID;

	--Get all error info, passed in as parameters, only available from EXCEPTION block
	--errorNumber := SQLSTATE;
	--errorMessage := SQLERRM;

	--	No corresponding functionality in PostgreSQL
	--errorStack := dbms_utility.format_error_stack;
	--errorBackTrace := dbms_utility.format_error_backtrace;

	--Update the audit step for the error
	select tm_cz.cz_write_audit(jobID, databaseName,procedureName, 'Job Failed: See error log for details',1, stepNo, 'FAIL') into rtnCd;

	--write out the error info
	select tm_cz.cz_write_error(jobID, errorNumber, errorMessage, errorStack, errorBackTrace) into rtnCd;

	return 1;

END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  COST 100;
ALTER FUNCTION tm_cz.cz_error_handler(numeric, character varying, character varying, character varying) SET search_path=tm_cz, pg_temp;

ALTER FUNCTION tm_cz.cz_error_handler(numeric, character varying, character varying, character varying)
  OWNER TO postgres;


CREATE OR REPLACE FUNCTION tm_cz.cz_write_audit (jobid numeric, databasename varchar, procedurename varchar, stepdesc varchar, recordsmanipulated numeric, stepnumber numeric, stepstatus varchar)
  RETURNS numeric
AS
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
DECLARE
        lastTime timestamp;
        currTime timestamp;
        elapsedSecs        numeric;
        rtnCd                numeric;

BEGIN

        select max(job_date)
    into lastTime
    from tm_cz.cz_job_audit
    where job_id = jobID;

        --        clock_timestamp() is the current system time

        select clock_timestamp() into currTime;

        elapsedSecs :=        coalesce(((DATE_PART('day', currTime - lastTime) * 24 +
                                   DATE_PART('hour', currTime - lastTime)) * 60 +
                                   DATE_PART('minute', currTime - lastTime)) * 60 +
                                   DATE_PART('second', currTime - lastTime),0);

        begin
        insert         into tm_cz.cz_job_audit
        (job_id
        ,database_name
         ,procedure_name
         ,step_desc
        ,records_manipulated
        ,step_number
        ,step_status
    ,job_date
    ,time_elapsed_secs
        )
        values(
                jobId,
                databaseName,
                procedureName,
                stepDesc,
                recordsManipulated,
                stepNumber,
                stepStatus,
                currTime,
                elapsedSecs
        );
          --raise notice '% / % / % seconds', stepDesc, recordsManipulated, elapsedSecs;
          raise notice '%: % [% / % recs / %s]', lastval(), stepDesc, stepStatus, recordsManipulated, round(elapsedSecs, 3);
        exception
        when OTHERS then
                --raise notice 'proc failed state=%  errm=%', SQLSTATE, SQLERRM;
                select tm_cz.cz_write_error(jobId,0::varchar,SQLSTATE::varchar,SQLERRM::varchar,null::varchar) into rtnCd;
                return -16;
        end;

        return 1;
END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;

CREATE OR REPLACE FUNCTION tm_cz.cz_write_error (jobid numeric, errornumber varchar, errormessage varchar, errorstack varchar, errorbacktrace varchar)
  RETURNS numeric
AS
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

BEGIN

	begin
	insert into tm_cz.cz_job_error(
		job_id,
		error_number,
		error_message,
		error_stack,
		error_backtrace,
		seq_id)
	select
		jobID,
		errorNumber,
		errorMessage,
		errorStack,
		errorBackTrace,
		max(seq_id)
  from tm_cz.cz_job_audit
  where job_id=jobID;

  end;

  return 1;

  exception
	when OTHERS then
		raise notice 'proc failed state=%  errm=%', SQLSTATE, SQLERRM;
		return -16;

END;
$BODY$
LANGUAGE plpgsql VOLATILE SECURITY DEFINER;
