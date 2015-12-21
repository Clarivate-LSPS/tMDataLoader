/*
Utility function for adding nodes to trial (including all missing intermediate nodes)
 */
CREATE OR REPLACE FUNCTION I2B2_ADD_TRIAL_NODES(trialId CHARACTER VARYING, trialPath CHARACTER VARYING, paths TEXT[], currentjobid NUMERIC)
	RETURNS NUMERIC
SET search_path FROM CURRENT
AS $BODY$
DECLARE
	-- audit variables
	rtnCd			    INTEGER;
	newJobFlag		INTEGER;
	databaseName 	VARCHAR(100);
	procedureName VARCHAR(100);
	jobID 			  NUMERIC(18,0);
	errorNumber		CHARACTER VARYING;
	errorMessage	CHARACTER VARYING;
	stepCt 			  NUMERIC(18,0);
	rowCt			    NUMERIC(18,0);
	rootPath			CHARACTER VARYING(2000);
BEGIN
	databaseName := current_schema();
	procedureName := 'I2B2_ADD_TRIAL_NODES';

	stepCt := 0;
	jobID := currentjobid;

	stepCt := stepCt + 1;
	PERFORM cz_write_audit(jobId, databaseName, procedureName, 'Starting ' || procedureName, 0, stepCt, 'Done');

	-- Add trial node if not exists
	if not exists(select 1 from i2b2 where c_fullname = trialPath) then
		rootPath := regexp_replace(trialPath, '^(\\[^\\]+\\).*', '\1');
		if not exists(
				select 1
				from i2b2metadata.i2b2 i2, i2b2metadata.table_access ta
				where i2.c_fullname = rootPath and ta.c_fullname = rootPath
		) then
			select i2b2_add_root_node(replace(rootPath, '\', ''), jobId) into rtnCd;
			if rtnCd < 0 then
				RETURN rtnCd;
			END IF;
			stepCt := stepCt + 1;
			PERFORM cz_write_audit(jobId, databaseName, procedureName, 'Root node added: ' || rootPath, 0, stepCt, 'Done');
		end if;

		select i2b2_add_node(trialId, trialPath, i2b2_get_node_name(trialPath), jobID) into rtnCd;
		if rtnCd < 0 then
			RETURN rtnCd;
		END IF;
		stepCt := stepCt + 1;
		PERFORM cz_write_audit(jobId, databaseName, procedureName, 'Top node added: ' || trialPath, 0, stepCt, 'Done');
		-- Check number of slashes in a path minus 1 (leading or trailing). Each slash corresponds to path entry.
		-- If this number greater than 2, then we have nodes between root and study node and should fill them
		if length(regexp_replace(trialPath, '[^\\]+', '', 'g')) - 1 > 2 then
			select i2b2_fill_in_tree(null, trialPath, jobID) into rtnCd;
			if rtnCd < 0 then
				RETURN rtnCd;
			end if;
			stepCt := stepCt + 1;
			PERFORM cz_write_audit(jobId, databaseName, procedureName, 'Fill missing parent nodes for top node', 0, stepCt, 'Done');
		end if;
	end if;

	IF (array_length(paths, 1) > 0) THEN
		select i2b2_add_nodes(trialId, paths, jobID, false) into rtnCd;
		if rtnCd >= 0 then
			select i2b2_fill_in_tree(trialId, trialPath, jobID) INTO rtnCd;
		end if;
		if rtnCd < 0 then
			RETURN rtnCd;
		END IF;
		stepCt := stepCt + 1;
		PERFORM cz_write_audit(jobId, databaseName, procedureName, 'Trial nodes added: ' || array_length(paths, 1), 0, stepCt, 'Done');
	ELSE
		stepCt := stepCt + 1;
		PERFORM cz_write_audit(jobId, databaseName, procedureName, 'No nodes was added', 0, stepCt, 'Done');
	END IF;

	BEGIN
		UPDATE i2b2metadata.i2b2 a
		SET c_visualattributes = 'FAS'
		WHERE a.c_fullname = trialPath;
	EXCEPTION
	WHEN OTHERS THEN
		errorNumber := SQLSTATE;
		errorMessage := SQLERRM;
		PERFORM cz_error_handler(jobID, procedureName, errorNumber, errorMessage);
		PERFORM cz_end_audit(jobID, 'FAIL');
		RETURN -16;
	END;

	stepCt := stepCt + 1;
	GET DIAGNOSTICS rowCt := ROW_COUNT;
	PERFORM cz_write_audit(jobId, databaseName, procedureName, 'Update visual attributes for study nodes in I2B2METADATA i2b2',
									 rowCt, stepCt, 'Done');

	stepCt := stepCt + 1;
	PERFORM cz_write_audit(jobId, databaseName, procedureName, 'End ' || procedureName, 0, stepCt, 'Done');

	RETURN 1;

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
