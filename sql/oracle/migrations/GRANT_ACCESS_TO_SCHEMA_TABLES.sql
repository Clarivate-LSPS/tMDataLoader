-- --------------------------------------------------------------
-- GRANT_ACCESS_TO_SCHEMA_TABLES.sql
-- Give access to TM_DATALOADER SCHEMA
-- from schemas: tm_cz, tm_lz, tm_wz, amapp,
-- i2b2metadata, i2b2demodata, biomart, deapp, searchapp, fmapp
--
-- Also grant select on sys.all tables,
-- and ALTER ANY TABLE, ALTER ANY INDEX.
-- Grant execute on any routines in tm_cz
-- Grant analyze any to tm_dataloader
-- Grant select on sequences in TM schemas.

-- --------------------------------------------------------------
SET SERVEROUTPUT ON 

DECLARE
	TYPE schema_names IS TABLE OF dba_users.username%type ;
	tm_schemas schema_names := schema_names('tm_cz','tm_lz','tm_wz', 'amapp',
											'i2b2metadata','i2b2demodata',
											'biomart','deapp','searchapp','fmapp','gwas_plink');
	CURSOR table_cursor (schemaname IN dba_users.username%type) is
	select table_name
	FROM dba_tables
	WHERE owner = schemaname;
	
	CURSOR proc_cursor is
	SELECT object_name
	FROM dba_procedures
	WHERE owner = 'TM_CZ'
	and object_type in ('PROCEDURE','FUNCTION')
	and object_name not in('I2B2_PROCESS_MRNA_INC_DATA_Old');

	CURSOR seq_cursor (schemaname IN dba_users.username%type) IS
	SELECT sequence_name
	FROM dba_sequences
	WHERE sequence_owner = schemaname;
	
	grant_sql VARCHAR2(1000);
	
BEGIN
	dbms_output.put_line('Executing grants');
	
	grant_sql := 'GRANT ALTER ANY TABLE TO tm_dataloader';
	EXECUTE IMMEDIATE grant_sql;
	
	grant_sql := 'GRANT ANALYZE ANY TO tm_dataloader';
	EXECUTE IMMEDIATE grant_sql;

	for i in tm_schemas.FIRST .. tm_schemas.LAST
	loop
		-- dbms_output.put_line(tm_schemas(i));
		for rec in table_cursor(upper(tm_schemas(i)))
		loop
			-- dbms_output.put_line(tm_schemas(i)|| '.' || rec.table_name);
			grant_sql := 'GRANT SELECT, INSERT, UPDATE, DELETE ON ' || tm_schemas(i) || '.' || rec.table_name || ' TO tm_dataloader';
			-- dbms_output.put_line(grant_sql);
			EXECUTE IMMEDIATE grant_sql;
		end loop;

		for rec in seq_cursor(upper(tm_schemas(i)))
		loop
		    grant_sql := 'GRANT SELECT ON ' || tm_schemas(i) || '.' || rec.sequence_name || ' TO tm_dataloader';
			-- dbms_output.put_line(grant_sql);
			EXECUTE IMMEDIATE grant_sql;
		end loop;
	end loop;
	
	-- grants on procedures
	for rec in proc_cursor
	LOOP
		grant_sql := 'GRANT EXECUTE ON TM_CZ.' || rec.object_name || ' TO TM_DATALOADER';
		-- dbms_output.put_line(grant_sql);
		EXECUTE IMMEDIATE grant_sql;
	END LOOP;
	
	EXCEPTION
	WHEN OTHERS
	THEN
		dbms_output.put_line(sqlerrm);
		raise;
END;
/
