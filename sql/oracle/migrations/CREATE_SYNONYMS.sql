-- -------------------------------
-- CREATE_SYNONYMS.sql
-- -------------------------------
set serveroutput on size unlimited
set linesize 180
set head off

DECLARE
	TYPE names IS TABLE OF dba_users.username%type ;
	tm_schemas names := names('tm_cz','tm_lz','tm_wz',
											'i2b2metadata','i2b2demodata',
											'biomart','deapp','searchapp','fmapp');
	-- find tables in the target schema that are not in TM_DATALOADER										
	CURSOR missing_synonym_cursor (schemaname IN dba_users.username%type) IS
	SELECT table_name
	FROM dba_tables target_schema
	WHERE owner = schemaname
	  and  NOT EXISTS 
	  (SELECT 1
	   FROM dba_tables tmd
   	   WHERE tmd.owner = 'TM_DATALOADER'
   		AND  tmd.table_name = target_schema.table_name
	  );
	
	syn_sql VARCHAR2(1000);
	source VARCHAR2(100) := 'CREATE_SYNONYMS.sql';
		
BEGIN
	dbms_output.put_line('Creating synonyms');
	for i in tm_schemas.FIRST .. tm_schemas.LAST
	loop
		for rec in missing_synonym_cursor(upper(tm_schemas(i)))
		loop
			syn_sql := 'CREATE OR REPLACE SYNONYM TM_DATALOADER.' || rec.table_name || ' for ' || tm_schemas(i) || '.' || rec.table_name;
			dbms_output.put_line(syn_sql);
			EXECUTE IMMEDIATE syn_sql;
		end loop;
	end loop;
	EXCEPTION
		WHEN OTHERS THEN
		dbms_output.put_line(source || ':' || SQLERRM);
END;
/
