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
											'biomart','deapp','searchapp','amapp','fmapp');
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
	  
	  CURSOR missing_seq_synonym_cursor (schemaname IN dba_users.username%type) IS
	  SELECT sequence_name
	  FROM dba_sequences target_schema
	  WHERE sequence_owner = schemaname
	  and not exists
	  (SELECT 1
		FROM dba_synonyms tmd
		WHERE tmd.owner = 'TM_DATALOADER'
		and tmd.synonym_name = target_schema.sequence_name
	  );
	  
	  
	  CURSOR privilege_cursor (schemaname IN dba_tab_privs.owner%type, tab IN dba_tab_privs.table_name%type) IS
	  SELECT privilege
	  FROM dba_tab_privs
	  WHERE owner = upper(schemaname)
	  and table_name = upper(tab);
	
	syn_sql VARCHAR2(1000);
	priv_sql VARCHAR2(1000);
	source VARCHAR2(100) := 'CREATE_SYNONYMS.sql';
		
BEGIN
	dbms_output.put_line(source);
	for i in tm_schemas.FIRST .. tm_schemas.LAST
	loop
		for rec in missing_synonym_cursor(upper(tm_schemas(i)))
		loop
			syn_sql := 'CREATE OR REPLACE SYNONYM TM_DATALOADER.' || rec.table_name || ' for ' || tm_schemas(i) || '.' || rec.table_name;
			dbms_output.put_line(syn_sql);
			EXECUTE IMMEDIATE syn_sql;
			
			-- grant appropriate privileges
			for priv_rec in privilege_cursor(tm_schemas(i),rec.table_name)
			LOOP
				priv_sql := 'GRANT ' || priv_rec.privilege || ' on ' || tm_schemas(i) || '.' || rec.table_name || ' to TM_DATALOADER';
				dbms_output.put_line(priv_sql);
				EXECUTE IMMEDIATE priv_sql; 
			END LOOP;
		end loop;
		
		for rec in missing_seq_synonym_cursor(upper(tm_schemas(i)))
		loop
			syn_sql := 'CREATE OR REPLACE SYNONYM TM_DATALOADER.' || rec.sequence_name || ' for ' || tm_schemas(i) || '.' || rec.sequence_name;
			dbms_output.put_line(syn_sql);
			EXECUTE IMMEDIATE syn_sql;
			
			priv_sql := 'GRANT SELECT ON ' || tm_schemas(i) || '.' || rec.sequence_name || ' to TM_DATALOADER';
			dbms_output.put_line(priv_sql);
			EXECUTE IMMEDIATE priv_sql;			
			-- grant appropriate privileges
		end loop;
		
	end loop;
	EXCEPTION
		WHEN OTHERS THEN
		dbms_output.put_line(source || ':' || SQLERRM);
END;
/
