-- -----------------------------------------------
-- show_invalid.sql
-- This tool compiles procedures and functions
-- that are invalid.
-- It resolves errors or displays error reasons
-- when compilation is not enough to resolve them.
-- -----------------------------------------------

SET SERVEROUTPUT ON SIZE UNLIMITED
set linesize 180
set pagesize 0

DECLARE
	CURSOR invalid_cursor is
	SELECT object_name, object_type
	from user_objects
	where status like 'INVALID%'
	  AND object_type  in ('PROCEDURE', 'FUNCTION')
	order by 1;
	
	CURSOR error_cursor is
	SELECT name
	,type
	,line
	,position
	,text
	,message_number
	FROM user_errors;
	
	alter_sql VARCHAR2(1000);
	error_line VARCHAR2(2000);
BEGIN
	for rec in invalid_cursor
	LOOP
		dbms_output.put_line(rec.object_name);
		BEGIN
			alter_sql := 'ALTER ' || rec.object_type || ' '|| rec.object_name || ' compile';
			EXECUTE IMMEDIATE alter_sql;
		EXCEPTION
			WHEN OTHERS THEN
			FOR err in error_cursor
			LOOP
				error_line := err.name || ':' || err.type || ': line ' || err.line || ': position ' || err.position || ': ' || err.text;
				dbms_output.put_line(error_line);
			END LOOP;
			dbms_output.put_line('');
		END;
	END LOOP;
END;
/