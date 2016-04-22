-- Apply different small database changes.
SET SERVEROUTPUT ON SIZE UNLIMITED
SET LINESIZE 180

DECLARE
	rows INT;
	source VARCHAR2(100) := 'CHANGES.sql';
	alter_sql VARCHAR2(2000);
BEGIN
	dbms_output.put_line(source);

	SELECT COUNT(*)
	INTO rows
	FROM dba_constraints
	WHERE owner = 'I2B2METADATA'
	AND constraint_name = 'I2B2_UK';
	
	IF rows < 1 THEN
		alter_sql := 'alter table I2B2METADATA.I2B2 add constraint i2b2_uk unique(C_FULLNAME)';
		dbms_output.put_line(alter_sql);
		EXECUTE IMMEDIATE alter_sql;
	END IF;
	
	EXCEPTION
		WHEN OTHERS THEN
		dbms_output.put_line(source || ':' || SQLERRM);
	
END;
/
	
