-- -------------------------------------------
-- Issue 87
-- Set category_cd column to a uniform size (250)
-- Leave category_cd unchanged in tables where the column size is larger
-- -------------------------------------------
SET SERVEROUTPUT ON SIZE 1000000
set linesize 180
set pagesize 0

DECLARE 
	sqltxt VARCHAR2(1000);
BEGIN
	FOR cur_rec IN (SELECT  OWNER,
							TABLE_NAME,
							COLUMN_NAME,
							DATA_LENGTH
					FROM dba_tab_cols
					WHERE COLUMN_NAME = 'CATEGORY_CD'
					  AND data_length < 250
					)
	LOOP BEGIN
		sqltxt := 'ALTER TABLE ' || cur_rec.owner || '.' || cur_rec.table_name || ' MODIFY CATEGORY_CD VARCHAR2(250)';
	    dbms_output.put_line(sqltxt);
		EXECUTE IMMEDIATE sqltxt;
		EXCEPTION
		WHEN OTHERS THEN
			DBMS_OUTPUT.PUT_LINE(cur_rec.owner || '.' || cur_rec.table_name || SQLERRM);
		END;
	END LOOP;
END;
/
