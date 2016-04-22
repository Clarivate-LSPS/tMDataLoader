-- -------------------------------------------
-- FOREIGN_KEYS_DELETE_CASCADE.sql
-- Replace foreign key constraints to 
-- searchapp.search_secure_object with version
-- that has ON DELETE CACADE clause
-- The idea is to delete searchapp.search_secure_object 
-- when a record it references is deleted.
-- -------------------------------------------
SET SERVEROUTPUT ON SIZE UNLIMITED
SET HEADING OFF
SET LINESIZE 180

SELECT 'FOREIGN_KEYS_DELETE_CASCADE.sql' FROM DUAL;

DECLARE
	CURSOR constraint_cursor IS
	SELECT parent_con.owner parent_owner
		,child_con.owner child_owner
		,parent_con.constraint_name parent_constraint_name
		,child_con.constraint_name child_constraint_name
		,parent_con.table_name parent_table_name
		,child_con.table_name child_table_name
	FROM dba_constraints parent_con
		, dba_constraints child_con
	WHERE parent_con.owner = 'SEARCHAPP'
		and parent_con.table_name = upper('search_secure_object')
		and parent_con.constraint_name = child_con.r_constraint_name
		and child_con.r_owner = parent_con.owner
		and child_con.delete_rule = 'NO ACTION';
		
		CURSOR constraint_col_cursor (cons_owner IN VARCHAR2,cons_name IN varchar2) IS
		SELECT column_name
		FROM dba_cons_columns 
		WHERE owner = cons_owner
		and constraint_name = cons_name;
		
		drop_sql VARCHAR2(2000);
		add_sql VARCHAR2(2000);
		source VARCHAR2(100) := 'FOREIGN_KEYS_DELETE_CASCADE.sql';
		parent_cols VARCHAR2(1000);
		ref_cols VARCHAR2(1000);
		first_y CHAR;
		
	TYPE command_type IS TABLE OF varchar2(300);
    commands command_type := command_type()	;
	command VARCHAR2(300);
BEGIN
FOR rec in constraint_cursor
LOOP	
	first_y := 'y';
	FOR prec in constraint_col_cursor(rec.parent_owner,rec.parent_constraint_name)
	LOOP
		IF first_y = 'y' THEN
			parent_cols := prec.column_name;
			first_y := 'n';
		else
			parent_cols := parent_cols || ',' || prec.column_name;
		end if;
		-- dbms_output.put_line(parent_cols);
	END LOOP;
	
	first_y := 'y';
	FOR prec in constraint_col_cursor(rec.child_owner,rec.child_constraint_name)
	LOOP
		IF first_y = 'y' THEN
			ref_cols := prec.column_name;
			first_y := 'n';
		else
			ref_cols := ref_cols || ',' || prec.column_name;
		end if;
		-- dbms_output.put_line(ref_cols);
	END LOOP;
	drop_sql := 'ALTER TABLE ' || rec.child_owner || '.' || rec.child_table_name || ' DROP CONSTRAINT ' ||
				rec.child_constraint_name || ' CASCADE';
	commands.extend;
	commands(commands.count) := drop_sql;
	add_sql := 'ALTER TABLE ' || rec.child_owner || '.' || rec.child_table_name || ' ADD CONSTRAINT ' ||
	rec.child_constraint_name || ' FOREIGN KEY (' || ref_cols || ') references ' || rec.parent_table_name || ' (' || parent_cols || ') on delete cascade';
	commands.extend;
	commands(commands.count) := add_sql;
END LOOP;

-- commands are created inside of cursor loop but executed outside to prevent table mutex problems
IF commands.count > 0 THEN
	FOR i in 1 .. commands.count
	LOOP
		dbms_output.put_line(commands(i));
		EXECUTE IMMEDIATE commands(i);
	END LOOP;
END IF;

EXCEPTION
WHEN OTHERS THEN
	dbms_output.put_line(source || ':' || SQLERRM);

END;
/






