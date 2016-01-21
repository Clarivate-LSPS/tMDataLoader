
CREATE OR REPLACE FUNCTION I2B2_CREATE_FULL_TREE(path character varying, currentjobid numeric DEFAULT (-1))
RETURNS numeric AS
$BODY$
DECLARE
	parent_c CURSOR FOR
	SELECT p.PATH, p.record_id, p.PATH_LEN FROM I2B2_LOAD_PATH p;
	rowCt			numeric(18,0);
	databaseName 	VARCHAR(100);
	procedureName 	VARCHAR(100);
	rtnCd			numeric;
BEGIN

	databaseName := current_schema();
	procedureName := 'I2B2_CREATE_FULL_TREE';


	DELETE FROM I2B2_LOAD_PATH;

	--Remove duplicates
	INSERT INTO I2B2_LOAD_PATH(PATH, RECORD_ID, PATH_LEN)
	SELECT P, MIN(RECORD_ID), LENGTH(P) FROM
	(
		SELECT  SUBSTR(p.c_fullname, LENGTH(path), LENGTH(p.c_fullname) - LENGTH(path) + 1) as P, p.RECORD_ID, 0 as PATH_LEN
		from i2b2metadata.i2b2 p
		where p.c_fullname like path || '%' escape '`'
	) t
	GROUP BY P;

	UPDATE I2B2_LOAD_PATH p SET PATH200 = SUBSTR(p.PATH, 1, 200), PATH150 = SUBSTR(p.PATH, 1, 150), PATH100 = SUBSTR(p.PATH, 1, 100), PATH50 = SUBSTR(p.PATH, 1, 50);

	ANALYZE I2B2_LOAD_PATH;

	get diagnostics rowCt := ROW_COUNT;
	select cz_write_audit(currentjobid, databaseName, procedureName,'Inserted into I2B2_LOAD_PATH',rowCt,0,'Done') into rtnCd;

	--execute immediate('analyze table I2B2_LOAD_PATH compute statistics');
	--execute immediate('truncate table I2B2_LOAD_TREE_FULL');

	DROP INDEX IF EXISTS TM_WZ_IDX_ROOT;
	DROP INDEX IF EXISTS TM_WZ_IDX_CHILD;
	DELETE FROM I2B2_LOAD_TREE_FULL;

	/*FOR p IN parent_c LOOP
		BEGIN
			INSERT INTO I2B2_LOAD_TREE_FULL(IDROOT, IDCHILD)
			SELECT p.RECORD_ID, c.RECORD_ID
			from I2B2_LOAD_PATH c
			where c.PATH_LEN >= p.PATH_LEN AND c.PATH like p.PATH || '%' escape '`';
		END;
	END LOOP;*/

	--PROCESS 200 < LENS
	INSERT INTO I2B2_LOAD_TREE_FULL(IDROOT, IDCHILD)
	SELECT p.RECORD_ID, c.RECORD_ID
	from I2B2_LOAD_PATH c, I2B2_LOAD_PATH p
	where p.PATH_LEN > 200 AND c.PATH200 = p.PATH200 AND c.PATH_LEN >= p.PATH_LEN AND c.PATH like p.PATH || '%' escape '`';

	--PROCESS 200 >= LENS > 150
	INSERT INTO I2B2_LOAD_TREE_FULL(IDROOT, IDCHILD)
	SELECT p.RECORD_ID, c.RECORD_ID
	from I2B2_LOAD_PATH c, I2B2_LOAD_PATH p
	where p.PATH_LEN > 150 AND p.PATH_LEN <= 200 AND c.PATH150 = p.PATH150 AND c.PATH_LEN >= p.PATH_LEN AND c.PATH like p.PATH || '%' escape '`';


	--PROCESS 150 >= LENS > 100
	INSERT INTO I2B2_LOAD_TREE_FULL(IDROOT, IDCHILD)
	SELECT p.RECORD_ID, c.RECORD_ID
	from I2B2_LOAD_PATH c, I2B2_LOAD_PATH p
	where p.PATH_LEN > 100 AND p.PATH_LEN <= 150 AND c.PATH100 = p.PATH100 AND c.PATH_LEN >= p.PATH_LEN AND c.PATH like p.PATH || '%' escape '`';

	--PROCESS 100 >= LENS > 50
	INSERT INTO I2B2_LOAD_TREE_FULL(IDROOT, IDCHILD)
	SELECT p.RECORD_ID, c.RECORD_ID
	from I2B2_LOAD_PATH c, I2B2_LOAD_PATH p
	where p.PATH_LEN > 50  AND p.PATH_LEN <= 100 AND c.PATH50 = p.PATH50 AND c.PATH_LEN >= p.PATH_LEN AND c.PATH like p.PATH || '%' escape '`';

	--PROCESS LENS < 50
	INSERT INTO I2B2_LOAD_TREE_FULL(IDROOT, IDCHILD)
	SELECT p.RECORD_ID, c.RECORD_ID
	from I2B2_LOAD_PATH c, I2B2_LOAD_PATH p
	where p.PATH_LEN <= 50 AND c.PATH_LEN >= p.PATH_LEN AND c.PATH like p.PATH || '%' escape '`';

	SELECT COUNT(*) FROM I2B2_LOAD_TREE_FULL INTO rowCt;
	--get diagnostics rowCt := ROW_COUNT;
	select cz_write_audit(currentjobid, databaseName, procedureName,'Inserted into I2B2_LOAD_TREE_FULL',rowCt,0,'Done') into rtnCd;

	CREATE INDEX TM_WZ_IDX_ROOT ON I2B2_LOAD_TREE_FULL (IDROOT, IDCHILD);
	CREATE INDEX TM_WZ_IDX_CHILD ON I2B2_LOAD_TREE_FULL (IDCHILD, IDROOT);

	--VERY SLOW IN POSTGRESQL
	/*INSERT INTO I2B2_LOAD_TREE_FULL
	SELECT p.RECORD_ID, c.RECORD_ID
	from I2B2_LOAD_PATH p ,I2B2_LOAD_PATH c
	where c.PATH like p.PATH || '%' escape '`';*/

	RETURN 0;
END;

$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  SET search_path FROM CURRENT;

ALTER FUNCTION I2B2_CREATE_FULL_TREE(character varying, numeric)
  OWNER TO postgres;