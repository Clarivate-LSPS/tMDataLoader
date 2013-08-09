create or replace 
PROCEDURE "I2B2_CREATE_FULL_TREE" 
(
  path VARCHAR2
 ,currentJobID NUMBER := null
)
AS

BEGIN
	
-- 	execute immediate('truncate table I2B2METADATA.I2B2_LOAD_TREE');

-- 		  INSERT INTO "I2B2METADATA"."I2B2_LOAD_TREE"
-- 			SELECT /*+ parallel(8) */ ROW_NUMBER()  OVER (ORDER BY c_fullname),  C_HLEVEL, NULL, chk.rowid as r FROM i2b2 chk
-- 			WHERE chk.c_fullname like path || '%'
			--REMOVE ITEMS WITHOUT PARENT
-- 			AND (C_HLEVEL = 1 OR EXISTS (SELECT 1 FROM i2b2 par WHERE par.c_fullname = (SUBSTR(chk.c_fullname, 1, INSTR(chk.c_fullname, '\', -2)))))
-- 			;
		  
-- 	execute immediate('analyze table I2B2METADATA.I2B2_LOAD_TREE compute statistics');


-- 		UPDATE /*+ parallel(4) */ I2B2METADATA.I2B2_LOAD_TREE t
-- 		SET t.lvl_last_id = (
-- 				WITH LastLVL as (
-- 				  SELECT tt.ID, (SELECT MIN(e.ID) FROM I2B2METADATA.I2B2_LOAD_TREE e
-- 				  WHERE e.LVL <= tt.LVL AND e.id > tt.ID) v FROM I2B2METADATA.I2B2_LOAD_TREE tt        
-- 				)          
-- 			  SELECT v FROM LastLVL WHERE LastLVL.ID = t.ID
-- 			) - 1;      
			
-- 		commit;
		
-- 		  UPDATE /*+ parallel(4) */ I2B2METADATA.I2B2_LOAD_TREE t
-- 		  SET t.lvl_last_id = (
-- 			  SELECT MAX(e.ID) FROM I2B2METADATA.I2B2_LOAD_TREE e
-- 			)
-- 		  WHERE t.lvl_last_id IS NULL;
		  
-- 	execute immediate('analyze table I2B2METADATA.I2B2_LOAD_TREE compute statistics');

-- commit; 

-- 	execute immediate('truncate table I2B2METADATA.I2B2_LOAD_TREE_FULL');

-- 	  INSERT INTO "I2B2METADATA"."I2B2_LOAD_TREE_FULL"
-- 	  select /*+ parallel(8) */ treefa.RECORD_ID, treela.RECORD_ID
-- 		from "I2B2METADATA"."I2B2_LOAD_TREE" treefa
-- 	  INNER JOIN "I2B2METADATA"."I2B2_LOAD_TREE" treela ON treela.id BETWEEN treefa.id AND treefa.lvl_last_id;
	  
-- 	execute immediate('analyze table I2B2METADATA.I2B2_LOAD_TREE_FULL compute statistics');
	
 
 -- The slow way of loading tree into I2B2_LOAD_TREE_FULL
 
	execute immediate('truncate table TM_WZ.I2B2_LOAD_PATH');
	INSERT INTO "TM_WZ"."I2B2_LOAD_PATH"(PATH, RECORD_ID)
	SELECT  SUBSTR(p.c_fullname, LENGTH(path), LENGTH(p.c_fullname) - LENGTH(path) + 1), p.rowid
	from i2b2 p 
	where p.c_fullname like path || '%';
	
	commit; 
	
	execute immediate('analyze table TM_WZ.I2B2_LOAD_PATH compute statistics');
	execute immediate('truncate table TM_WZ.I2B2_LOAD_TREE_FULL');

	INSERT INTO "TM_WZ"."I2B2_LOAD_TREE_FULL" 
	SELECT /*+ parallel(8) */ p.RECORD_ID, c.RECORD_ID
	from "TM_WZ"."I2B2_LOAD_PATH" p ,"TM_WZ"."I2B2_LOAD_PATH" c
	where c.PATH like p.PATH || '%';
	  
	commit; 
    
	execute immediate('analyze table TM_WZ.I2B2_LOAD_TREE_FULL compute statistics');

 

END;