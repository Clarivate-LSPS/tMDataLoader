CREATE OR REPLACE
PROCEDURE     "I2B2_MOVE_STUDY"
  ( old_path		varchar2,
    new_path    varchar2,
    currentJobID	NUMBER := null
  )
AS
  --Audit variables
  newJobFlag 	INTEGER(1);
  databaseName 	VARCHAR(100);
  procedureName VARCHAR(100);
  jobID 		number(18,0);
  stepCt 		number(18,0);
  tText			varchar2(2000);


  old_root_node VARCHAR2(700 BYTE);
  new_root_node VARCHAR2(700 BYTE);
  new_root_node_name VARCHAR2(700 BYTE);
  new_path_last_node_name VARCHAR2(700 BYTE);
  rowsExists int;
  counter int;

  old_study_missed exception;
  new_study_exists exception;
  duplicated_paths exception;

BEGIN

--Audit JOB Initialization
stepCt := 0;

--Set Audit Parameters
newJobFlag := 0; -- False (Default)
jobID := currentJobID;

SELECT sys_context('USERENV', 'CURRENT_SCHEMA') INTO databaseName FROM dual;
procedureName := $$PLSQL_UNIT;

--If Job ID does not exist, then this is a single procedure run and we need to create it
IF(jobID IS NULL or jobID < 1)
THEN
  newJobFlag := 1; -- True
  cz_start_audit (procedureName, databaseName, jobID);
END IF;

stepCt := 0;

stepCt := stepCt + 1;
tText := 'Start i2b2_move_study from ' || old_path || ' to ' || new_path;
cz_write_audit(jobId,databaseName,procedureName,tText,0,stepCt,'Done');

--old_path='\Test Studies\ClinicalSample\';
--new_path='\Test Studies 2\ClinicalSample\';
        --\Test Studies\ClinicalSample2\
        --\Test Studies2\ClinicalSample2\
        --\Test Studies2\ClinicalSample2\NewNode\ !!
old_root_node:=REGEXP_REPLACE(old_path,'(\\(\w|\s)*\\)(.*)', '\1'); --\Test Studies\
new_root_node:=REGEXP_REPLACE(new_path,'(\\(\w|\s)*\\)(.*)', '\1'); -- '\Test Studies 2\';
new_root_node_name:=REGEXP_REPLACE(new_path,'\\((\w|\s)*)\\(.*)', '\1'); --Test Studies 2
new_path_last_node_name:=REGEXP_REPLACE(new_path,'(.*)\\((\w|\s)*)\\', '\2'); -- ClinicalSample

if old_path=new_path then
  raise duplicated_paths;
end if;
-- check old root node exists
select count(*) into rowsExists from i2b2metadata.i2b2 where c_fullname=old_path; --1
if rowsExists=0 then
  raise old_study_missed;
end if;

select count(*) into rowsExists from i2b2metadata.i2b2 where c_fullname=new_path; --0
if rowsExists<>0 then
  raise new_study_exists;
end if;

-- if 1
-- check new root node exists

--select count(*) from i2b2metadata.i2b2 where c_fullname=REGEXP_REPLACE('\Test Studies\ClinicalSample2\','(\\.*\\)(.*\\)', '\1'); --1
--select count(*) from i2b2metadata.i2b2 where c_fullname=REGEXP_REPLACE('\Test Studies 2\ClinicalSample','(\\.*\\)(.*\\)', '\1'); --0

select count(*) into rowsExists
from i2b2metadata.i2b2
where c_fullname=new_root_node;

if rowsExists=0 then
      -- create new root in table_access,
      insert into i2b2metadata.table_access
        select new_root_node_name as c_table_cd
          ,'i2b2' as c_table_name
          ,'N' as protected_access
          ,0 as c_hlevel
          ,new_root_node as c_fullname
          ,new_root_node_name as c_name
          ,'N' as c_synonym_cd
          ,'CA' as c_visualattributes
          ,null as c_totalnum
          ,null as c_basecode
          ,null as c_metadataxml
          ,'concept_cd' as c_facttablecolumn
          ,'concept_dimension' as c_dimtablename
          ,'concept_path' as c_columnname
          ,'T' as c_columndatatype
          ,'LIKE' as c_operator
          ,new_root_node as c_dimcode
          ,null as c_comment
          ,new_root_node as c_tooltip
          ,sysdate as c_entry_date
          ,null as c_change_date
          ,null as c_status_cd
          ,null as valuetype_cd
        from dual
        where not exists
        (select 1 from i2b2metadata.table_access x
            where x.c_table_cd = new_root_node);

      stepCt := stepCt + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Create new root node in table_access',SQL%ROWCOUNT,stepCt,'Done');
      commit;

      -- create new root in i2b2
      insert into i2b2metadata.i2b2 (
          C_HLEVEL,
          C_FULLNAME,
          UPDATE_DATE,
          C_DIMCODE,
          C_TOOLTIP,
          C_NAME
          )
          values (0,
                  new_root_node,
                  sysdate,
                  new_root_node,
                  new_root_node,
                  new_root_node_name);

      update i2b2metadata.i2b2 SET
          (C_SYNONYM_CD,
          C_VISUALATTRIBUTES,
          C_BASECODE,
          C_FACTTABLECOLUMN,
          C_TABLENAME,
          C_COLUMNNAME,
          C_COLUMNDATATYPE,
          C_OPERATOR,
          C_COMMENT,
          DOWNLOAD_DATE ,
          IMPORT_DATE,
          M_APPLIED_PATH)
          =
          (select
          C_SYNONYM_CD,
          C_VISUALATTRIBUTES,
          C_BASECODE,
          C_FACTTABLECOLUMN,
          C_TABLENAME,
          C_COLUMNNAME,
          C_COLUMNDATATYPE,
          C_OPERATOR,
          C_COMMENT,
          DOWNLOAD_DATE,
          IMPORT_DATE,
          M_APPLIED_PATH from i2b2metadata.i2b2
          where C_FULLNAME = old_root_node)
      where C_FULLNAME = new_root_node;

      stepCt := stepCt + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Create new root node in i2b2',SQL%ROWCOUNT,stepCt,'Done');
      commit;

      -- create new root in i2b2_secure
      insert into i2b2metadata.i2b2_secure (
        C_HLEVEL,
        C_FULLNAME,
        UPDATE_DATE,
        C_DIMCODE,
        C_TOOLTIP,
        C_NAME
      )
        values (0,
                new_root_node,
                sysdate,
                new_root_node,
                new_root_node,
                new_root_node_name);

      update i2b2metadata.i2b2_secure SET
        (C_SYNONYM_CD,
         C_VISUALATTRIBUTES,
         C_BASECODE,
         C_FACTTABLECOLUMN,
         C_TABLENAME,
         C_COLUMNNAME,
         C_COLUMNDATATYPE,
         C_OPERATOR,
         C_COMMENT,
         DOWNLOAD_DATE ,
         IMPORT_DATE,
         M_APPLIED_PATH,
         SECURE_OBJ_TOKEN)
        =
        (select
           C_SYNONYM_CD,
           C_VISUALATTRIBUTES,
           C_BASECODE,
           C_FACTTABLECOLUMN,
           C_TABLENAME,
           C_COLUMNNAME,
           C_COLUMNDATATYPE,
           C_OPERATOR,
           C_COMMENT,
           DOWNLOAD_DATE,
           IMPORT_DATE,
           M_APPLIED_PATH,
           SECURE_OBJ_TOKEN from i2b2metadata.i2b2_secure
         where C_FULLNAME = old_root_node)
      where C_FULLNAME = new_root_node;

      stepCt := stepCt + 1;
      cz_write_audit(jobId,databaseName,procedureName,'Create new root node in i2b2_secure',SQL%ROWCOUNT,stepCt,'Done');
      commit;

      -- TODO Add new root to concept_dimension - concept_cd?

end if;

-- check if old root has another child
select count(c_fullname) into counter from i2b2metadata.i2b2
where c_fullname like old_root_node || '%' and
        c_fullname not in
        (select c_fullname
         from i2b2metadata.i2b2
         where c_fullname like old_path || '%');

if old_root_node<>new_root_node and counter = 1 then
    -- if has not - remove old root node from i2b2, i2b2_secure, table_access
    delete from i2b2metadata.i2b2 where c_fullname=old_root_node;
    delete from i2b2metadata.i2b2_secure where c_fullname=old_root_node;
    delete from i2b2metadata.table_access where c_fullname=old_root_node;

    stepCt := stepCt + 1;
    cz_write_audit(jobId,databaseName,procedureName,'Remove old root node from i2b2, i2b2_secure, table_access',SQL%ROWCOUNT,stepCt,'Done');
    commit;
end if;

-- rename paths in i2b2 and i2b2_secure
update i2b2metadata.i2b2 set c_fullname=replace(c_fullname, old_path, new_path),
  c_dimcode=replace(c_dimcode, old_path, new_path),
  c_tooltip=replace(c_tooltip, old_path, new_path)
  where c_fullname like old_path || '%';

update i2b2metadata.i2b2_secure set c_fullname=replace(c_fullname, old_path, new_path),
  c_dimcode=replace(c_dimcode, old_path, new_path),
  c_tooltip=replace(c_tooltip, old_path, new_path)
  where c_fullname like old_path || '%';

stepCt := stepCt + 1;
cz_write_audit(jobId,databaseName,procedureName,'Rename paths in i2b2 and i2b2_secure',SQL%ROWCOUNT,stepCt,'Done');
commit;


-- rename c_name in i2b2 and i2b2_secure
update i2b2metadata.i2b2 set c_name=new_path_last_node_name where c_fullname=new_path;
update i2b2metadata.i2b2_secure set c_name=new_path_last_node_name where c_fullname=new_path;

stepCt := stepCt + 1;
cz_write_audit(jobId,databaseName,procedureName,'Update c_name in i2b2 and i2b2_secure',SQL%ROWCOUNT,stepCt,'Done');
commit;

--rename paths in concept_dimension
update i2b2demodata.concept_dimension
set concept_path = replace(concept_path, old_path, new_path)
where concept_path like old_path || '%';
stepCt := stepCt + 1;
cz_write_audit(jobId,databaseName,procedureName,'Rename paths in concept_dimension',SQL%ROWCOUNT,stepCt,'Done');
commit;

-- rename old_root_node in concept_counts
update i2b2demodata.concept_counts
 set parent_concept_path=new_root_node
 where concept_path=old_path;
stepCt := stepCt + 1;
cz_write_audit(jobId,databaseName,procedureName,'Update parent_concept_path in concept_counts',SQL%ROWCOUNT,stepCt,'Done');
commit;

-- update concept_counts
update i2b2demodata.concept_counts set concept_path=replace(concept_path, old_path, new_path),
  parent_concept_path=replace(parent_concept_path, old_path, new_path)
  where concept_path like old_path || '%';
stepCt := stepCt + 1;
cz_write_audit(jobId,databaseName,procedureName,'Rename paths in concept_counts',SQL%ROWCOUNT,stepCt,'Done');
commit;

--rename paths in concept_dimension
update concept_dimension
set CONCEPT_PATH = replace(concept_path, old_path, new_path)
where concept_path like old_path || '%';
stepCt := stepCt + 1;
cz_write_audit(jobId,databaseName,procedureName,'Rename paths in concept_dimension',SQL%ROWCOUNT,stepCt,'Done');
commit;


-- if additional level added
/*--update level data in i2b2
update i2b2metadata.i2b2
SET C_HLEVEL = (length(C_FULLNAME) - nvl(length(replace(C_FULLNAME, '\')),0)) / length('\') - 3
where c_fullname like new_path || '%';
commit;*/


EXCEPTION
    WHEN old_study_missed then
    cz_write_audit(jobId,databasename,procedurename,'Please select exists study path to move',1,stepCt,'ERROR');
    cz_error_handler(jobid,procedurename);
    cz_end_audit (jobId,'FAIL');
    DBMS_OUTPUT.PUT_LINE('old_study_missed');

    WHEN new_study_exists then
    cz_write_audit(jobId,databasename,procedurename,'Selected path to move already exists in db',1,stepCt,'ERROR');
    cz_error_handler(jobid,procedurename);
    cz_end_audit (jobId,'FAIL');
    DBMS_OUTPUT.PUT_LINE('new_study_exists');

    WHEN duplicated_paths then
    cz_write_audit(jobId,databasename,procedurename,'Please select different old and new paths',1,stepCt,'ERROR');
    cz_error_handler(jobid,procedurename);
    cz_end_audit (jobId,'FAIL');
    DBMS_OUTPUT.PUT_LINE('duplicated_paths');

    WHEN OTHERS THEN
    --Handle errors.
    cz_error_handler (jobID, procedureName);
    --End Proc
    cz_end_audit (jobID, 'FAIL');
END;