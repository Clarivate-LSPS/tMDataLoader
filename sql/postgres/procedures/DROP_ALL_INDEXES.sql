create or replace function drop_all_indexes(schema_name varchar, table_name varchar) returns text
 AS
$BODY$
declare
	drop_sql text;
	recreate_sql text;
begin
	select array_to_string(array(select 'drop index ' || schemaname || '.' || indexname from pg_indexes where schemaname = schema_name and tablename = table_name and indexdef not like '% UNIQUE %'), E';\n') into drop_sql;
	select array_to_string(array(select indexdef from pg_indexes where schemaname=schema_name and tablename=table_name and indexdef not like '% UNIQUE %'), E';\n') into recreate_sql;
	execute(drop_sql);
	return recreate_sql;
end
$BODY$
  LANGUAGE plpgsql VOLATILE SECURITY DEFINER
  SET search_path FROM CURRENT
  COST 100;
