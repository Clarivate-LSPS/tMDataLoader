do $$
declare
  l_cname text; l_csch text; l_tname text; l_tsch text; l_sql text; l_col text;
  i record; j record;
begin
  for i in (
    select rc.constraint_schema, rc.constraint_name, tc.table_schema, tc.table_name
      from      information_schema.referential_constraints rc
           join information_schema.table_constraints tc on tc.constraint_schema= rc.constraint_schema and tc.constraint_name= rc.constraint_name
     where     rc.delete_rule <> 'CASCADE'
           and tc.constraint_type= 'FOREIGN KEY'
           and (rc.unique_constraint_schema, rc.unique_constraint_name) in (
                 select constraint_schema, constraint_name
                   from information_schema.table_constraints
                  where     table_schema= 'searchapp'
                        and table_name= 'search_secure_object'
                        and constraint_type= 'PRIMARY KEY'
               )
  ) loop
    l_col := '';
    l_csch := i.constraint_schema;
    l_cname := i.constraint_name;
    l_tsch := i.table_schema;
    l_tname := i.table_name;
    for j in (
      select column_name
        from information_schema.key_column_usage
       where     constraint_schema= l_csch
             and constraint_name= l_cname
             and table_schema= l_tsch
             and table_name= l_tname
    ) loop
      if length(l_col) > 0 then
        raise exception 'Constraint %.% is not supposed to be a compound key.', l_csch, l_cname;
      end if;
      l_col := j.column_name;
    end loop;
    if length(l_col) = 0 then
      raise exception 'No referenced columns found for constraint %.%.', l_csch, l_cname;
    end if;
    l_sql := 'alter table '||l_tsch||'.'||l_tname||' drop constraint '||l_cname||', add constraint '||l_cname||' foreign key('||l_col||') references searchapp.search_secure_object on delete cascade';
    raise info '%', l_sql;
    execute l_sql;
  end loop;
end $$;
