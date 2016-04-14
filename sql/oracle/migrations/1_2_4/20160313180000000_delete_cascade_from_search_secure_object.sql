-- -------------------------------------------
-- Re-create constraints referencing SEARCHAPP.SEARCH_SECURE_OBJECT
-- with "ON DELETE CASCADE" option.
-- -------------------------------------------
declare
  l_cons varchar2(4000);
  l_col varchar2(4000);
  l_owner varchar2(4000);
  l_sql varchar2(4000);
  l_table varchar2(4000);
begin
  for i in (
    select owner, constraint_name, table_name
      from dba_constraints
     where     r_owner= 'SEARCHAPP'
           and constraint_type= 'R'
           and delete_rule= 'NO ACTION'
           and r_constraint_name in (
                   select constraint_name
                     from dba_constraints
                    where     constraint_type= 'P'
                          and owner= 'SEARCHAPP'
                          and table_name= 'SEARCH_SECURE_OBJECT'
               )
  ) loop
    l_cons := i.constraint_name;
    l_col := null;
    l_owner := i.owner;
    l_table := i.table_name;
    for j in (
      select column_name
        from dba_cons_columns
       where     owner= l_owner
             and constraint_name= l_cons
             and table_name= l_table
    )
    loop
      if l_col is not null then
        raise TOO_MANY_ROWS;
      end if;
      l_col := j.column_name;
    end loop;
    if l_col is null then
      raise NO_DATA_FOUND;
    end if;
    l_sql := 'alter table '||l_owner||'.'||l_table||' drop constraint '||l_cons;
    dbms_output.put_line(l_sql);
    execute immediate l_sql;
    l_sql := 'alter table '||l_owner||'.'||l_table||' add constraint '||l_cons||' foreign key('||l_col||') references SEARCHAPP.SEARCH_SECURE_OBJECT on delete cascade';
    dbms_output.put_line(l_sql);
    execute immediate l_sql;
  end loop;
exception
  when others then
    dbms_output.put_line('Can''t update constraint: '||l_sql);
    raise;
end;
/
