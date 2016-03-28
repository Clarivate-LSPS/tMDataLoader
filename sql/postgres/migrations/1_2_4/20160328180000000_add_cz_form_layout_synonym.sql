DO $$
begin
	if exists(select * from pg_tables where schemaname = 'searchapp' and tablename = 'search_form_layout') then
		if not exists(select * from pg_views where schemaname = 'biomart_user' and viewname = 'cz_form_layout') then
			create view biomart_user.cz_form_layout as select * from searchapp.search_form_layout;
		end if;
	elsif exists(select * from pg_tables where schemaname = 'tm_cz' and tablename = 'cz_form_layout') then
		grant select on tm_cz.cz_form_layout to biomart_user;
	end if;
end;
$$