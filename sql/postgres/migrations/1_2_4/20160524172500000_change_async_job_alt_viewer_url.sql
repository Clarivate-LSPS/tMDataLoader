DO $$
begin
  if (select character_maximum_length != 4000
      from INFORMATION_SCHEMA.COLUMNS
      where
        table_schema = 'i2b2demodata' and
        table_name = 'async_job' and
        column_name = 'alt_viewer_url') then
    ALTER TABLE i2b2demodata.async_job ALTER COLUMN alt_viewer_url TYPE character varying(4000);
  end if;
end;
$$