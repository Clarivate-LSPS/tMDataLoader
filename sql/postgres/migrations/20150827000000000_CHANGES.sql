-- Apply different small database changes.
alter table I2B2METADATA.I2B2 add constraint i2b2_uk unique(C_FULLNAME);

