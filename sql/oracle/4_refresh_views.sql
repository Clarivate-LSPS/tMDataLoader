EXECUTE DBMS_MVIEW.REFRESH('BIOMART.BIO_MARKER_CORREL_MV');

-- biomart
grant insert, delete, UPDATE, select on biomart.bio_experiment to tm_dataloader;
grant insert, delete, UPDATE, select on biomart.bio_data_uid to tm_dataloader;

--i2b2demodata
GRANT insert, delete, UPDATE, SELECT on i2b2demodata.observation_fact to tm_dataloader;
GRANT insert, delete, UPDATE, SELECT on i2b2demodata.patient_trial to tm_dataloader;

--i2b2metadata
GRANT insert, delete, UPDATE, SELECT on i2b2metadata.i2b2_secure to tm_dataloader;

--searchapp
GRANT insert, delete, UPDATE, SELECT on searchapp.search_secure_object to tm_dataloader;
