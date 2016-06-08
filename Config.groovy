// sample configuration file
// replace DB parameters and dataDir with the actual values

db.hostname = 'localhost'
db.port = 1521 // change this to 5432 for PostgreSQL version
db.sid = 'xe'
db.username = 'tm_dataloader'
db.password = 'tm_dataloader'

// Comment or delete the following two lines for PostgreSQL:

db.jdbcConnectionString = "jdbc:oracle:thin:@//${db.hostname}:${db.port}/${db.sid}"
db.jdbcDriver = 'oracle.jdbc.driver.OracleDriver'

// Uncomment the following 3 lines for PostgreSQL:

//db.jdbcConnectionString = "jdbc:postgresql://${db.hostname}:${db.port}/transmart"
//db.jdbcDriver = 'org.postgresql.Driver'
//db.sql.storedProcedureSyntax = 'PostgreSQL'

// The following specifies a directory containing studies
// It should have the proper data structure, for instance:
// YOUR_ETL_DIRECTORY
//	Public Studies
//		Multiple Sclerosis_Baranzini_GSE13732
//			ClinicalData
//			ExpressionData
//		Multiple Sclerosis_Goertsches_GSE24427
//			ClinicalDataToUpload
//			ExpressionDataToUpload
// As of 0.8 and higher, folders can be nested, e.g: \Public Studies\Test\Multiple Sclerosis_Baranzini_GSE13732

dataDir = '/home/transmart/YOUR_ETL_DIRECTORY'

// Do not rename if failed (-n option)
// isNoRenameOnFail = true

// Override default ETL behavior and put VISIT NAME prior to data value (--visit-name-first option)
// If you set this, you may still use --data-value-first to override
// visitNameFirst = true
