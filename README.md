tMDataLoader
============

tm_etl.jar - Transmart ETL tool
Contributed by Thomson Reuters

src folder contains all sources
sample_data folder contains sample public datasets from GEO


INSTALLING
==========

Just copy tm_etl.jar to any directory on the server. It can be on the same machine as Transmart or any other one that has direct access to TM_CZ database schema used by tranSMART.

Then, edit Config.groovy file and put it in your ~/.tm_etl directory.
Please make sure you edit the configuration file before using the tool.

For Oracle
==========

Launch SQLDeveloper and execute the code of each *.sql file in sql-oracle directory, starting with 1_run_first.sql.
This will apply necessary fixes to the database without breaking compatibility with other tools.

For PostgreSQL
==============

First of all, you need to apply fixes to the schema and stored procedures, as they are broken in 1.1 release.
Please, clone transmartApp-DB repository:

	git clone https://github.com/eugene-rakhmatulin/transmartApp-DB.git
	git checkout post_GPL1.1.0_fixes
	
Then apply the fixes to your PostgreSQL database:
	
	cd transmartApp-DB/postgresql_wGEO
	psql -d transmart -f post_1.1.0_update.sql
	
If you want "realtime" log updates in ETL tool, you also need:

1) Make sure dblink extension is installed with PostgreSQL (normally comes with standard 'contrib' package)
2) Edit etl/functions/CZ_WRITE_AUDIT_withdblink.sql and specify login/password if ETL tool is not connecting to DB as a superuser
3) Apply the fix:
	
	psql -d transmart -f etl/functions/CZ_WRITE_AUDIT_withdblink.sql
	

PREPARING DATA FOR UPLOAD
=========================

First, you need to put studies you want to upload in the corresponding directory on the machine, that you specified in the configuration file.
It should have the following structure (below is just an example):

NEW! Starting with version 0.8, nested folders are supported - see below.

	YOUR_ETL_DIRECTORY
		Public Studies <-- put public studies here
			Multiple Sclerosis_Baranzini_GSE13732
				ClinicalData
				ExpressionData
				MetaData
				SNPData
			Multiple Sclerosis_Goertsches_GSE24427
				ClinicalDataToUpload <-- both ClinicalData and ClinicalDataToUpload are fine
				ExpressionDataToUpload <-- same for this one
				MetaDataToUpload <-- same for this one
				SNPDataToUpload
		Internal Studies <-- put internal studies here
			MyStudyFolder
				MyStudy
					ClinicalData
					ExpressionData
					MetaData
					SNPData
		_MetaData <-- you can put metadata here if it contains metadata for several studies. Each .txt file will be processed.
			
Basically, the first level of the directory defines the top category in the Dataset Explorer tree.
The last level (before Clinical/Expression folders) defines the study name that will be used in the tree. All folders in between define intermediate folders in the Dataset Explorer tree.

Please, refer to the data format description for further information.

USING TOOL
==========

To start ETL process, just run the following command:

    java -jar tm_etl.jar

You can run it with "-h" option to get a list of all available options:

    $ java -jar tm_etl.jar -h
    usage: tm_etl [options] [<data_dir>]
        --alt-clinical-proc <proc_name>   Name of alternative clinical stored
                                          procedure (expert option)
     -c,--config <config>                 Configuration filename
        --data-value-first                Put VISIT NAME after the data value
                                          (default behavior, use to override
                                          non-standard config)
     -h,--help                            Show usage information
     -i,--interactive                     Interactive (console) mode: progress
                                          bar
     -n,--no-rename                       Don't rename folders when failed
     -s,--stop-on-fail                    Stop when upload is failed
        --secure-study                    Make study securable
     -t,--use-t                           Do not use Z datatype for T
                                          expression data (expert option)
     -v,--version                         Display version information and exit
        --visit-name-first                Put VISIT_NAME before the data value

By default, the configuration file location is ~/.tm_etl/Config.groovy.
You can specify the configuration file name using -c option.

If you don't redirect output to the file, you can find -i option useful - it displays progress for some long operations.

IMPORTANT! If your machine doesn't have a direct connection to the internet and requires http proxy, make sure you specify it when running the tool:

    $ java -Dhttp.proxyHost=webproxy.host.com -Dhttp.proxyPort=80 -jar ./tm_etl.jar

Refer to JVM documentation for more information on these options.

IMPORTANT! If you are using Oracle db you should download driver (http://www.oracle.com/technetwork/database/enterprise-edition/jdbc-10201-088211.html) and add it to class path to executable tm_etl.jar:
    java -classpath "[path_to_driver]ojdbc14.jar" -jar tm_etl.jar [options]

After processing is complete, the study folders and subfolders will be renamed according to the following principle:

 - If any component (clinical, expression, etc) failed, that particular folder will be marked with `_FAIL_` prefix, e.g. `_FAIL_ClinicalDataToUpload`
 - In addition, the entire study folder will be renamed accordingly
 - In case of success, folders will be prefixed with `_DONE_`
 
You can disable any study or study component processing by using `_DISABLED_` prefix for a folder name.

NB! If you want to add Expression or SNP data to downloaded study you should only load new data to existing study.
Loader does not clear all data in study before adding new one, it merges previously loaded data with new samples to avoid duplicates and loss of information.



 
 

