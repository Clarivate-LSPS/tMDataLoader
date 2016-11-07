tMDataLoader
============

tm_etl.jar - Transmart ETL tool
Contributed by Thomson Reuters

src folder contains all sources
sample_data folder contains sample public datasets from GEO

WARNING: Since version 1.0.0 tMDataLoader working with own schema tm_dataloader for PostgreSQL version (it also uses
its own user tm_dataloader and you should update you connection string).
Since version 1.2.4-96 it also has own schema for Oracle.
If you are switching from earlier version be sure you run scripts
from sql/postgresql folder and remove controlSchema option from Config file and --alt-control-schema from command line
arguments if any. The option still takes effect, but it was intended as hack to avoid tm_cz schema conflicts and you
shouldn't worry about it anymore.


INSTALLING
==========

Run following command to produce tm_etl.jar from sources:

	gradlew deployJar

Gradle will create tm_etl.jar in root directory with all necessary dependencies.

Then, just copy tm_etl.jar to any directory on the server. It can be on the same machine as Transmart or any other one that has direct access to TM_DATALOADER database schema used by tranSMART.

Then, edit Config.groovy file and put it in your ~/.tm_etl directory.
Please make sure you edit the configuration file before using the tool.

### For Oracle

From `sql/oracle/` run following scripts using SQLDeveloper or sqlplus: 
  As dba:
    @migrations.sql
  As tm_dataloader:
    @run_as_tm_dataloader.sql

### For PostgreSQL

From `sql/postgres` directory run following commands (you may need to add host/port information or change "postgres" to your database's superuser name):
				
	psql -d transmart -U postgres -f migrations.sql
	psql -d transmart -U postgres -f permissions.sql
	psql -d transmart -U tm_dataloader -f procedures.sql

### Notes	
	
You should connect to database with user `tm_dataloader` (default password is `tm_dataloader` as well, but it is highly recommended to change the default password).

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
				MetabolomicsData
        MIRNA_QPCRData
        MIRNA_SEQData
        ProteinData
        RBMData
        RNASeqData
			Multiple Sclerosis_Goertsches_GSE24427
				ClinicalDataToUpload <-- both ClinicalData and ClinicalDataToUpload are fine
				ExpressionDataToUpload <-- same for this one
				MetaDataToUpload <-- same for this one
				SNPDataToUpload
				MetabolomicsDataToUpload
				MIRNA_QPCRDataToUpload
				MIRNA_SEQDataToUpload
				ProteinDataToUpload
				RBMDataToUpload
				RNASeqDataToUpload
		Internal Studies <-- put internal studies here
			MyStudyFolder
				MyStudy
					ClinicalData
					ExpressionData
					MetaData
					SNPData
					MetabolomicsData
          MIRNA_QPCRData
          MIRNA_SEQData
          ProteinData
          RBMData
          RNASeqData
		_MetaData <-- you can put metadata here if it contains metadata for several studies. Each .txt file will be processed.
			
Basically, the first level of the directory defines the top category in the Dataset Explorer tree.
The last level (before Clinical/Expression folders) defines the study name that will be used in the tree. All folders in between define intermediate folders in the Dataset Explorer tree.

Please, refer to the data format description for further information.
Please, don't use Non UTF-8 characters in the headers of file or data values

USING TOOL
==========

To start ETL process, just run the following command:

    java -jar tm_etl.jar

You can run it with "-h" option to get a list of all available options:

    $ java -jar tm_etl.jar -h
    usage: tm_etl [options] [<data_dir>]
        --allow-non-unique-columns             Allow non-unique column names
                                               in clinical data files
        --alt-clinical-proc <proc_name>        Name of alternative clinical   
                                               stored procedure (expert       
                                               option)                        
        --alt-control-schema <schema>          Name of alternative control    
                                               schema (TM_DATALOADER) - expert option
         --always-set-visit-name               Add visit name to concept path
                                               even if only one visit found
     -c,--config <config>                      Configuration filename         
        --data-value-first                     Put VISIT NAME after the data  
                                               value (default behavior, use to
                                               override non-standard config)  
        --delete-study-by-id <delete_id>       Delete study by id             
        --delete-study-by-path <delete_path>   Delete study by path           
        --force-start                          Force TM Data Loader start     
                                               (even if another instance is   
                                               already running)               
     -h,--help                                 Show usage information
        --highlight-clinical-data              Highlight studies with clinical
                                               data
     -i,--interactive                          Interactive (console) mode:    
                                               progress bar                   
     -m,--move-study <old_path new_path>       Move study                     
     -n,--no-rename                            Don't rename folders when      
                                               failed                         
     -s,--stop-on-fail                         Stop when upload is failed     
        --secure-study                         Make study securable           
     -t,--use-t                                Do not use Z datatype for T    
                                               expression data (expert option)
     -v,--version                              Display version information and
                                               exit                           
        --visit-name-first                     Put VISIT_NAME before the data 
                                               value

By default, the configuration file location is ~/.tm_etl/Config.groovy.
You can specify the configuration file name using -c option.

If you don't redirect output to the file, you can find -i option useful - it displays progress for some long operations.

IMPORTANT! If your machine doesn't have a direct connection to the internet and requires http proxy, make sure you specify it when running the tool:

    $ java -Dhttp.proxyHost=webproxy.host.com -Dhttp.proxyPort=80 -jar ./tm_etl.jar

Refer to JVM documentation for more information on these options.

After processing is complete, the study folders and subfolders will be renamed according to the following principle:

 - If any component (clinical, expression, etc) failed, that particular folder will be marked with `_FAIL_` prefix, e.g. `_FAIL_ClinicalDataToUpload`
 - In addition, the entire study folder will be renamed accordingly
 - In case of success, folders will be prefixed with `_DONE_`
 
You can disable any study or study component processing by using `_DISABLED_` prefix for a folder name.

NB! If you want to add Expression or SNP data to downloaded study you should only load new data to existing study.
Loader does not clear all data in study before adding new one, it merges previously loaded data with new samples to avoid duplicates and loss of information.



 
 

