tMDataLoader
============

tm_etl.jar - Transmart ETL tool
Contributed by Thomson Reuters

src folder contains all sources
sample_data folder contains sample public datasets from GEO


INSTALLING
==========

Just copy tm_etl.jar to any directory on the server. It can be on the same machine as Transmart or any other one that has direct access to TM_CZ Oracle schema used by Transmart.
Then, place an appropriate Config.groovy file in your ~/.tm_etl directory. Please make sure you edit the configuration file before using the tool.

PREPARING DATA FOR UPLOAD
=========================

First, you need to put studies you want to upload in the corresponding directory on the machine, that you specified in the configuration file.
It should have the following structure (below is just an example):

YOUR_ETL_DIRECTORY
	Public Studies <-- put public studies here
		Multiple Sclerosis_Baranzini_GSE13732
			ClinicalData
			ExpressionData
			MetaData
		Multiple Sclerosis_Goertsches_GSE24427
			ClinicalDataToUpload <-- both ClinicalData and ClinicalDataToUpload are fine
			ExpressionDataToUpload <-- same for this one
			MetaDataToUpload <-- same for this one
	Internal Studies <-- put internal studies here
		MyStudy
			ClinicalData
			ExpressionData
			MetaData
	_MetaData <-- you can put metadata here if it contains metadata for several studies. Each .txt file will be processed.
			
Basically, the first level of the directory defines the top category in the Dataset Explorer tree.
The second level defines the study name that will be used in the tree.

Please, refer to the data format description for further information.

USING TOOL
==========

To start ETL process, just run the following command:

java -jar tm_etl.jar

You can run it with "-h" option to get a list of all available options:

$ java -jar tm_etl.jar -h
usage: tm_etl [options] [<data_dir>]
 -c,--config <config>   Configuration filename
 -h,--help              Show usage information
 -i,--interactive       Interactive (console) mode: progress bar
 -n,--no-rename         Don't rename folders when failed
 -v,--version           Display version information
 
By default, the configuration file location is ~/.tm_etl/Config.groovy.
You can specify the configuration file name using -c option.

If you don't redirect output to the file, you can find -i option useful - it displays progress for some long operations.

IMPORTANT! If your machine doesn't have a direct connection to the internet and requires http proxy, make sure you specify it when running the tool:
$ java -Dhttp.proxyHost=webproxy.host.com -Dhttp.proxyPort=80 -jar ./tm_etl.jar

Refer to JVM documentation for more information on these options.

After processing is complete, the study folders and subfolders will be renamed according to the following principle:

 - If any component (clinical, expression, etc) failed, that particular folder will be marked with _FAIL_ prefix, e.g. _FAIL_ClinicalDataToUpload
 - In addition, the entire study folder will be renamed accordingly
 - In case of success, folders will be prefixed with _DONE_
 
You can disable any study or study component processing by using _DISABLED_ prefix for a folder name.



 
 

