/*************************************************************************
 * tranSMART Data Loader - ETL tool for tranSMART
 *
 * Copyright 2012-2013 Thomson Reuters
 *
 * This product includes software developed at Thomson Reuters
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License 
 * as published by the Free Software  
 * Foundation, either version 3 of the License, or (at your option) any later version, along with the following terms:
 * 1.	You may convey a work based on this program in accordance with section 5, provided that you retain the above notices.
 * 2.	You may convey verbatim copies of this program code as you receive it, in any medium, provided that you retain the above notices.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS    * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *
 ******************************************************************/

package com.thomsonreuters.lsps.transmart.etl

import groovy.sql.Sql

class ExpressionDataProcessor extends DataProcessor {

    public ExpressionDataProcessor(Object conf) {
        super(conf);
    }

    @Override
    public boolean processFiles(File dir, Sql sql, Object studyInfo) {
        sql.execute('TRUNCATE TABLE tm_lz.lt_src_mrna_subj_samp_map')
        sql.execute('TRUNCATE TABLE tm_lz.lt_src_mrna_data')

        def platformList = [] as Set

        dir.eachFileMatch(~/(?i).+_Subject_Sample_Mapping_File(_GPL\d+)*\.txt/) {
            platformList.addAll(processMappingFile(it, sql, studyInfo))
        }

        platformList = platformList.toList()

        if (platformList.size() > 0) {
            loadPlatforms(dir, sql, platformList, studyInfo)

            dir.eachFileMatch(~/(?i).+_Gene_Expression_Data_[RLTZ](_GPL\d+)*\.txt/) {
                processExpressionFile(it, sql, studyInfo)
            }
        } else {
            throw new Exception("No platforms defined")
        }

        return true;
    }

    @Override
    public boolean runStoredProcedures(Object jobId, Sql sql, Object studyInfo) {
        def studyId = studyInfo['id']
        def studyNode = studyInfo['node']
        def studyDataType = studyInfo['datatype']

        if (studyDataType == 'T' && !config.useT) {
            config.logger.log("Original DataType='T', but using 'Z' instead (workaround); use -t option to alter this behavior")
            studyDataType = 'Z' // temporary workaround due to a bug in Transmart
        }

        if (studyId && studyNode && studyDataType) {
            config.logger.log("Study ID=${studyId}; Node=${studyNode}; Data Type=${studyDataType}")

            if (studyInfo['runPlatformLoad']) {
                sql.call("{call " + config.controlSchema + ".i2b2_load_annotation_deapp()}")
            }

            sql.call("{call " + config.controlSchema + ".i2b2_process_mrna_data (?, ?, ?, null, null, '" + config.securitySymbol + "', ?, ?)}",
                    [studyId, studyNode, studyDataType, jobId, Sql.NUMERIC]) {}
        } else {
            config.logger.log(LogType.ERROR, "Study ID or Node or DataType not defined!")
            return false;
        }
        return true;
    }

    @Override
    public String getProcedureName() {
        return "I2B2_PROCESS_MRNA_DATA";
    }

    private List processMappingFile(File f, Sql sql, studyInfo) {
        def platformList = [] as Set
        def studyIdList = [] as Set

        config.logger.log("Mapping file: ${f.name}")

        def lineNum = 0

        sql.withTransaction {
            sql.withBatch(100, """\
				INSERT into tm_lz.lt_src_mrna_subj_samp_map (TRIAL_NAME, SITE_ID, 
					SUBJECT_ID, SAMPLE_CD, PLATFORM, TISSUE_TYPE, 
					ATTRIBUTE_1, ATTRIBUTE_2, CATEGORY_CD, SOURCE_CD) 
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'STD')
		""") {
                stmt ->

                    f.splitEachLine("\t") {
                        cols ->

                            lineNum++
                            // cols: 0:study_id, 1:site_id, 2:subject_id, 3:sample_cd, 4:platform, 5:tissuetype, 6:attr1, 7:attr2, 8:category_cd
                            if (cols[0] && lineNum > 1) {
                                if (!(cols[2] && cols[3] && cols[4] && cols[8]))
                                    throw new Exception("Incorrect mapping file: mandatory columns not defined")

                                platformList << cols[4]
                                studyIdList << cols[0]

                                stmt.addBatch(cols)
                            }
                    }
            }
        }

        studyIdList = studyIdList.toList()
        platformList = platformList.toList()

        sql.commit()
        config.logger.log("Processed ${lineNum} rows")

        if (studyIdList.size() > 0) {
            if (studyIdList.size() > 1) {
                throw new Exception("Multiple studies in one mapping file")
            } else {
                def studyId = studyIdList[0]
                if (studyInfo['id'] && studyId != studyInfo['id']) {
                    throw new Exception("Study ID doesn't match clinical data")
                } else {
                    studyInfo['id'] = studyId
                }
            }
        }

        return platformList
    }

    private void loadPlatforms(File dir, Sql sql, List platformList, studyInfo) {
        def platformLoader = new PlatformLoader(sql, config)
        platformList.each { String platform ->
            platformLoader.doLoad(new File(dir, "${platform}.txt"), platform, studyInfo)
        }
    }

    private void processExpressionFile(File f, Sql sql, studyInfo) {
        config.logger.log("Processing ${f.name}")

        // retrieve data type
        def m = f.name =~ /(?i)Gene_Expression_Data_([RLTZ])/
        if (m[0]) {
            def dataType = m[0][1]
            if (studyInfo['datatype']) {
                if (studyInfo['datatype'] != dataType)
                    throw new Exception("Multiple data types in one study are not supported")
            } else {
                studyInfo['datatype'] = dataType
            }
        }

        if (isLocalPostgresConnection()) {
            processExpressionFileForPostgres(f, sql, studyInfo)
        } else {
            processExpressionFileForGeneric(f, sql, studyInfo)
        }
    }

    private void processExpressionFileForPostgres(File f, Sql sql, studyInfo) {
        def tempCsv = File.createTempFile("expressionData", ".csv")
        def lineNum = 0
        tempCsv.withPrintWriter {
            writer ->
                lineNum = processEachRow f, studyInfo, {
                    row ->
                        writer.append(row[0]).append(',').
                                append(row[1]).append(',').
                                append(row[2]).append(',').
                                append(row[3]).append("\n")
                }
        }
        config.logger.log("Loading ${lineNum} rows into database")
        sql.execute("COPY tm_lz.lt_src_mrna_data FROM '${tempCsv.getCanonicalPath()}' WITH DELIMITER ','".toString())
        tempCsv.delete()
        config.logger.log("Processed ${lineNum} rows")
    }

    private void processExpressionFileForGeneric(File f, Sql sql, studyInfo) {
        def lineNum = 0
        sql.withTransaction {
            sql.withBatch(1000, """\
				INSERT into tm_lz.lt_src_mrna_data (TRIAL_NAME, PROBESET, EXPR_ID, INTENSITY_VALUE)
				VALUES (?, ?, ?, ?)
			""") {
                stmt ->
                    lineNum = processEachRow f, studyInfo, { row -> stmt.addBatch(row) }
            }
        }

        sql.commit()
        config.logger.log(LogType.PROGRESS, "")
        config.logger.log("Processed ${lineNum} rows")
    }

    private long processEachRow(File f, studyInfo, Closure<List> processRow) {
        def row = [studyInfo.id as String, null, null, null]
        def lineNum = 0
        def header = []
        f.splitEachLine("\t") {
            cols ->
                lineNum++;
                if (lineNum == 1) {
                    if (cols[0] != "ID_REF") throw new Exception("Incorrect gene expression file")

                    cols.each {
                        header << it
                    }
                } else {
                    config.logger.log(LogType.PROGRESS, "[${lineNum}]")
                    row[1] = cols[0]
                    cols.eachWithIndex { val, i ->
                        // skip first column
                        // rows should have intensity assigned to them, otherwise not interested
                        if (i > 0 && val) {
                            row[2] = header[i] as String
                            row[3] = val
                            processRow(row)
                        }
                    }
                }
        }
        config.logger.log(LogType.PROGRESS, "")
        return lineNum
    }
}
