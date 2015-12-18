package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.etl.platforms.aCGHPlatform
import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import groovy.sql.Sql

/**
 * Created by transmart on 3/6/15.
 */
class ACGHDataProcessor extends DataProcessor {
    ACGHDataProcessor(Object conf) {
        super(conf)
    }

    private List processMappingFile(File f, Sql sql, studyInfo){
        def platformList = [] as Set
        def studyIdList = [] as Set

        config.logger.log("Mapping file: ${f.name}")

        int lineNum = 0
        def mappingFile = new CsvLikeFile(f)

        sql.withTransaction {
            sql.withBatch(100, """\
				INSERT into lt_src_mrna_subj_samp_map (TRIAL_NAME, SITE_ID,
					SUBJECT_ID, SAMPLE_CD, PLATFORM, TISSUE_TYPE,
					ATTRIBUTE_1, ATTRIBUTE_2, CATEGORY_CD, SOURCE_CD)
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		""") { stmt ->
                mappingFile.eachEntry { cols ->
                    lineNum++
                    // cols: 0:study_id, 1:site_id, 2:subject_id, 3:sample_cd, 4:platform, 5:tissuetype, 6:attr1, 7:attr2, 8:category_cd

                    if (!(cols[2] && cols[3] && cols[4] && cols[8]))
                        throw new Exception("Incorrect mapping file: mandatory columns not defined")

                    platformList << cols[4]
                    studyIdList << cols[0]?.toUpperCase()

                    stmt.addBatch(cols)
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

    @Override
    boolean processFiles(File dir, Sql sql, Object studyInfo) {
        sql.execute("DELETE FROM lt_src_mrna_subj_samp_map" as String)
        sql.execute("DELETE FROM lt_src_acgh_data" as String)

        def platformList = [] as Set

        dir.eachFileMatch(~/(?i).+_Subject_Sample_Mapping_File(_GPL\d+)*\.txt/) {
            platformList.addAll(processMappingFile(it, sql, studyInfo))
        }
        checkStudyExist(sql, studyInfo)

        platformList = platformList.toList()
        if (platformList.size() > 0) {
            loadPlatforms(dir, sql, platformList, studyInfo)

            dir.eachFileMatch(~/(?i).+_acgh_data(_GPL\d+)*\.txt/) {
                processACGHFile(it, sql, studyInfo)
            }
        } else {
            throw new Exception("No platforms defined")
        }

        return true;
    }

    private void loadPlatforms(File dir, Sql sql, List platformList, studyInfo) {
        platformList.each { String platform ->

            File acghPlatformFile;
            dir.eachFileMatch(~/${platform}_region_platform.txt|${platform}.txt/){
                acghPlatformFile = new File('', it)
            }

            if (acghPlatformFile.exists()){
                def acghPlatform = new aCGHPlatform(acghPlatformFile, platform, config)
                studyInfo['loadaCGHPlatform'] = !acghPlatform.isLoaded(sql)
                acghPlatform.load(sql, studyInfo)
            } else {
                def row = sql.firstRow("SELECT count(*) as cnt FROM deapp.de_chromosomal_region WHERE gpl_id = ?",[platform])
                if (row?.cnt == 0)
                    throw new Exception("No platforms file")

            }
        }
    }



    void processACGHFile(File f, Sql sql, studyInfo) {
        config.logger.log("Processing ${f.name}")

        DataLoader.start(database, "lt_src_acgh_data", ['TRIAL_NAME', 'REGION_NAME', 'EXPR_ID', 'CHIP', 'SEGMENTED', 'FLAG', 'PROBLOSS', 'PROBNORM', 'PROBGAIN', 'PROBAMP']) {
            st ->
                def lineNum = processEachRow(f, studyInfo) { row ->
                    st.addBatch(row)
                }
                config.logger.log("Processed ${lineNum} rows")
        }
    }

    long processEachRow(File f, studyInfo, Closure<List> processRow) {
        def row = [studyInfo.id as String, null, null, null, null, null, null, null, null, null]
        def lineNum = 0
        def dataFile = new CsvLikeFile(f)
        def header = dataFile.header
        if (header[0] != 'region_id') {
            throw new Exception("Incorrect acgh data file")
        }

        dataFile.eachEntry { cols ->
            lineNum++;

            config.logger.log(LogType.PROGRESS, "[${lineNum}]")
            row[1] = cols[0]
            def j = 0
            cols.eachWithIndex { val, i ->
                if (i > 0) {
                    if (j == 0) {
                        row[2] = (header[i] as String).replace('.chip', '')
                        row[3] = val
                    } else if (j > 0 && j < 8) {
                        row[(j+3)] = val as String
                    }
                    j++
                    if (j == 7) {
                        processRow(row)
                        j = 0
                    }
                }
            }
        }
        config.logger.log(LogType.PROGRESS, "")
        return lineNum
    }

    @Override
    boolean runStoredProcedures(Object jobId, Sql sql, Object studyInfo) {
        def studyId = studyInfo['id']
        def studyNode = studyInfo['node']

        if (studyId && studyNode) {
            if (studyInfo['loadaCGHPlatform']) {
                sql.call("{call " + config.controlSchema + ".i2b2_load_chrom_region()}")
            }

            sql.call("{call " + config.controlSchema + ".i2b2_process_acgh_data (?, ?, ?, '" + config.securitySymbol + "', ?, ?)}",
                    [studyId, studyNode, 'STD', jobId, Sql.NUMERIC]) {}
        } else {
            config.logger.log(LogType.ERROR, "Study ID or Node not defined!")
            return false;
        }
        return true
    }

    @Override
    String getProcedureName() {
        return  "I2B2_PROCESS_ACGH_DATA"
    }
}
