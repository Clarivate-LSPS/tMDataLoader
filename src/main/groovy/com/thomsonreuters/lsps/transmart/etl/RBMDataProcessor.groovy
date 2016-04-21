package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.db.loader.DataLoader
import com.thomsonreuters.lsps.transmart.etl.platforms.RBMPlatform
import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import com.thomsonreuters.lsps.db.core.DatabaseType;
import groovy.sql.Sql

import java.nio.file.Path

public class RBMDataProcessor extends AbstractDataProcessor {
    public RBMDataProcessor(Object conf) {
        super(conf);
    }

    @Override
    public boolean processFiles(Path dir, Sql sql, studyInfo) {
        database.truncateTable(sql, 'lt_src_rbm_subj_samp_map')
        database.truncateTable(sql, 'lt_src_rbm_data')

        def platformList = [] as Set

        dir.eachFileMatch(~/(?i).+_Subject_Sample_Mapping_File(_GPL\d+)*\.txt/) {
            platformList.addAll(processMappingFile(it, sql, studyInfo))
            checkStudyExist(sql, studyInfo)
        }

        platformList = platformList.toList()

        if (platformList.size() > 0) {
            loadPlatforms(dir, sql, platformList, studyInfo)

            dir.eachFileMatch(~/(?i).+_RBM_Data_[RLTZ](_GPL\d+)*\.txt/) {
                processRBMFile(it, sql, studyInfo)
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
                sql.call("{call " + config.controlSchema + ".i2b2_load_rbm_annotation()}")
            }

            sql.call("{call " + config.controlSchema + ".i2b2_load_rbm_data (?, ?, ?, null, null, '" + config.securitySymbol + "', ?, ?)}",
                    [studyId, studyNode, studyDataType, jobId, Sql.NUMERIC]) {}
        } else {
            config.logger.log(LogType.ERROR, "Study ID or Node or DataType not defined!")
            return false;
        }
        return true;
    }

    @Override
    public String getProcedureName() {
        return "I2B2_LOAD_RBM_DATA";
    }

    private List processMappingFile(Path f, Sql sql, studyInfo) {
        def platformList = [] as Set
        def studyIdList = [] as Set

        config.logger.log("Mapping file: ${f.getFileName()}")

        int lineNum = 0
        def mappingFile = new CsvLikeFile(f)

        sql.withTransaction {
            sql.withBatch(100, """\
				INSERT into lt_src_rbm_subj_samp_map (TRIAL_NAME, SITE_ID,
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

    private void loadPlatforms(Path dir, Sql sql, List platformList, studyInfo) {
        platformList.each { String platform ->
            def rbmPlatform = new RBMPlatform(dir.resolve("${platform}.txt"), platform, config)
            rbmPlatform.load(sql, studyInfo)
        }
    }

    private void processRBMFile(Path f, Sql sql, studyInfo) {
        config.logger.log("Processing ${f.fileName}")

        // retrieve data type
        def m = f.fileName.toString() =~ /(?i)RBM_Data_([RLTZ])/
        if (m[0]) {
            def dataType = m[0][1]
            if (studyInfo['datatype']) {
                if (studyInfo['datatype'] != dataType)
                    throw new Exception("Multiple data types in one study are not supported")
            } else {
                studyInfo['datatype'] = dataType
            }
        }

        if (database?.databaseType == DatabaseType.Postgres) {
            processRBMFileForPostgres(f, studyInfo)
        } else {
            processRBMFileForGeneric(f, sql, studyInfo)
        }
    }

    private void processRBMFileForPostgres(Path f, studyInfo) {
        DataLoader.start(database, "lt_src_rbm_data", ['TRIAL_ID', 'SAMPLE_ID', 'ANALYTE', 'AVALUE']) {
            st ->
                def lineNum = processEachRow(f, studyInfo) { row ->
                    st.addBatch(row)
                }
                config.logger.log("Processed ${lineNum} rows")
        }
    }

    private void processRBMFileForGeneric(Path f, Sql sql, studyInfo) {
        def lineNum = 0
        sql.withTransaction {
            sql.withBatch(1000, """\
				INSERT into lt_src_rbm_data (TRIAL_ID, SAMPLE_ID, ANALYTE, AVALUE)
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

    private long processEachRow(Path f, studyInfo, Closure<List> processRow) {
        def row = [studyInfo.id as String, null, null, null]
        def lineNum = 0
        def dataFile = new CsvLikeFile(f)
        def header = dataFile.header
        if (header[2].toUpperCase() != 'SAMPID' && !header[5].toUpperCase().contains('ANALYTE')
                && header[7].toUpperCase() != 'AVALUE') {
            throw new Exception("Incorrect RBM data file")
        }
        dataFile.eachEntry { cols ->
            lineNum++;

            config.logger.log(LogType.PROGRESS, "[${lineNum}]")
            row[1] = cols[2]
            row[2] = cols[5]
            row[3] = cols[7]
            if (row[1] && row[2] && row[3]) {
                processRow(row)
            }

        }
        config.logger.log(LogType.PROGRESS, "")
        return lineNum
    }
}
