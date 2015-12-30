package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import com.thomsonreuters.lsps.transmart.sql.DatabaseType
import groovy.sql.Sql

import java.nio.file.Path

class SerialHDDDataProcessor extends ExpressionDataProcessor {
    SerialHDDDataProcessor(Object conf) {
        super(conf)
    }

    @Override
    public boolean processFiles(Path dir, Sql sql, studyInfo) {
        database.truncateTable(sql, 'lt_src_mrna_subj_samp_map')
        database.truncateTable(sql, 'lt_src_mrna_data')
        database.truncateTable(sql, 'lt_src_mrna_xml_data')

        def platformList = [] as Set

        dir.eachFileMatch(~/(?i).+_Subject_Sample_Mapping_File(_GPL\d+)*\.txt/) {
            platformList.addAll(processMappingFile(it, sql, studyInfo))
            checkStudyExist(sql, studyInfo)
        }

        platformList = platformList.toList()

        if (platformList.size() > 0) {
            loadPlatforms(dir, sql, platformList, studyInfo)

            dir.eachFileMatch(~/(?i).+_Gene_Expression_Data_[RLTZ](_GPL\d+)*\.txt/) {
                processExpressionFile(it, sql, studyInfo)
            }

            loadSerialMetadata(dir, sql, studyInfo)
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
                    [studyId, studyNode, studyDataType, jobId, Sql.NUMERIC])

            // Load serial hdd data
            if (studyInfo['runSerialHDDLoad']) {
                sql.call("{call " + config.controlSchema + ".i2b2_process_serial_hdd_data(?, ?, ?)}", [studyId, jobId, Sql.NUMERIC])
            }

        } else {
            config.logger.log(LogType.ERROR, "Study ID or Node or DataType not defined!")
            return false;
        }
        return true;
    }

    private boolean loadSerialMetadata(Path dir, Sql sql, Object studyInfo) {
        def metadataFiles = [] as Set

        dir.eachFileMatch(~/(?i).+_Sample_Dimensions_Mapping.txt/) {
            studyInfo['runSerialHDDLoad'] = true
            def fileName = it.getFileName().toString()

            config.logger.log("Processing ${fileName}")
            metadataFiles.add(fileName)

            if (database?.databaseType == DatabaseType.Postgres) {
                processDimensionsMappingFileForPostgres(it, studyInfo)
            } else {
                processDimensionsMappingFileForGeneric(it, sql, studyInfo)
            }
        }

        if (metadataFiles.isEmpty()) {
            throw new Exception("File with sample dimensions mapping was not found in ${dir.getAbsolutePath()}!")
        }
    }

    void processDimensionsMappingFileForPostgres(Path f, studyInfo) {
        DataLoader.start(database, "lt_src_mrna_xml_data", ['STUDY_ID', 'CATEGORY_CD', 'C_METADATAXML']) {
            st ->
                def lineNum = processEachMappingRow(f, studyInfo) { row ->
                    st.addBatch(row)
                }
                config.logger.log("Processed ${lineNum} rows")
        }
    }

    private processDimensionsMappingFileForGeneric(f, sql, studyInfo) {
        def lineNum = 0
        sql.withTransaction {
            sql.withBatch(1000, """\
				INSERT into lt_src_mrna_xml_data (STUDY_ID, CATEGORY_CD, C_METADATAXML)
				VALUES (?, ?, ?)
			""") {
                stmt ->
                    lineNum = processEachMappingRow f, studyInfo, { row -> stmt.addBatch(row) }
            }
        }

        sql.commit()
        config.logger.log(LogType.PROGRESS, "")
        config.logger.log("Processed ${lineNum} rows")
    }

    private processEachMappingRow(Path f, studyInfo, Closure<List> processRow) {
        def row = [studyInfo.id as String, null, null]
        def lineNum = 0
        def dataFile = new CsvLikeFile(f)
        def header = dataFile.header
        def xml = ''
        def category_cd = ''
        if (!(header[0] ==~ /(?i)category_cd/)) {
            throw new Exception("Incorrect sample dimension file header!")
        }
        dataFile.eachEntry { cols ->
            lineNum++;

            config.logger.log(LogType.PROGRESS, "[${lineNum}]")
            category_cd = cols[0].replace('_', ' ')
            xml = """<?xml version="1.0"?>
                        <ValueMetadata>
                            <Oktousevalues>Y</Oktousevalues>
                            <SeriesMeta>
                                <Value>${cols[1]}</Value>
                                <Unit>${cols[2]}</Unit>
                                <DisplayName>${cols[3]}</DisplayName>
                            </SeriesMeta>
                    </ValueMetadata>"""
            row[1] = category_cd
            row[2] = xml.toString()
            processRow(row)
        }
        config.logger.log(LogType.PROGRESS, "")
        return lineNum
    }
}
