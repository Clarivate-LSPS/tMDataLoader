package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.db.core.Database
import com.thomsonreuters.lsps.db.core.DatabaseType
import com.thomsonreuters.lsps.db.loader.DataLoader
import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import groovy.sql.Sql

import java.nio.file.Path

class SerialHDDDataLoader {

    Database database
    Map config

    SerialHDDDataLoader(Database db, Map config) {
        this.database = db
        this.config = config
    }

    def loadSerialHDDData(String table, Path dir, Sql sql, Object studyInfo) {
        database.truncateTable(sql, table)

        def metadataFiles = [] as Set
        dir.eachFileMatch(~/(?i).+_Sample_Dimensions_Mapping.txt/) {
            studyInfo['runSerialHDDLoad'] = true
            def fileName = it.getFileName().toString()

            config.logger.log("Processing ${fileName}")
            metadataFiles.add(fileName)

            processDimensionsMappingFileForPostgres(it, studyInfo, table, sql)
        }

        if (metadataFiles.isEmpty()) {
            throw new Exception("File with sample dimensions mapping was not found in ${dir.getAbsolutePath()}!")
        }
    }

    private processDimensionsMappingFileForPostgres(Path f, studyInfo, String table, Sql sql) {
        DataLoader.start(sql, table, ['STUDY_ID', 'CATEGORY_CD', 'C_METADATAXML']) {
            st ->
                def lineNum = processEachMappingRow(f, studyInfo) { row ->
                    st.addBatch(row)
                }
                config.logger.log("Processed ${lineNum} rows")
        }
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
