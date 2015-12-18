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

import com.thomsonreuters.lsps.transmart.sql.Database
import groovy.sql.Sql
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter

abstract class DataProcessor {
    def config
    Database database

    DataProcessor(conf) {
        config = conf
        database = new Database(config)
    }

    abstract boolean processFiles(File dir, Sql sql, studyInfo)

    abstract boolean runStoredProcedures(jobId, Sql sql, studyInfo)

    abstract String getProcedureName()

    Logger getLogger() {
        return config.logger
    }

    boolean process(File dir, studyInfo) {
        def res = false

        logger.log("Connecting to database server")
        database.withSql { sql ->
            sql.connection.autoCommit = false

            String currentNodePath = (studyInfo.node[studyInfo.node.size() - 1] == '\\' ? studyInfo.node : studyInfo.node + '\\')
            currentNodePath = currentNodePath[0] == '\\' ? (currentNodePath) : ('\\' + currentNodePath)

            def row = sql.rows("select distinct sourcesystem_cd from i2b2metadata.i2b2 where " +
                    "c_fullname like ? ESCAPE '~'", [currentNodePath + '%'])

            if (row.size() > 1) {
                throw new Exception("This path contains several different studyId : ${currentNodePath}")
            }

            if ((config?.replaceStudy) && (row.size() != 0)) {
                def processor = new DeleteDataProcessor(config)
                studyInfo.put('oldId', row[0].sourcesystem_cd)
                processor.process('id': row[0].sourcesystem_cd, 'path': currentNodePath);
            }

            if (processFiles(dir, sql, studyInfo)) {
                res = new AuditableJobRunner(sql, config).runJob(procedureName) { jobId ->
                    logger.log("Run procedures: ${getProcedureName()}")
                    runStoredProcedures(jobId, sql, studyInfo)
                }
            }
        }
        if ((config?.checkDuplicates) && (!res)) {
            database.withSql { sql ->
                def rows = sql.rows("select * from wt_clinical_data_dups" as String)
                CSVFormat csvFormat = CSVFormat.DEFAULT.withRecordSeparator('\n')
                try {
                    new File(dir, 'duplicates.csv').withWriter { fileWriter ->
                        new CSVPrinter(fileWriter, csvFormat).withCloseable { CSVPrinter csvFilePrinter ->
                            Object[] FILE_HEADER = ["site_id", "subject_id", "visit_name", "data_label", "category_cd", "modifier_cd", "link_value"]
                            csvFilePrinter.printRecord(FILE_HEADER);
                            rows.each {
                                List csvRow = new ArrayList();
                                csvRow.add(it.site_id ?: '')
                                csvRow.add(it.subject_id ?: '')
                                csvRow.add(it.visit_name ?: '')
                                csvRow.add(it.data_label ?: '')
                                csvRow.add(it.category_cd ?: '')
                                csvRow.add(it.modifier_cd ?: '')
                                csvRow.add(it.link_value ?: '')

                                csvFilePrinter.printRecord(csvRow)
                            }
                        }
                    }
                } catch (Exception e) {
                    logger.log(LogType.ERROR, e)
                }
            }
        }

        if ((config?.replaceStudy) && (res) && (studyInfo.oldId != null)) {
            database.withSql { sql ->
                String newToken = ("EXP:" + studyInfo.id).toUpperCase();
                String oldToken = "EXP:" + studyInfo.oldId;

                sql.execute("DELETE FROM biomart.bio_experiment WHERE accession = :newToken",
                        [newToken: (String) studyInfo.id.toUpperCase()])
                sql.execute("DELETE FROM biomart.bio_data_uid WHERE unique_id = :newToken",
                        [newToken: newToken])
                sql.execute("DELETE FROM searchapp.search_secure_object WHERE bio_data_unique_id = :newToken",
                        [newToken: newToken])

                sql.executeUpdate("UPDATE biomart.bio_data_uid SET unique_id = :newToken WHERE unique_id = :oldToken",
                        [newToken: newToken,
                         oldToken: oldToken])

                sql.executeUpdate("UPDATE biomart.bio_experiment SET accession = :newToken WHERE accession = :oldToken",
                        [newToken: (String) studyInfo.id.toUpperCase(),
                         oldToken: (String) studyInfo.oldId]
                )
                sql.executeUpdate("UPDATE searchapp.search_secure_object " +
                        "SET bio_data_unique_id = :newToken " +
                        "WHERE bio_data_unique_id = :oldToken",
                        [newToken: newToken,
                         oldToken: oldToken])
            }
        }

        return res
    }

    void checkStudyExist(Sql sql, studyInfo) {
        String fullName = "${(studyInfo['node'] =~ /^\\.*/ ? '' : '\\')}${studyInfo['node']}\\%";
        def row = sql.firstRow("""
                select c_fullname
                from i2b2metadata.i2b2
                where sourcesystem_cd = UPPER(?) and c_fullname not like ? escape '`' order by c_fullname""",
                [studyInfo['id'], fullName])
        if (row) {
            throw new Exception("Other study with same id found by different path: ${row.c_fullname}")
        }
    }

    void ckeckStudyIdExist(Sql sql, studyInfo) {
        String fullName = "${(studyInfo['node'] =~ /^\\.*/ ? '' : '\\')}${studyInfo['node']}\\%";
        def row = sql.firstRow("""
                                select c_fullname
                                from i2b2metadata.i2b2
                                where sourcesystem_cd <> UPPER(?) and c_fullname like ? escape '`' order by c_fullname""",
                [studyInfo['id'], fullName])
        if (row && (!config.replaceStudy)) {
            throw new Exception("Other study with same path found by different studyId: ${row.c_fullname}")
        }
    }
}
