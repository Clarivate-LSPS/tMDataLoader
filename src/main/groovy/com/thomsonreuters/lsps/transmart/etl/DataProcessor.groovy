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

import java.nio.file.FileSystems

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

            if (processFiles(dir, sql, studyInfo)) {
                res = new AuditableJobRunner(sql, config).runJob(procedureName) { jobId ->
                    logger.log("Run procedures: ${getProcedureName()}")
                    runStoredProcedures(jobId, sql, studyInfo)
                }
            }
        }
        if ((config?.checkDublicates) && (!res)) {
            database.withSql { sql ->
                FileWriter fileWriter = null;
                CSVPrinter csvFilePrinter = null;

                def rows = sql.rows("select * from wt_clinical_data_dups" as String)
                CSVFormat csvFormat = CSVFormat.DEFAULT.withRecordSeparator('\n')
                try {
                    def fs = FileSystems.getDefault()
                    fileWriter = new FileWriter(fs.getPath(dir.path,'result.csv').toString());

                    csvFilePrinter = new CSVPrinter(fileWriter, csvFormat);
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
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    try {
                        fileWriter.flush();
                        fileWriter.close();
                        csvFilePrinter.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
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
}
