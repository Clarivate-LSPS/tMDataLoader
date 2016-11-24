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
import com.thomsonreuters.lsps.db.core.Database
import groovy.sql.Sql
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path

abstract class AbstractDataProcessor implements DataProcessor {
    Map config
    Database database

    MergeMode mergeMode = MergeMode.REPLACE

    AbstractDataProcessor(conf) {
        config = conf
        database = TransmartDatabaseFactory.newDatabase(config)
    }

    abstract boolean processFiles(Path dir, Sql sql, studyInfo)

    abstract boolean runStoredProcedures(jobId, Sql sql, studyInfo)

    abstract String getProcedureName()

    Logger getLogger() {
        return config.logger
    }

    boolean process(Path dir, studyInfo) {
        def res = false

        studyInfo.node = "\\${studyInfo.node}\\".replace("\\\\", '\\')

        logger.log("Connecting to database server")
        database.withSql { Sql sql ->
            sql.withTransaction {
                checkStudiesBySamePath(studyInfo, sql)

                if (processFiles(dir, sql, studyInfo)) {
                    res = new AuditableJobRunner(sql, config).runJob(procedureName) { jobId ->
                        logger.log("Run procedures: ${getProcedureName()}")
                        runStoredProcedures(jobId, sql, studyInfo)
                    }
                }
                if (res) {
                    res = new AuditableJobRunner(sql, config).runJob(procedureName) { jobId ->
                        def postStudyProcessor = new PostStudyProcessor(config, sql, dir, studyInfo, jobId)
                        postStudyProcessor.process()
                    }
                }
            }
        }

        return res
    }

    protected void checkStudiesBySamePath(studyInfo, sql) {
        def row = sql.rows("""
            select distinct sourcesystem_cd
            from i2b2metadata.i2b2
            where sourcesystem_cd is not null
              and c_fullname like ? || '%' ESCAPE '`'
            order by sourcesystem_cd
        """, [studyInfo.node])

        if (row.size() > 1) {
            throw new DataProcessingException("'${studyInfo.node}' path contains several different studyIds: ${row*.sourcesystem_cd}")
        }

        if (row.size() == 1) {
            studyInfo.oldId = row[0].sourcesystem_cd
        }

        if (studyInfo.oldId && (config?.replaceStudy)) {
            logger.log(LogType.MESSAGE, "Found another study by path: '${studyInfo.node}' with ID: ${studyInfo.oldId}. Removing...")
            new DeleteDataProcessor(config).process('id': studyInfo.oldId, 'path': studyInfo.node);
        }
    }

    void checkStudyExist(Sql sql, studyInfo) {
        if (studyInfo.oldId && !config.replaceStudy && studyInfo.oldId != studyInfo.id?.toUpperCase()) {
            throw new DataProcessingException("Other study by the same path found with different studyId: old = '${studyInfo.oldId}', new = '${studyInfo.id}'" as String)
        }

        def row = sql.firstRow("""
                select distinct
                  first_value(c_fullname) over (partition by sourcesystem_cd order by c_fullname) as c_fullname
                from i2b2metadata.i2b2
                where sourcesystem_cd = UPPER(?)""",
                [studyInfo.id])
        if (row && row.c_fullname != studyInfo.node) {
            throw new DataProcessingException("Other study with same id found by different path: ${row.c_fullname}" as String)
        }
    }
}
