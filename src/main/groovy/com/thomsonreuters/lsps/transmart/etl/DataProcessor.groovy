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
import com.thomsonreuters.lsps.transmart.sql.DatabaseType
import groovy.sql.Sql

import java.sql.SQLException
import java.sql.SQLWarning
import java.sql.Statement
import java.util.concurrent.atomic.AtomicReference

abstract class DataProcessor {
    def config
    Database database

    DataProcessor(conf) {
        config = conf
        database = config.db ?: new Database(config.db)
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
                res = new AuditableJobRunner(sql, config).runJob(procedureName) { jobId->
                    logger.log("Run procedures: ${getProcedureName()}")
                    runStoredProcedures(jobId, sql, studyInfo)
                }
            }
        }
        return res
    }

}
