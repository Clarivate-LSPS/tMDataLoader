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

    private interface AuditPrinter {
        void printNewMessages(Sql sql)
    }

    private class DefaultAuditPrinter implements AuditPrinter {
        private def jobId
        protected long lastSeqId

        DefaultAuditPrinter(jobId) {
            this.jobId = jobId
        }

        void printNewMessages(Sql sql) {
            def queryText = "SELECT * FROM " + getConfig().controlSchema + ".cz_job_audit WHERE job_id=${jobId} and seq_id>${lastSeqId} order by seq_id"
            sql.eachRow(queryText) { row ->
                getLogger().log(LogType.DEBUG, "-- ${row.step_desc} [${row.step_status} / ${row.records_manipulated} recs / ${row.time_elapsed_secs}s]")
                lastSeqId = row.seq_id
            }
        }
    }

    private class PostgresAuditPrinter extends DefaultAuditPrinter  {
        private volatile Statement statementToMonitor
        private SQLWarning lastWarning
        private Statement lastStatement

        public PostgresAuditPrinter(jobId, Sql sql) {
            super(jobId)
            sql.withStatement { this.statementToMonitor = it }
        }

        private def printWarnings(SQLWarning warnings) {
            long seqId
            while (warnings) {
                def message = warnings.localizedMessage
                def match
                if ((match = message =~ /^(\d+):\s(.*)$/)) {
                    seqId = match[0][1] as long
                    message = match[0][2]
                }
                if (seqId > lastSeqId) {
                    getLogger().log(LogType.DEBUG, "-- ${message}")
                    lastSeqId = seqId
                }
                lastWarning = warnings
                warnings = warnings.nextWarning
            }
        }

        void printNewMessages(Sql sql) {
            if (statementToMonitor != lastStatement) {
                lastStatement = statementToMonitor
                try {
                    printWarnings(lastStatement.warnings)
                } catch (SQLException ignored) {
                }
            } else if (lastWarning) {
                printWarnings(lastWarning.nextWarning)
            }
            super.printNewMessages(sql)
        }
    }

    boolean process(File dir, studyInfo) {
        def res = false

        logger.log("Connecting to database server")
        database.withSql { sql ->
            sql.connection.autoCommit = false

            if (processFiles(dir, sql, studyInfo)) {
                // run stored procedure(s) in the background here
                // poll log & watch for stored procedure to finish

                // retrieve job number
                sql.call('{call ' + config.controlSchema + '.cz_start_audit(?,?,?)}', [getProcedureName(), config.db.username, Sql.NUMERIC]) {
                    jobId ->

                        sql.commit() // we need this for PostgreSQL version
                        def auditPrinter = database.databaseType == DatabaseType.Postgres ?
                                new PostgresAuditPrinter(jobId, sql) :
                                new DefaultAuditPrinter(jobId)
                        logger.log("Job ID: ${jobId}")

                        def t = Thread.start {
                            logger.log("Run procedures: ${getProcedureName()}")
                            res = runStoredProcedures(jobId, sql, studyInfo)
                            sql.commit() // we need it for postgreSQL version
                        }

                        // opening control connection for the main thread
                        database.withSql { Sql ctrlSql ->
                            while (true) {
                                auditPrinter.printNewMessages(ctrlSql)
                                if (!t.isAlive()) break

                                Thread.sleep(2000)
                            }
                        }
                        auditPrinter.printNewMessages(sql)

                        // figuring out if there are any errors in the error log
                        sql.eachRow("SELECT * FROM " + config.controlSchema + ".cz_job_error where job_id=${jobId} order by seq_id") {
                            logger.log(LogType.ERROR, "${it.error_message} / ${it.error_stack} / ${it.error_backtrace}")
                            res = false
                        }

                        if (res) {
                            logger.log("Procedure completed successfully")
                            sql.call("{call " + config.controlSchema + ".cz_end_audit(?,?)}", [jobId, 'SUCCESS'])
                        } else {
                            logger.log(LogType.ERROR, "Procedure completed with errors!")
                            sql.call("{call " + config.controlSchema + ".cz_end_audit(?,?)}", [jobId, 'FAIL'])
                        }

                        sql.commit() // need this for PostgreSQL version
                }

            }
        }

        return res
    }

}
