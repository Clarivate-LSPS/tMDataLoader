package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.sql.Database
import com.thomsonreuters.lsps.transmart.sql.DatabaseType
import groovy.sql.Sql

import java.sql.SQLException
import java.sql.SQLWarning
import java.sql.Statement
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/**
 * Created by bondarev on 5/7/14.
 */
class AuditableJobRunner {
    Sql sql
    def config

    private Database database

    def getLogger() {
        config.logger
    }

    AuditableJobRunner(Sql sql, config) {
        this.sql = sql
        this.config = config
        this.database = new Database(config.db)
    }

    boolean runJob(String jobName, Closure block) {
        AtomicBoolean res = new AtomicBoolean(false);

        // run stored procedure(s) in the background here
        // poll log & watch for stored procedure to finish

        // retrieve job number
        sql.call('{call ' + config.controlSchema + '.cz_start_audit(?,?,?)}', [jobName, config.db.username, Sql.NUMERIC]) { jobId ->
            sql.commit() // we need this for PostgreSQL version
            def auditPrinter = database.databaseType == DatabaseType.Postgres ?
                    new PostgresAuditPrinter(jobId, sql) :
                    new DefaultAuditPrinter(jobId)
            logger.log("Job ID: ${jobId}")

            def t = Thread.start {
                res.set(block.call(jobId))
                sql.commit() // we need it for postgreSQL version
            }

            // opening control connection for the main thread
            database.withSql { Sql ctrlSql ->
                while (t.isAlive()) {
                    auditPrinter.printNewMessages(ctrlSql)
                    t.join(1000)
                }
            }
            auditPrinter.printNewMessages(sql)

            // figuring out if there are any errors in the error log
            sql.eachRow("SELECT * FROM " + config.controlSchema + ".cz_job_error where job_id=${jobId} order by seq_id") {
                logger.log(LogType.ERROR, "${it.error_message} / ${it.error_stack} / ${it.error_backtrace}")
                res.set(false)
            }

            if (res.get()) {
                logger.log("Procedure completed successfully")
                sql.call("{call " + config.controlSchema + ".cz_end_audit(?,?)}", [jobId, 'SUCCESS'])
            } else {
                logger.log(LogType.ERROR, "Procedure completed with errors!")
                sql.call("{call " + config.controlSchema + ".cz_end_audit(?,?)}", [jobId, 'FAIL'])
            }

            sql.commit() // need this for PostgreSQL version
        }
        return res.get()
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
        private class StatementHolder {
            final Statement statement

            StatementHolder(Statement statement) {
                this.statement = statement
            }
        }

        private AtomicReference<StatementHolder> statementHolder = new AtomicReference<>()
        private StatementHolder lastStatement
        private SQLWarning lastWarning

        public PostgresAuditPrinter(jobId, Sql sql) {
            super(jobId)
            sql.withStatement { Statement statement->
                this.statementHolder.set(new StatementHolder(statement))
            }
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
            StatementHolder current = statementHolder.get()
            if (!current.is(null) && (!current.is(lastStatement) || lastWarning == null)) {
                lastWarning = null
                lastStatement = current
                try {
                    printWarnings(lastStatement.statement.warnings)
                } catch (SQLException ignored) {
                }
            } else if (lastWarning) {
                printWarnings(lastWarning.nextWarning)
            }
            super.printNewMessages(sql)
        }
    }
}
