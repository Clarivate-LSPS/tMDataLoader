package com.thomsonreuters.lsps.transmart.etl

import groovy.sql.Sql

abstract class DataOperationProcessor {
    def config

    DataOperationProcessor(conf) {
        config = conf
    }

    abstract boolean runStoredProcedures(jobId, Sql sql, data)

    abstract String getProcedureName()

    abstract def processData()

    boolean process(data) {
        def res = false

        config.logger.log("Connecting to database server")
        def sql = Sql.newInstance(config.db.jdbcConnectionString, config.db.username, config.db.password, config.db.jdbcDriver)
        sql.connection.autoCommit = false

        if (processData()) {
            // run stored procedure(s) in the background here
            // poll log & watch for stored procedure to finish

            // retrieve job number
            sql.call('{call ' + config.controlSchema + '.cz_start_audit(?,?,?)}', [getProcedureName(), config.db.username, Sql.NUMERIC]) {
                jobId ->

                    sql.commit() // we need this for PostgreSQL version

                    config.logger.log("Job ID: ${jobId}")

                    def t = Thread.start {
                        config.logger.log("Run procedures: ${getProcedureName()}")
                        res = runStoredProcedures(jobId, sql, data)
                        sql.commit() // we need it for postgreSQL version
                    }

                    def lastSeqId = 0

                    // opening control connection for the main thread
                    def ctrlSql = Sql.newInstance(config.db.jdbcConnectionString, config.db.username, config.db.password, config.db.jdbcDriver)

                    while (true) {
                        // fetch last log message
                        ctrlSql.eachRow("SELECT * FROM " + config.controlSchema + ".cz_job_audit WHERE job_id=${jobId} and seq_id>${lastSeqId} order by seq_id") {
                            row ->

                                config.logger.log(LogType.DEBUG, "-- ${row.step_desc} [${row.step_status} / ${row.records_manipulated} recs / ${row.time_elapsed_secs}s]")
                                lastSeqId = row.seq_id
                        }

                        if (!t.isAlive()) break

                        Thread.sleep(2000)
                    }

                    // closing control connection - don't need it anymore
                    ctrlSql.close()

                    // figuring out if there are any errors in the error log
                    sql.eachRow("SELECT * FROM " + config.controlSchema + ".cz_job_error where job_id=${jobId} order by seq_id") {
                        config.logger.log(LogType.ERROR, "${it.error_message} / ${it.error_stack} / ${it.error_backtrace}")
                        res = false
                    }

                    if (res) {
                        config.logger.log("Procedure completed successfully")
                        sql.call("{call " + config.controlSchema + ".cz_end_audit(?,?)}", [jobId, 'SUCCESS'])
                    } else {
                        config.logger.log(LogType.ERROR, "Procedure completed with errors!")
                        sql.call("{call " + config.controlSchema + ".cz_end_audit(?,?)}", [jobId, 'FAIL'])
                    }

                    sql.commit() // need this for PostgreSQL version
            }

        }

        sql.close()

        return res
    }
}
