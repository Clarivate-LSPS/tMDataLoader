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
        Sql.withInstance(config.db.jdbcConnectionString, config.db.username, config.db.password, config.db.jdbcDriver) { sql ->
            sql.connection.autoCommit = false

            if (processData()) {
                res = new AuditableJobRunner(sql, config).runJob(procedureName) { jobId ->
                    config.logger.log("Run procedures: ${getProcedureName()}")
                    res = runStoredProcedures(jobId, sql, data)
                }
            }
        }

        return res
    }
}
