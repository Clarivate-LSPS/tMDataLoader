package com.thomsonreuters.lsps.transmart.etl

import groovy.sql.Sql

class DeleteCrossProcessor extends DataOperationProcessor {
    DeleteCrossProcessor(Object conf) {
        super(conf)
    }

    @Override
    boolean runStoredProcedures(Object jobId, Sql sql, data) {
        String path = data.path?.toString()
        Boolean isDeleteConcepts = data.isDeleteConcepts ? 1 : 0
        if (path) {
            sql.call("{call " + config.controlSchema + "." + getProcedureName() + "(?,?,?)}", [path, isDeleteConcepts, jobId])
        } else {
            config.logger.log(LogType.ERROR, "Path doesn't set")
            return false
        }

        return true
    }

    @Override
    String getProcedureName() {
        return config.altDeleteProcName ?: "I2B2_DELETE_CROSS_DATA"
    }

    @Override
    def processData() {
        def data = [
                path           : config.deleteTreePath,
                isDeleteConcept: config?.isDeleteConcepts
        ]
        return data
    }
}
