package com.thomsonreuters.lsps.transmart.etl

import groovy.sql.Sql

class DeleteCrossProcessor extends DataOperationProcessor {
    DeleteCrossProcessor(Object conf) {
        super(conf)
    }

    @Override
    boolean runStoredProcedures(Object jobId, Sql sql, data) {
        String path = data.path?.toString()
        Integer isDeleteConcepts = data.isDeleteConcepts ? 1 : 0
        String conceptCD = data?.conceptCD
        if (path || conceptCD) {
            sql.call("{call " + config.controlSchema + "." + getProcedureName() + "(?,?,?,?)}", [path, conceptCD, isDeleteConcepts, jobId])
        } else {
            config.logger.log(LogType.ERROR, "Path and concept_cd don't set")
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
                path            : config?.deleteTreePath,
                isDeleteConcepts: config?.isDeleteConcepts,
                conceptCD       : config?.deleteConceptCD
        ]
        return data
    }
}
