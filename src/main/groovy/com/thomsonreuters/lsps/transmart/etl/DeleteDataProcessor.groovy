package com.thomsonreuters.lsps.transmart.etl

import groovy.sql.Sql

class DeleteDataProcessor extends DataOperationProcessor {

    DeleteDataProcessor(Object conf) {
        super(conf)
    }

    @Override
    def processData() {
        def data = ['id'  : (config.deleteStudyById?.toString()?.toUpperCase() ?: null),
                    'path': (config.deleteStudyByPath ?: null)
        ]
        return data
    }

    @Override
    boolean runStoredProcedures(jobId, Sql sql, data) {
        def trialId = data.id?.toUpperCase()
        def path = data.path?.toString()
        if (trialId || path) {
            sql.call("{call " + config.controlSchema + "." + getProcedureName() + "(?,?,?)}", [trialId, path, jobId])
        } else {
            config.logger.log(LogType.ERROR, "Study ID or Node not defined!")
            return false
        }

        return true
    }

    String getProcedureName() {
        return config.altDeleteProcName ?: "I2B2_DELETE_ALL_DATA"
    }
}
