package com.thomsonreuters.lsps.transmart.etl

import groovy.sql.Sql

class DeleteDataProcessor extends DataOperationProcessor {

    public DeleteDataProcessor(Object conf) {
        super(conf);
    }

    @Override
    public def processData() {
        def data = ['id'  : (config.deleteStudyByIdValue ?: null),
                    'path': (config.deleteStudyByPathValue ?: null),
                    'ds'  : (config.deleteSecurity ? 'Y' : 'N')
        ]
        return data;
    }

    @Override
    public boolean runStoredProcedures(jobId, Sql sql, data) {
        def trialId = data.id?.toString()?.toUpperCase()
        def path = data.path?.toString()
        def deleteSecurity = data.ds
        if (trialId || path) {
            sql.call("{call " + config.controlSchema + "." + getProcedureName() + "(?,?,?,?)}", [trialId, path, deleteSecurity, jobId])
        } else {
            config.logger.log(LogType.ERROR, "Study ID or Node not defined!")
            return false;
        }

        return true;
    }

    public String getProcedureName() {
        return config.altDeleteProcName ?: "I2B2_DELETE_ALL_DATA"
    }
}
