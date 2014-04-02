package com.thomsonreuters.lsps.transmart.etl

import groovy.sql.Sql

class DeleteDataProcessor extends DataOperationProcessor {

    public DeleteDataProcessor(Object conf) {
        super(conf);
    }

    @Override
    public def processData() {
        def data = ['id': (config.deleteStudyByIdValue ?: null),
                'path': (config.deleteStudyByPathValue ?: null)
        ]
        return data;
    }

    @Override
    public boolean runStoredProcedures(jobId, Sql sql, data) {
        def trialId = data['id'].toString()
        def path = data['path'].toString()
        if (trialId || path) {
            sql.call("{call " + config.controlSchema + "." + getProcedureName() + "(?,?,?)}", [trialId, path, jobId])
            //sql.rows("SELECT tm_cz.i2b2_load_clinical_data(?,?,?,?,?)", [ studyId, studyNode, config.securitySymbol, 'N', jobId ])
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
