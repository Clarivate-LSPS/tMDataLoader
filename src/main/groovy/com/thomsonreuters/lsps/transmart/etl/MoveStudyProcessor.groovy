package com.thomsonreuters.lsps.transmart.etl

import groovy.sql.Sql

class MoveStudyProcessor extends DataOperationProcessor {

    MoveStudyProcessor(Object conf) {
        super(conf)
    }

    @Override
    boolean runStoredProcedures(jobId, Sql sql, data) {
        def oldPath = data['old_path'].toString()
        def newPath = data['new_path'].toString()
        def saveSecurity = data['keepSecurity']?'Y':'N'
        if (oldPath || newPath) {
            sql.call("{call " + config.controlSchema + "." + getProcedureName() + "(?,?,?,?)}", [oldPath, newPath, saveSecurity, jobId])
        } else {
            config.logger.log(LogType.ERROR, "Old or new study path is not defined!")
            return false;
        }

        return true;
    }

    @Override
    public String getProcedureName() {
        return "I2B2_MOVE_STUDY_BY_PATH"
    }

    @Override
    public def processData() {
        def data = ['old_path': (config.moveStudyOldPath ?: null),
                    'new_path': (config.moveStudyNewPath ?: null) ,
                    'keepSecurity': (config.keepSecurity ?: false)
        ]
        return data;
    }
}
