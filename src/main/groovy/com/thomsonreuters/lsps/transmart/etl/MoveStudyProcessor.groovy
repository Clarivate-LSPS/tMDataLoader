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
        def saveSecurity = data['keepSecurity'] ? 'Y' : 'N'
        if (oldPath || newPath) {
            //Check Cross node
            def query = """
                    SELECT count(*)
                        FROM i2b2demodata.observation_fact
                        WHERE concept_cd IN (
                          SELECT concept_cd
                          FROM i2b2demodata.concept_dimension cd
                          WHERE concept_path LIKE ? || '%' ESCAPE '`' AND
                                                                  exists(SELECT 1
                                                                         FROM i2b2metadata.i2b2
                                                                         WHERE c_dimcode = cd.concept_path AND sourcesystem_cd IS NULL)
                        )
                    """
            def res = sql.firstRow(query, [oldPath])
            if (res[0] == 0)
                sql.call("{call " + config.controlSchema + "." + getProcedureName() + "(?,?,?,?)}", [oldPath, newPath, saveSecurity, jobId])
            else
                sql.call("{call " + config.controlSchema + "." + getCrossProcedureName() + "(?,?,?,?)}", [oldPath, newPath, saveSecurity, jobId])
        } else {
            config.logger.log(LogType.ERROR, "Old or new study path is not defined!")
            return false
        }

        return true
    }

    @Override
    String getProcedureName() {
        return "I2B2_MOVE_STUDY_BY_PATH"
    }

    String getCrossProcedureName() {
        return "I2B2_MOVE_CROSS_BY_PATH"
    }

    @Override
    def processData() {
        def data = ['old_path'    : (config.moveStudyOldPath ?: null),
                    'new_path'    : (config.moveStudyNewPath ?: null),
                    'keepSecurity': (config.keepSecurity ?: false)
        ]
        return data
    }
}
