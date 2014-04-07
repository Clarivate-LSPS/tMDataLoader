package com.thomsonreuters.lsps.transmart.sql

import groovy.sql.Sql

/**
 * Created by bondarev on 4/7/14.
 */
class SqlMethods {
    static void callProcedure(Sql sql, String procedureName, Object... params) {
        sql.call("{call ${procedureName}(${(['?'] * params.size()).join(',')})}", params.collect {
            if (it instanceof GString) {
                it as String
            } else {
                it
            }
        })
    }
}
