package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.sql.SqlMethods
import groovy.sql.Sql

/**
 * Created by bondarev on 4/21/14.
 */
class SqlDataLoader extends DataLoader {
    long withBatch(Closure block) {
        database.withSql { Sql sql ->
            SqlMethods.insertRecords(sql, tableName, columnNames, block)
        }
    }
}
