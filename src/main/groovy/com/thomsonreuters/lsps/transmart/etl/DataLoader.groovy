package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.sql.Database
import com.thomsonreuters.lsps.transmart.sql.SqlMethods
import groovy.sql.Sql

/**
 * Created by bondarev on 4/8/14.
 */
class DataLoader {
    Database database
    CharSequence tableName
    Collection<CharSequence> columnNames

    DataLoader(Database database, CharSequence tableName, Collection<CharSequence> columnNames) {
        this.database = database
        this.tableName = tableName
        this.columnNames = columnNames
    }

    static def start(Database database, CharSequence tableName, Collection<CharSequence> columnNames, Closure block) {
        new DataLoader(database, tableName, columnNames).withBatch(block)
    }

    def withBatch(Closure block) {
        database.withSql { Sql sql->
            SqlMethods.insertRecords(sql, tableName, columnNames, block)
        }
    }
}
