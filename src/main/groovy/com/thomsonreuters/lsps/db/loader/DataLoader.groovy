package com.thomsonreuters.lsps.db.loader

import com.thomsonreuters.lsps.db.core.Database
import com.thomsonreuters.lsps.db.core.DatabaseType

/**
 * Created by bondarev on 4/8/14.
 */
abstract class DataLoader {
    Database database
    CharSequence tableName
    Collection<? extends CharSequence> columnNames

    static long start(Database database, CharSequence tableName, Collection<CharSequence> columnNames, Closure block) {
        columnNames = columnNames.collect { "\"${it}\"" }
        //FIXME: find better solution
        if (database.databaseType == DatabaseType.Postgres) {
            columnNames = columnNames*.toLowerCase()
        }
        DataLoader dataLoader
        if (database.databaseType == DatabaseType.Postgres) {
            dataLoader = new CsvDataLoader(database: database, tableName: tableName, columnNames: columnNames)
        } else {
            dataLoader = new SqlDataLoader(database: database, tableName: tableName, columnNames: columnNames)
        }
        return dataLoader.withBatch(block)
    }

    abstract long withBatch(Closure block);
}
