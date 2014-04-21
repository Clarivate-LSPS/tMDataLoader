package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.sql.Database
import com.thomsonreuters.lsps.transmart.sql.DatabaseType

/**
 * Created by bondarev on 4/8/14.
 */
abstract class DataLoader {
    Database database
    CharSequence tableName
    Collection<CharSequence> columnNames

    static def start(Database database, CharSequence tableName, Collection<CharSequence> columnNames, Closure block) {
        //FIXME: find better solution
        if (database.databaseType == DatabaseType.Postgres) {
            columnNames = columnNames*.toLowerCase()
        }
        if (database.databaseType == DatabaseType.Postgres) {
            new CsvDataLoader(database: database, tableName: tableName, columnNames: columnNames).withBatch(block)
        } else {
            new SqlDataLoader(database: database, tableName: tableName, columnNames: columnNames).withBatch(block)
        }
    }

    abstract def withBatch(Closure block);
}
