package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.db.core.Database
import com.thomsonreuters.lsps.db.core.DatabaseType

/**
 * Date: 26-Feb-16
 * Time: 15:05
 */
class TransmartDatabaseFactory {
    static Database newDatabase(config) {
        Database database = new Database(config.db)
        def controlSchema
        if (config.controlSchema) {
            controlSchema = config.controlSchema
        } else {
            controlSchema = database.databaseType == DatabaseType.Postgres ? 'tm_dataloader' : 'tm_dataloader'
        }
        database.schema = controlSchema
        if (database.databaseType == DatabaseType.Postgres) {
            def searchPath = 'tm_cz, tm_lz, tm_wz, i2b2demodata, i2b2metadata, deapp, fmapp, amapp, pg_temp'
            if (controlSchema.toLowerCase() != 'tm_cz') {
                searchPath = "${controlSchema}, ${searchPath}"
            }
            database.searchPath = searchPath
        }
        database
    }
}
