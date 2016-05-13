package com.thomsonreuters.lsps.utils

import com.thomsonreuters.lsps.db.core.Database
import com.thomsonreuters.lsps.db.core.DatabaseType
import org.springframework.jdbc.support.lob.DefaultLobHandler
import org.springframework.jdbc.support.lob.LobHandler

/**
 * Date: 13-May-16
 * Time: 17:15
 */
@Category(Database)
class DatabaseExtensions {
    private static final LobHandler pgLobHandler = new DefaultLobHandler(wrapAsLob: true)
    private static final LobHandler defaultLobHandler = new DefaultLobHandler()

    public LobHandler getLobHandler() {
        databaseType == DatabaseType.Postgres ? pgLobHandler : defaultLobHandler
    }
}
