package com.thomsonreuters.lsps.transmart.cfg

import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.nullValue
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 4/3/14.
 */
class DatabaseTest extends GroovyTestCase {
    private void assertParse(String jdbcConnectionString, Map info) {
        def db = new Database([jdbcConnectionString: jdbcConnectionString])
        if (info.host.is(null)) {
            assertThat(db.host, nullValue())
        } else {
            assertThat(db.host, equalTo(info.host))
        }
        assertThat(db.port, equalTo(info.port))
        if (info.database.is(null)) {
            assertThat(db.database, nullValue())
        } else {
            assertThat(db.database, equalTo(info.database))
        }
    }

    void testLocalPostgresConnection() {
        def db = new Database([jdbcConnectionString: 'jdbc:postgresql:transmart'])
        assertThat(db.isPostgresConnection(), equalTo(true))
        assertThat(db.isLocalPostgresConnection(), equalTo(true))
    }

    void testRemotePostgresConnection() {
        def db = new Database([jdbcConnectionString: 'jdbc:postgresql://server/transmart'])
        assertThat(db.isPostgresConnection(), equalTo(true))
        assertThat(db.isLocalPostgresConnection(), equalTo(false))
    }

    void testLocalOracleConnection() {
        def db = new Database([jdbcConnectionString: 'jdbc:oracle:thin:@:orcl'])
        assertThat(db.isPostgresConnection(), equalTo(false))
        assertThat(db.isLocalPostgresConnection(), equalTo(false))
    }

    void testRemoteOracleConnection() {
        def db = new Database([jdbcConnectionString: 'jdbc:oracle:thin:@server:orcl'])
        assertThat(db.isPostgresConnection(), equalTo(false))
        assertThat(db.isLocalPostgresConnection(), equalTo(false))
    }

    void testItParsesPostgresJdbcConnectionString() {
        assertParse('jdbc:postgresql:malformed url', [host: null, port: -1, database: null])
        assertParse('jdbc:postgresql:transmart', [host: 'localhost', port: 5432, database: 'transmart'])
        assertParse('jdbc:postgresql://server/transmart', [host: 'server', port: 5432, database: 'transmart'])
        assertParse('jdbc:postgresql://server:5433/transmart', [host: 'server', port: 5433, database: 'transmart'])
    }

    void testItParseOracleJdbcConnectionString() {
        assertParse('jdbc:oracle:thin:@malformed url', [host: null, port: -1, database: null])
        assertParse('jdbc:oracle:thin:@:orcl', [host: 'localhost', port: 1521, database: 'orcl'])
        assertParse('jdbc:oracle:thin:@server:orcl', [host: 'server', port: 1521, database: 'orcl'])
        assertParse('jdbc:oracle:thin:@server:1522:orcl', [host: 'server', port: 1522, database: 'orcl'])
        assertParse('jdbc:oracle:thin:@:1522:orcl', [host: 'localhost', port: 1522, database: 'orcl'])
        assertParse('jdbc:oracle:thin:@///orcl', [host: 'localhost', port: 1521, database: 'orcl'])
        assertParse('jdbc:oracle:thin:@//server/orcl', [host: 'server', port: 1521, database: 'orcl'])
        assertParse('jdbc:oracle:thin:@//:1522/orcl', [host: 'localhost', port: 1522, database: 'orcl'])
        assertParse('jdbc:oracle:thin:@//server:1522/orcl', [host: 'server', port: 1522, database: 'orcl'])
    }
}
