package com.thomsonreuters.lsps.transmart.sql

import com.thomsonreuters.lsps.transmart.etl.ConfigAwareTestCase
import groovy.sql.Sql

import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.hamcrest.CoreMatchers.nullValue
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 4/3/14.
 */
class DatabaseTest extends ConfigAwareTestCase {
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
        assertThat(db.databaseType, equalTo(DatabaseType.Postgres))
        assertThat(db.isLocal(), equalTo(true))
    }

    void testRemotePostgresConnection() {
        def db = new Database([jdbcConnectionString: 'jdbc:postgresql://server/transmart'])
        assertThat(db.databaseType, equalTo(DatabaseType.Postgres))
        assertThat(db.isLocal(), equalTo(false))
    }

    void testLocalOracleConnection() {
        def db = new Database([jdbcConnectionString: 'jdbc:oracle:thin:@:orcl'])
        assertThat(db.databaseType, equalTo(DatabaseType.Oracle))
        assertThat(db.isLocal(), equalTo(true))
    }

    void testRemoteOracleConnection() {
        def db = new Database([jdbcConnectionString: 'jdbc:oracle:thin:@server:orcl'])
        assertThat(db.databaseType, equalTo(DatabaseType.Oracle))
        assertThat(db.isLocal(), equalTo(false))
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

    void testItRunScript() {
        File sampleScript = File.createTempFile('sample', '.sql')
        sampleScript.deleteOnExit()
        def db = new Database(config.db)
        db.withSql { Sql sql->
            sql.execute('delete from tm_lz.lt_src_mrna_subj_samp_map where trial_name = ?', 'TEST SCRIPT LOAD')
        }
        sampleScript.write('insert into tm_lz.lt_src_mrna_subj_samp_map (trial_name) values (\'TEST SCRIPT LOAD\');\n')
        if (db.databaseType == DatabaseType.Oracle) {
            sampleScript.append('exit;')
        }
        println db.runScript(sampleScript).errorStream.text
        db.withSql { Sql sql->
            def result = sql.firstRow('select * from tm_lz.lt_src_mrna_subj_samp_map where trial_name = ?',
                    'TEST SCRIPT LOAD')
            assertThat(result, notNullValue())
        }
    }
}
