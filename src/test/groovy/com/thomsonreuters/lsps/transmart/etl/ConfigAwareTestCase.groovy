package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.sql.Database
import com.thomsonreuters.lsps.transmart.sql.DatabaseType
import com.thomsonreuters.lsps.transmart.sql.SqlMethods
import groovy.sql.Sql
import org.junit.Assume

/**
 * Created by bondarev on 3/28/14.
 */
public abstract class ConfigAwareTestCase extends GroovyTestCase {
    def config
    Database database

    @Override
    void setUp() {
        URL testConfigUrl = getClass().classLoader.getResource('TestConfig.groovy')
        Assume.assumeTrue("No database config was found. Please, copy src/test/resources/TestConfig.groovy.sample " +
                "to src/test/resources/TestConfig.groovy and set-up your database connection", !testConfigUrl.is(null))
        config = new ConfigSlurper().parse(testConfigUrl)
        config.logger = config.logger ?: new Logger([isInteractiveMode: true])
        config.controlSchema = config.controlSchema ?: 'tm_cz'
        config.loadSchema = config.loadSchema ?: 'tm_lz'
        config.securitySymbol = config.securitySymbol ?: 'N'
    }
    private Sql _db

    File getDbScriptsDir() {
        def databaseType = getDatabase().databaseType
        File dir = new File("sql", databaseType.toString().toLowerCase())
        if (databaseType == DatabaseType.Postgres) {
            dir = new File(dir, 'procedures')
        }
        return dir;
    }

    void runScript(String scriptName) {
        File sqlFile = dbScriptsDir.listFiles().find { it.name.endsWith(scriptName) }
        if (database.databaseType == DatabaseType.Postgres) {
            File tmpDir = new File("tmp")
            tmpDir.mkdirs();
            File tmpFile = File.createTempFile("script", ".sql", tmpDir)
            tmpFile.deleteOnExit();
            tmpFile.setReadable(true, false);
            tmpFile.setWritable(true, false);
            tmpFile.setExecutable(true, false);
            tmpFile.withWriter {
                it.println("set SEARCH_PATH = ${config.controlSchema}, ${config.loadSchema}, tm_wz, i2b2demodata, i2b2metadata, deapp, pg_temp;")
                it.append(sqlFile.text)
            }
            sqlFile = tmpFile;
        }
        def p = database.runScript(sqlFile)
        String errors = p.err.text
        if (errors) {
            println(errors)
        }
    }

    Sql getSql() {
        return db
    }

    Database getDatabase() {
        database ?: (database = new Database(config.db))
    }

    Sql getDb() {
        return _db ?: (_db = Sql.newInstance(config.db.jdbcConnectionString,
                config.db.password, config.db.username,
                config.db.jdbcDriver))
    }

    void callProcedure(String procedureName, Object... params) {
        SqlMethods.callProcedure(db, procedureName, params)
    }

    void insertIfNotExists(String tableName, Map data) {
        def columns = data.keySet()
        def values = columns.collect { data[it] }
        if (!db.firstRow("select * from ${tableName} where ${columns.collect { "${it}=?" }.join(' and ')}" as String, values as List)) {
            db.executeInsert("insert into ${tableName}(${columns.join(', ')}) values (${(['?'] * columns.size()).join(',')})" as String, values as List)
        }
    }

    void withAudit(String jobName, Closure block) {
        database.withSql { Sql sql ->
            def jobId = -1
            sql.call('{call ' + config.controlSchema + '.cz_start_audit(?,?,?)}',
                    [jobName, config.db.username, Sql.NUMERIC]) { jobId = it }
            block.call(jobId)
            def succeed = true
            sql.eachRow("SELECT * FROM " + config.controlSchema + ".cz_job_audit where job_id=${jobId} order by seq_id") { row ->
                println "-- ${row.step_desc} [${row.step_status} / ${row.records_manipulated} recs / ${row.time_elapsed_secs}s]"
            }
            sql.eachRow("SELECT * FROM " + config.controlSchema + ".cz_job_error where job_id=${jobId} order by seq_id") {
                println "${it.error_message} / ${it.error_stack} / ${it.error_backtrace}"
                succeed = false
            }
            if (succeed) {
                sql.call("{call " + config.controlSchema + ".cz_end_audit(?,?)}", [jobId, 'SUCCESS'])
            } else {
                sql.call("{call " + config.controlSchema + ".cz_end_audit(?,?)}", [jobId, 'FAIL'])
            }
        }
    }
}
