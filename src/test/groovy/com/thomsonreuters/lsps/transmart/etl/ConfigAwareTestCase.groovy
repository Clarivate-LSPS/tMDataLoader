package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.db.core.Database
import com.thomsonreuters.lsps.db.core.DatabaseType
import com.thomsonreuters.lsps.db.sql.SqlMethods
import groovy.sql.Sql
import org.junit.Assume

/**
 * Created by bondarev on 3/28/14.
 */
public trait ConfigAwareTestCase {
    private final Logger logger = Logger.getLogger(getClass())

    def config
    Database _database

    void setUp() {
        URL testConfigUrl = getClass().classLoader.getResource('TestConfig.groovy')
        Assume.assumeTrue("No database config was found. Please, copy src/test/resources/TestConfig.groovy.sample " +
                "to src/test/resources/TestConfig.groovy and set-up your database connection", !testConfigUrl.is(null))
        Logger.setInteractiveMode(true)
        config = new ConfigSlurper().parse(testConfigUrl)
        config.logger = config.logger ?: logger
        _database = TransmartDatabaseFactory.newDatabase(config)
        config.controlSchema = config.controlSchema ?: (_database.databaseType == DatabaseType.Postgres ? 'tm_dataloader' : 'tm_dataloader')
        config.securitySymbol = config.securitySymbol ?: 'N'
    }
    private Sql _db

    File getDbScriptsDir() {
        def databaseType = database.databaseType
        File dir = new File("sql", databaseType.toString().toLowerCase())
        if (databaseType == DatabaseType.Postgres) {
            dir = new File(dir, 'procedures')
        }
        return dir;
    }

    void runScript(String scriptName) {
        File sqlFile = new File(dbScriptsDir, scriptName)
        if (!sqlFile.exists()) {
            throw new RuntimeException("Can't run script '$sqlFile'. It doens't exists")
        }
        database.runScript(sqlFile)
    }

    Sql getSql() {
        return db
    }

    Database getDatabase() {
        return _database
    }

    Sql getDb() {
        if (_db.is(null)) {
            _db = database.newSql()
        }
        return _db
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

    void withErrorLogging(Closure block) {
        try {
            block.call()
        } catch (Exception ex) {
            logger.log(LogType.ERROR, ex)
        }
    }

    void withAudit(String jobName, Closure block) {
        database.withSql { Sql sql ->
            def jobId = -1
            sql.call('{call ' + config.controlSchema + '.cz_start_audit(?,?,?)}',
                    [jobName, config.db.username, Sql.NUMERIC]) { jobId = it }
            block.call(jobId)
            def succeed = true
            sql.eachRow("SELECT * FROM cz_job_audit where job_id=${jobId} order by seq_id") { row ->
                println "-- ${row.step_desc} [${row.step_status} / ${row.records_manipulated} recs / ${row.time_elapsed_secs}s]"
            }
            sql.eachRow("SELECT * FROM cz_job_error where job_id=${jobId} order by seq_id") {
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
