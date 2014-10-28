package com.thomsonreuters.lsps.transmart.sql

import groovy.sql.Sql

/**
 * Created by bondarev on 4/3/14.
 */
class Database {
    def config
    DatabaseType databaseType = DatabaseType.Unknown
    String database
    String host
    int port = -1

    Database(config) {
        this.config = config.db
        parseJdbcConnectionString()
    }

    void withSql(Closure block) {
        Sql sql = null
        try {
            sql = newSql()
            block.call(sql)
        } finally {
            if (sql != null) sql.close()
        }
    }

    Sql newSql() {
        Sql sql = Sql.newInstance(config.jdbcConnectionString, config.username, config.password, config.jdbcDriver)
        if (databaseType == DatabaseType.Postgres) {
            def schemas = 'tm_cz, tm_lz, tm_wz, i2b2demodata, i2b2metadata, deapp, pg_temp'
            def controlSchema = config.controlSchema ? config.controlSchema : 'tm_dataloader'
            if (controlSchema.toLowerCase() != 'tm_cz') {
                schemas = "${controlSchema}, ${schemas}"
            }
            sql.execute("SET SEARCH_PATH=${Sql.expand(schemas)};")
        }
        return sql
    }

    Process runPsqlCommand(String ... additionalArgs) {
        def env = System.getenv().entrySet().collect { "${it.key}=${it.value}" }
        env << "PGPASSWORD=${config.password}"
        def cmd = ['psql', '-h', host, '-U', config.username, '-d', database]
        cmd.addAll(additionalArgs)
        return Runtime.runtime.exec(cmd as String[], env as String[])
    }

    Process runScript(File script) {
        Process runner
        switch (databaseType) {
            case DatabaseType.Postgres:
                runner = runPsqlCommand('-f', script.path)
                break
            case DatabaseType.Oracle:
                def command = "sqlplus -l ${config.username}/${config.password}@${host}:${port}/${database} @${script.absolutePath}"
                runner = Runtime.runtime.exec(command)
                break
            default:
                throw new UnsupportedOperationException("Can't run script for database: ${databaseType}")
        }
        runner.waitFor()
        if (runner.exitValue() != 0) {
            throw new RuntimeException(runner.errorStream.text)
        }
        return runner
    }

    private final def parseJdbcConnectionString() {
        if (config.jdbcConnectionString) {
            if (config.jdbcConnectionString.startsWith('jdbc:postgresql:')) {
                parsePostgresJdbcConnectionString()
            } else {
                parseOracleJdbcConnectionString()
            }
        }
    }

    private void parseOracleJdbcConnectionString() {
        def match = config.jdbcConnectionString =~ /^jdbc:oracle:thin:@((?:\w|[-.])+)?(?::(\d+))?:(\w+)$/ ?:
                config.jdbcConnectionString =~ /^jdbc:oracle:thin:@\/\/((?:\w|[-.])+)?(?::(\d+))?\/(\w+)$/
        if (match.size()) {
            databaseType = DatabaseType.Oracle
            host = match[0][1] ?: 'localhost'
            port = match[0][2]?.asType(Integer.class) ?: 1521
            database = match[0][3]
        }
    }

    private void parsePostgresJdbcConnectionString() {
        def match = config.jdbcConnectionString =~ /^jdbc:postgresql:(?:\/\/((?:\w|[-.])+)(?::(\d+))?\/)?(\w+)(?:\?.*)?$/
        if (match) {
            databaseType = DatabaseType.Postgres
            host = match[0][1] ?: 'localhost'
            port = match[0][2]?.asType(Integer.class) ?: 5432
            database = match[0][3]
        }
    }

    boolean isLocal() {
        host == 'localhost' || host == '127.0.0.1'
    }
}
