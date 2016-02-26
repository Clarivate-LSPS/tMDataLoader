package com.thomsonreuters.lsps.db.core

import com.thomsonreuters.lsps.io.file.TempStorage
import groovy.sql.Sql
import org.codehaus.groovy.control.io.NullWriter

import java.util.regex.Pattern

/**
 * Created by bondarev on 4/3/14.
 */
class Database {
    def config
    DatabaseType databaseType = DatabaseType.Unknown
    String database
    String host
    String controlSchema
    int port = -1

    Database(config) {
        this.config = config.db
        parseJdbcConnectionString()
        if (config.controlSchema) {
            this.controlSchema = config.controlSchema
        } else {
            this.controlSchema = databaseType == DatabaseType.Postgres ? 'tm_dataloader' : 'TM_CZ'
        }
    }

    Database withCredentials(String username, String password) {
        new Database(db: config + [username: username, password: password])
    }

    void truncateTable(Sql sql, String tableName) {
        if (databaseType == DatabaseType.Oracle) {
            tableName = tableName.toUpperCase()
            def row = sql.firstRow(
                    "select table_owner || '.' || table_name from user_synonyms where synonym_name = $tableName")
            if (row) {
                tableName = row.getAt(0)
            }
        }
        sql.execute("truncate table $tableName" as String)
    }

    def withSql(Closure block) {
        Sql sql = null
        try {
            sql = newSql()
            block.call(sql)
        } finally {
            if (sql != null) sql.close()
        }
    }

    private String getSearchPath() {
        def schemas = 'tm_cz, tm_lz, tm_wz, i2b2demodata, i2b2metadata, deapp, fmapp, amapp, pg_temp'
        if (controlSchema.toLowerCase() != 'tm_cz') {
            schemas = "${controlSchema}, ${schemas}"
        }
        return schemas
    }

    Sql newSql() {
        Sql sql = Sql.newInstance(config.jdbcConnectionString, config.username, config.password, config.jdbcDriver)
        if (databaseType == DatabaseType.Postgres) {
            sql.execute("SET SEARCH_PATH=${Sql.expand(searchPath)};")
        }
        return sql
    }

    Process runPsqlCommand(String ... additionalArgs) {
        return runPsqlCommand(null, additionalArgs)
    }

    Process runPsqlCommand(File dir, String ... additionalArgs) {
        def env = System.getenv().entrySet().collect { "${it.key}=${it.value}" }
        env << "PGPASSWORD=${config.password}"
        def cmd = ['psql', '-h', host, '-U', config.username, '-d', database, '-p', port]
        cmd.addAll(additionalArgs)
        return Runtime.runtime.exec(cmd as String[], env as String[], dir)
    }

    private final RE_ORACLE_SPLITTER = Pattern.compile(/^\/\s*$/, Pattern.MULTILINE)

    private File prepareScript(File sqlFile) {
        File tmpFile = TempStorage.instance.createTempFile("script", ".sql")

        String content = sqlFile.text
        tmpFile.withWriter {
            if (databaseType == DatabaseType.Postgres) {
                it.println("set SEARCH_PATH = ${searchPath};")
                it.append(content)
            } else if (databaseType == DatabaseType.Oracle) {
                it.println("ALTER SESSION SET CURRENT_SCHEMA=${controlSchema};")
                it.println('/')
                def parts = RE_ORACLE_SPLITTER.split(content)
                for (def part : parts) {
                    it.println(content)
                    it.println('/')
                }
                it.println("exit SQL.SQLCODE;")
            }
        }
        return tmpFile;
    }

    private class MulticastAppendable implements Appendable {
        private Appendable first;
        private Appendable second;

        MulticastAppendable(Appendable first, Appendable second) {
            this.first = first
            this.second = second
        }

        @Override
        Appendable append(CharSequence csq) throws IOException {
            first.append(csq)
            second.append(csq)
            return this
        }

        @Override
        Appendable append(CharSequence csq, int start, int end) throws IOException {
            first.append(csq, start, end)
            second.append(csq, start, end)
            return this
        }

        @Override
        Appendable append(char c) throws IOException {
            first.append(c)
            second.append(c)
            return this
        }
    }

    Process runScript(File script, boolean showOutput=false) {
        Process runner
        File preparedScript = prepareScript(script)
        switch (databaseType) {
            case DatabaseType.Postgres:
                runner = runPsqlCommand(script.parentFile, '-f', preparedScript.absolutePath)
                break
            case DatabaseType.Oracle:
                def command = "sqlplus -l ${config.username}/${config.password}@${host}:${port}/${database} @${preparedScript.absolutePath}"
                runner = Runtime.runtime.exec(command, new String[0], script.parentFile)
                break
            default:
                throw new UnsupportedOperationException("Can't run script for database: ${databaseType}")
        }

        StringBuffer err = new StringBuffer()
        StringBuffer out = new StringBuffer()
        Appendable stdout = new MulticastAppendable(out, showOutput ? System.out : NullWriter.DEFAULT)
        Appendable stderr = showOutput ? new MulticastAppendable(System.err, err) : err
        runner.consumeProcessOutput(stdout, stderr)

        runner.waitFor()
        if (runner.exitValue() != 0) {
            def msg = err.toString() ?: out.toString()
            throw new RuntimeException(msg)
        }

        if (err.length() > 0) {
            println(err.toString())
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
        def match = config.jdbcConnectionString =~ /^jdbc:oracle:thin:@((?:\w|[-.])+)?(?::(\d+))?[:\/]([\w.]+)$/ ?:
                config.jdbcConnectionString =~ /^jdbc:oracle:thin:@\/\/((?:\w|[-.])+)?(?::(\d+))?\/([\w.]+)$/
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
