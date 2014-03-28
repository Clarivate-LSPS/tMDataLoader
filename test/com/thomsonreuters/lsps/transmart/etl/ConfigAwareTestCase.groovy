package com.thomsonreuters.lsps.transmart.etl;

import groovy.sql.Sql;
import groovy.util.GroovyTestCase
import org.junit.Ignore;

/**
 * Created by bondarev on 3/28/14.
 */
@Ignore
public class ConfigAwareTestCase extends GroovyTestCase {
    def connectionSettings = [
            jdbcConnectionString: 'jdbc:oracle:thin:@localhost:1521:ORCL',
            username            : 'tm_cz',
            password            : 'tm_cz',
            jdbcDriver          : 'oracle.jdbc.OracleDriver'
    ]

    @Override
    void setUp() {
        File testConfig = new File('test/TestConfig.groovy')
        if (testConfig.exists()) {
            connectionSettings = new ConfigSlurper().parse(testConfig.toURI().toURL()).db
        }
    }
    private Sql _sql

    Sql getSql() {
        return _sql ?: (_sql = Sql.newInstance(connectionSettings.jdbcConnectionString,
                connectionSettings.password, connectionSettings.username,
                connectionSettings.jdbcDriver))
    }

    def getConfig() {
        [
                logger        : new Logger([isInteractiveMode: true]),
                db            : connectionSettings,
                controlSchema : 'tm_cz',
                securitySymbol: 'N'
        ]
    }
}
