package com.thomsonreuters.lsps.transmart.etl;

import groovy.sql.Sql;
import groovy.util.GroovyTestCase
import org.junit.Assume
import org.junit.Ignore;

/**
 * Created by bondarev on 3/28/14.
 */
@Ignore
public class ConfigAwareTestCase extends GroovyTestCase {
    def connectionSettings

    @Override
    void setUp() {
        File testConfig = new File('test/TestConfig.groovy')
        Assume.assumeTrue("No database config was found. Please, copy test/TestConfig.groovy.sample " +
                "to test/TestConfig.groovy and set-up your database connection", testConfig.exists())
        connectionSettings = new ConfigSlurper().parse(testConfig.toURI().toURL()).db
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
