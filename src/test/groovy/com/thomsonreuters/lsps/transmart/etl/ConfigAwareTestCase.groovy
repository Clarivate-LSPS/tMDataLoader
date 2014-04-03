package com.thomsonreuters.lsps.transmart.etl

import groovy.sql.Sql
import org.junit.Assume

/**
 * Created by bondarev on 3/28/14.
 */
public abstract class ConfigAwareTestCase extends GroovyTestCase {
    def connectionSettings

    @Override
    void setUp() {
        URL testConfigUrl = getClass().classLoader.getResource('TestConfig.groovy')
        Assume.assumeTrue("No database config was found. Please, copy src/test/resources/TestConfig.groovy.sample " +
                "to src/test/resources/TestConfig.groovy and set-up your database connection", !testConfigUrl.is(null))
        connectionSettings = new ConfigSlurper().parse(testConfigUrl).db
    }
    private Sql _sql

    Sql getSql() {
        return _sql ?: (_sql = Sql.newInstance(connectionSettings.jdbcConnectionString,
                connectionSettings.password, connectionSettings.username,
                connectionSettings.jdbcDriver))
    }

    Sql getDb() {
        return sql
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
