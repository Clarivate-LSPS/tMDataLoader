package com.thomsonreuters.lsps.transmart.cfg

import groovy.sql.Sql

/**
 * Created by bondarev on 4/3/14.
 */
class Database {
    def config

    Database(dbConfig) {
        config = dbConfig
    }

    void withSql(Closure block) {
        Sql.withInstance(config.jdbcConnectionString, config.username, config.password, config.jdbcDriver, block)
    }

    boolean isPostgresConnection() {
        config.jdbcConnectionString?.startsWith("jdbc:postgresql:")
    }

    boolean isLocalPostgresConnection() {
        isPostgresConnection() && config.jdbcConnectionString?.matches('^jdbc:postgresql:(?://localhost(?::\\d+)?/)?(\\w+)$')
    }
}
