package com.thomsonreuters.lsps.db.sql

import groovy.sql.GroovyRowResult
import groovy.sql.Sql

/**
 * Created by bondarev on 4/7/14.
 */
@Category(Sql)
class SqlMethods {
    private static Object prepareValue(Object value) {
        if (value.is(null)) {
            return null
        }
        switch (value.class) {
            case GString:
                return value as String
            case Calendar:
                return prepareValue((value as Calendar).time)
            case Date:
                return new java.sql.Date((value as Date).time)
            default:
                return value
        }
    }

    void callProcedure(String procedureName, Object... params) {
        call("{call ${procedureName}(${(['?'] * params.size()).join(',')})}", params.collect { SqlMethods.prepareValue(it) })
    }

    private static String buildInsertCommand(CharSequence tableName, Collection<CharSequence> columns) {
        "insert into ${tableName}(${columns.join(',')}) values (${(['?'] * columns.size()).join(',')})"
    }

    GroovyRowResult findRecord(Map<CharSequence, Object> attrs, CharSequence tableName) {
        List<String> conditions = []
        List<Object> values = []
        attrs.entrySet().each {
            if (!it.value.is(null)) {
                conditions << "${it.key}=?"
                values << SqlMethods.prepareValue(it.value)
            } else {
                conditions << "${it.key} is null"
            }
        }
        firstRow("select * from ${tableName} where ${conditions.join(' and ')}", values)
    }

    long insertRecords(CharSequence tableName, Collection<CharSequence> columns, Closure block) {
        long records = 0
        withBatch(200, buildInsertCommand(tableName, columns)) { st ->
            block.call(st)
            records++
        }
        return records
    }

    def insertRecord(Map<CharSequence, Object> attrs, CharSequence tableName) {
        def columns = attrs.keySet()
        def values = columns.collect { column -> SqlMethods.prepareValue(attrs[column]) }
        executeInsert(buildInsertCommand(tableName, columns), values)
    }
}
