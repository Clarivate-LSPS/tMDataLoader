package com.thomsonreuters.lsps.transmart.sql

import groovy.sql.GroovyRowResult
import groovy.sql.Sql

/**
 * Created by bondarev on 4/7/14.
 */
@Category(Sql)
class SqlMethods {
    private static Object prepareValue(Object value) {
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
        call("{call ${procedureName}(${(['?'] * params.size()).join(',')})}", params.collect { prepareValue(it) })
    }

    private static String buildInsertCommand(CharSequence tableName, Collection<CharSequence> columns) {
        "insert into ${tableName}(${columns.join(',')}) values (${(['?'] * columns.size()).join(',')})"
    }

    GroovyRowResult findRecord(Map<CharSequence, Object> attrs, CharSequence tableName) {
        def columns = attrs.keySet()
        firstRow("select * from ${tableName} where ${columns.collect { "${it}=?" }.join(' and ')}",
                columns.collect { prepareValue(attrs[it]) })
    }

    def insertRecords(CharSequence tableName, Collection<CharSequence> columns, Closure block) {
        withBatch(buildInsertCommand(tableName, columns)) { st->
            block.call(st)
        }
    }

    def insertRecord(Map<CharSequence, Object> attrs, CharSequence tableName) {
        def columns = attrs.keySet()
        def values = columns.collect { column -> prepareValue(attrs[column]) }
        executeInsert(buildInsertCommand(tableName, columns), values)
    }
}
