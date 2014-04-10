package com.thomsonreuters.lsps.transmart.etl.matchers

import com.thomsonreuters.lsps.transmart.sql.SqlMethods
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.Description

import java.sql.Clob

/**
 * Created by bondarev on 4/8/14.
 */
class HasRecord extends BaseMatcher<Sql> {
    private Map<CharSequence, Object> keyAttrs
    private Map<CharSequence, Object> valueAttrs
    private CharSequence tableName
    private GroovyRowResult record

    def HasRecord(CharSequence tableName, Map<CharSequence, Object> keyAttrs, Map<CharSequence, Object> valueAttrs) {
        this.tableName = tableName
        this.keyAttrs = keyAttrs
        this.valueAttrs = valueAttrs
    }

    private Object normalizeValue(Object value) {
        if (value instanceof Clob) {
            return  ((Clob)value).getCharacterStream().text
        } else {
            return value
        }
    }

    @Override
    boolean matches(Object item) {
        record = SqlMethods.findRecord(item as Sql, keyAttrs, tableName)
        valueAttrs.every { it.value == normalizeValue(record[it.key]) }
    }

    @Override
    void describeTo(Description description) {
        description.appendText("record ").appendValue(keyAttrs).
                appendText(" in table ").appendValue(tableName)
        if (valueAttrs) {
            description.appendText(" with values ").appendValue(valueAttrs)
        }
    }

    @Override
    void describeMismatch(Object item, Description description) {
        if (record) {
            description.appendText("differs by: ")
            description.appendValueList("(", ", ", ")",
                    valueAttrs.findAll { it.value != record[it.key] }.
                            collect { "${it.key}=${normalizeValue(record[it.key])}" })
        } else {
            description.appendText("no record ").appendValue(keyAttrs).
                    appendText(' found in table ').appendValue(tableName)
        }
    }
}
