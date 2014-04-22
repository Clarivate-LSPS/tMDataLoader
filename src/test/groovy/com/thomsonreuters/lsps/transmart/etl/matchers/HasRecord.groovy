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

    private static Object normalizeValue(Object value, Class targetClass) {
        if (value instanceof Clob) {
            value = ((Clob)value).getCharacterStream().text
        }
        switch (targetClass) {
            case Boolean:
                if (!(value instanceof Boolean)) {
                    value = !(value.toString().toLowerCase() in ['0', 'n', 'f', 'false'])
                }
                break
        }
        return value
    }

    @Override
    boolean matches(Object item) {
        record = SqlMethods.findRecord(item as Sql, keyAttrs, tableName)
        record && valueAttrs.every { it.value == normalizeValue(record[it.key], it.value?.class) }
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
            boolean first = true
            valueAttrs.each {
                def actualValue = normalizeValue(record[it.key], it.value?.class)
                if (it.value == actualValue) {
                    return;
                }
                if (!first) {
                    description.appendText(', ')
                } else {
                    first = false
                }
                description.appendText("${it.key} (").
                        appendValue(it.value).appendText('!=').appendValue(actualValue).
                        appendText(")")
            }
        } else {
            description.appendText("no record ").appendValue(keyAttrs).
                    appendText(' found in table ').appendValue(tableName)
        }
    }
}
