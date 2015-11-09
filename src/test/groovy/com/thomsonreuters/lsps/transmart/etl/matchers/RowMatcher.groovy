package com.thomsonreuters.lsps.transmart.etl.matchers

import groovy.sql.GroovyRowResult
import org.hamcrest.BaseMatcher
import org.hamcrest.Description

import java.sql.Clob

/**
 * Date: 09.11.2015
 * Time: 15:19
 */
class RowMatcher extends BaseMatcher<GroovyRowResult> {
    protected final Map<CharSequence, Object> valueAttrs

    RowMatcher(Map valueAttrs) {
        this.valueAttrs = valueAttrs
    }

    protected static Object normalizeValue(Object value, Class targetClass) {
        if (value instanceof Clob) {
            value = ((Clob)value).getCharacterStream().text
        }
        switch (targetClass) {
            case Boolean:
                if (!(value instanceof Boolean)) {
                    value = !(value.toString().toLowerCase() in ['0', 'n', 'f', 'false'])
                }
                break
            case Double:
                value = value as Double
                break
        }
        return value
    }

    @Override
    boolean matches(Object item) {
        GroovyRowResult record = item as GroovyRowResult
        return valueAttrs.every {
            def targetClass = it.value?.class
            def value = normalizeValue(record[it.key], targetClass)
            if ((targetClass == Double || targetClass == Float || targetClass == BigDecimal) && value != null) {
                Math.abs((Double) it.value - value) < 0.001
            } else {
                it.value == value
            }
        }
    }

    @Override
    void describeTo(Description description) {
        description.appendText(" row with values ").appendValue(valueAttrs)
    }

    @Override
    void describeMismatch(Object item, Description description) {
        GroovyRowResult record = item as GroovyRowResult
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
    }
}
