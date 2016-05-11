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
    protected final Map<String, Object> valueAttrs

    RowMatcher(Map valueAttrs) {
        this.valueAttrs = valueAttrs
    }

    protected static Object normalizeValue(Object value, Class targetClass) {
        if (value instanceof Clob) {
            value = ((Clob) value).getCharacterStream().text
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

    private static boolean matchValue(actualValue, expectedValue) {
        def targetClass = expectedValue?.class
        if (targetClass && Closure.isAssignableFrom(targetClass)) {
            return ((Closure) expectedValue).call(normalizeValue(actualValue, String))
        }
        actualValue = normalizeValue(actualValue, targetClass)
        if ((targetClass == Double || targetClass == Float || targetClass == BigDecimal) && actualValue != null) {
            Math.abs((Double) expectedValue - actualValue) < 0.001
        } else {
            expectedValue == actualValue
        }
    }

    @Override
    boolean matches(Object item) {
        GroovyRowResult record = item as GroovyRowResult
        return valueAttrs.every { matchValue(record[it.key], it.value) }
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
            if (matchValue(record[it.key], it.value)) {
                return
            }
            if (!first) {
                description.appendText(', ')
            } else {
                first = false
            }
            description.appendText("${it.key} (")
            if (it.value instanceof Closure) {
                def mismatchDescription = 'not matches validation block'
                description.appendValue(normalizeValue(record[it.key], String)).appendText(': ').
                        appendText(mismatchDescription)
            } else {
                description.appendValue(it.value).
                        appendText('!=').appendValue(normalizeValue(record[it.key], it.value?.class)).
                        appendText(")")
            }

        }
    }
}
