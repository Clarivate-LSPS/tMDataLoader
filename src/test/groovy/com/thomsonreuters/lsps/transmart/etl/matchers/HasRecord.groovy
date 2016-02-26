package com.thomsonreuters.lsps.transmart.etl.matchers
import com.thomsonreuters.lsps.db.sql.SqlMethods
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.Description

class HasRecord extends BaseMatcher<Sql> {
    private Map<CharSequence, Object> keyAttrs
    private CharSequence tableName
    private GroovyRowResult record
    private RowMatcher rowMatcher

    def HasRecord(CharSequence tableName, Map<CharSequence, Object> keyAttrs, Map<CharSequence, Object> valueAttrs) {
        this.tableName = tableName
        this.keyAttrs = keyAttrs
        this.rowMatcher = new RowMatcher(valueAttrs)
    }

    @Override
    boolean matches(Object item) {
        record = SqlMethods.findRecord(item as Sql, keyAttrs, tableName)
        record && rowMatcher.matches(record)
    }

    @Override
    void describeTo(Description description) {
        description.appendText("record ").appendValue(keyAttrs).
                appendText(" in table ").appendValue(tableName)
        if (rowMatcher.valueAttrs) {
            description.appendText(" with values ").appendValue(rowMatcher.valueAttrs)
        }
    }

    @Override
    void describeMismatch(Object item, Description description) {
        if (record) {
            rowMatcher.describeMismatch(record, description)
        } else {
            description.appendText("no record ").appendValue(keyAttrs).
                    appendText(' found in table ').appendValue(tableName)
        }
    }
}
