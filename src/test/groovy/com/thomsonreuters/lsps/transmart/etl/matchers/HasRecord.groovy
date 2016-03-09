package com.thomsonreuters.lsps.transmart.etl.matchers
import com.thomsonreuters.lsps.db.sql.SqlMethods
import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.Description
import org.hamcrest.Matcher

class HasRecord extends BaseMatcher<Sql> {
    private Map<String, Object> keyAttrs
    private String tableName
    private GroovyRowResult record
    private Matcher<? super GroovyRowResult> rowMatcher

    HasRecord(String tableName, Map<String, Object> keyAttrs, Map<String, Object> valueAttrs) {
        this(tableName, keyAttrs, new RowMatcher(valueAttrs))
    }

    HasRecord(String tableName, Map<String, Object> keyAttrs, Matcher<? super GroovyRowResult> rowMatcher) {
        this.tableName = tableName
        this.keyAttrs = keyAttrs
        this.rowMatcher = rowMatcher
    }

    @Override
    boolean matches(Object item) {
        record = SqlMethods.findRecord(item as Sql, keyAttrs, tableName)
        record && rowMatcher.matches(record)
    }

    @Override
    void describeTo(Description description) {
        description.appendText("record ").appendValue(keyAttrs).
                appendText(" in table ").appendValue(tableName).
                appendText(" where: ").appendDescriptionOf(rowMatcher)
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
