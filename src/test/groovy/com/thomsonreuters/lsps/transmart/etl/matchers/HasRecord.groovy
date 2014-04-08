package com.thomsonreuters.lsps.transmart.etl.matchers

import com.thomsonreuters.lsps.transmart.sql.SqlMethods
import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.Description

/**
 * Created by bondarev on 4/8/14.
 */
class HasRecord extends BaseMatcher<Sql> {
    private Map<CharSequence, Object> attrs
    private CharSequence tableName

    def HasRecord(Map<CharSequence, Object> attrs, CharSequence tableName) {
        this.attrs = attrs
        this.tableName = tableName
    }

    @Override
    boolean matches(Object item) {
        SqlMethods.findRecord(item as Sql, attrs, tableName)
    }

    @Override
    void describeTo(Description description) {
        description.appendText("record ").appendValue(attrs).
                appendText(" in table ").appendValue(tableName)
    }

    @Override
    void describeMismatch(Object item, Description description) {
        description.appendText("no record ").appendValue(attrs).
                appendText(' found in table ').appendValue(tableName)
    }
}
