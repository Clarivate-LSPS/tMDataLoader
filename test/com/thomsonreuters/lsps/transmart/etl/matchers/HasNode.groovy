package com.thomsonreuters.lsps.transmart.etl.matchers

import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.CoreMatchers
import org.hamcrest.Description
import org.hamcrest.Matcher

import static org.hamcrest.CoreMatchers.both

/**
 * Created by bondarev on 4/1/14.
 */
class HasNode extends BaseMatcher<Sql> {
    private String conceptPath

    HasNode(String conceptPath) {
        this.conceptPath = conceptPath
    }

    BaseMatcher<Sql> withPatientCount(int patientCount) {
        return both(this).and(new HasNodeWithPatientCount(conceptPath, patientCount))
    }

    @Override
    boolean matches(Object item) {
        def sql = item as Sql
        def attrName = conceptPath.split('\\\\')[-1]
        def result = sql.firstRow('select c_name from i2b2metadata.i2b2 where C_FULLNAME=?', conceptPath)
        if (result?.c_name != attrName) {
            return false;
        }

        def query = sql.rows('select * from i2b2demodata.concept_dimension where CONCEPT_PATH=?', conceptPath)
        return query.size() == 1
    }

    @Override
    void describeTo(Description description) {
        description.appendValue(conceptPath)
    }

    @Override
    void describeMismatch(Object item, Description description) {
        description.appendValue("node was not found")
    }
}
