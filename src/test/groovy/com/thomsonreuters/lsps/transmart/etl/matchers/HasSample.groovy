package com.thomsonreuters.lsps.transmart.etl.matchers

import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.Description

/**
 * Created by transmart on 4/2/14.
 */
class HasSample extends BaseMatcher<Sql> {
    String studyId
    String sampleId

    HasSample(String studyId, String sampleId) {
        this.studyId = studyId
        this.sampleId = sampleId
    }

    @Override
    boolean matches(sql) {
        def sample = sql.firstRow('select * from deapp.de_subject_sample_mapping where trial_name = ? and sample_cd = ?',
                studyId, sampleId)
        return !sample.is(null)
    }

    @Override
    void describeTo(Description description) {
        description.appendText('has sample ').appendValue("${studyId}:${sampleId}")
    }

    @Override
    void describeMismatch(Object item, Description description) {
        description.appendText("sample ").appendValue("${studyId}:${sampleId}").appendText(" was not found")
    }
}
