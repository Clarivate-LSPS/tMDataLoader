package com.thomsonreuters.lsps.transmart.etl.matchers

import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.Description

/**
 * Created by transmart on 4/2/14.
 */
class HasSample extends BaseMatcher<Sql> {
    private sample
    String studyId
    String sampleId
    Map<String, Object> props

    HasSample(Map<String, Object> props = [:], String studyId, String sampleId) {
        this.studyId = studyId
        this.sampleId = sampleId
        this.props = props
    }

    @Override
    boolean matches(sql) {
        sample = sql.firstRow('select * from deapp.de_subject_sample_mapping where trial_name = ? and sample_cd = ?',
                studyId.toUpperCase(), sampleId)
        return !sample.is(null) && props.keySet().every { props[it] == sample.getAt(it) }
    }

    @Override
    void describeTo(Description description) {
        description.appendText('has sample ').appendValue("${studyId}:${sampleId}")
        if (props) {
            description.appendText(' with props: ').appendValue(props)
        }
    }

    @Override
    void describeMismatch(Object item, Description description) {
        if (sample) {
            description.appendText("differs by following props: ")
            props.entrySet().each {
                if (it.value != sample.getAt(it.key)) {
                    description.appendText(it.key).appendText('!=').appendValue(sample.getAt(it.key))
                }
            }
        } else {
            description.appendText("sample ").appendValue("${studyId}:${sampleId}").appendText(" was not found")
        }
    }
}
