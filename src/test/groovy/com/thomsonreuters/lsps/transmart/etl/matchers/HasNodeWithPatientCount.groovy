package com.thomsonreuters.lsps.transmart.etl.matchers

import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.Description

/**
 * Created by bondarev on 4/1/14.
 */
class HasNodeWithPatientCount extends BaseMatcher<Sql> {
    private String conceptPath
    private int expectedPatientCount
    private def patientCount

    HasNodeWithPatientCount(String conceptPath, int patientCount) {
        this.conceptPath = conceptPath
        this.expectedPatientCount = patientCount
    }

    @Override
    boolean matches(Object item) {
        def sql = item as Sql
        def result = sql.firstRow('select patient_count from i2b2demodata.concept_counts where CONCEPT_PATH=?',
                conceptPath)
        this.patientCount = result?.patient_count
        return this.patientCount == expectedPatientCount
    }

    @Override
    void describeTo(Description description) {
        description.appendText('node ').appendValue(conceptPath).appendText(' with patient count ').
                appendValue(expectedPatientCount)
    }

    @Override
    void describeMismatch(Object item, Description description) {
        if (patientCount.is(null)) {
            description.appendText('node ').appendValue(conceptPath).appendText(' was not found')
        } else {
            description.appendText('was ').appendValue(patientCount).appendText(' patients')
        }
    }
}
