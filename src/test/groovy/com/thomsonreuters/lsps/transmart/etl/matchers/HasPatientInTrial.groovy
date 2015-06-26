package com.thomsonreuters.lsps.transmart.etl.matchers

import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.Description

/**
 * Created by bondarev on 4/1/14.
 */
class HasPatientInTrial extends BaseMatcher<Sql> {
    private String trialId
    private String subjectId

    HasPatientInTrial(String trialId, String subjectId) {
        this.trialId = trialId
        this.subjectId = subjectId
    }

    @Override
    boolean matches(Object item) {
        Sql sql = item as Sql
        def query = sql.rows('select * from i2b2demodata.patient_trial where patient_num in ' +
                '(select patient_num from i2b2demodata.patient_dimension where SOURCESYSTEM_CD=?)',
                trialId.toUpperCase() + ':' + subjectId)
        return query.size() == 1
    }

    @Override
    void describeTo(Description description) {
        description.appendValue(subjectId).appendText(" in ").appendValue(trialId)
    }

    @Override
    void describeMismatch(Object item, Description description) {
        description.appendText("patient ").appendValue(subjectId).appendText(" was not found in ").appendValue(trialId)
    }
}
