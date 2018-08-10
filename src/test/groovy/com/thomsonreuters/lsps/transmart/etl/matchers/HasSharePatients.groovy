package com.thomsonreuters.lsps.transmart.etl.matchers

import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.Description

class HasSharePatients extends BaseMatcher<Sql> {
    private String sharePatientId
    private List<String> patients
    private Integer cnt
    private Integer expectedCount


    HasSharePatients(String sharePatientId, List<String> patients) {
        this.sharePatientId = sharePatientId
        this.patients = patients
    }

    HasSharePatients(String sharePatientId, List<String> patients, expectedCount) {
        this.sharePatientId = sharePatientId
        this.patients = patients
        this.expectedCount = expectedCount
    }

    @Override
    boolean matches(Object item) {
        def sql = item as Sql
        def checkPatient = sql.firstRow("""
                SELECT count(*) as cnt from 
                  i2b2demodata.patient_mapping  pm
                  inner JOIN
                  i2b2demodata.patient_dimension pd
                  ON pm.patient_num = pd.patient_num
                  where pd.sourcesystem_cd in (${patients.collect { '?' }.join(',')})                                                
            """, patients.collect { sharePatientId + ':' + it })
        cnt = checkPatient.cnt

        if (expectedCount) {
            if (cnt == expectedCount) return true
        } else if (cnt == patients.size()) return true

        return false
    }

    @Override
    void describeTo(Description description) {
        if (expectedCount)
            description.appendText("expected ${expectedCount} patients")
        else
            description.appendText('patients ')
                    .appendList('(', ', ', ')', patients)
                    .appendText(' exist')
    }

    @Override
    void describeMismatch(Object item, Description description) {
        description.appendText("found ${cnt} patients instead of ${(expectedCount ?: patients.size())}")
    }
}
