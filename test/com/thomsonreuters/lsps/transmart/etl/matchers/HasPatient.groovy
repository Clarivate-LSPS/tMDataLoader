package com.thomsonreuters.lsps.transmart.etl.matchers

/**
 * Created by bondarev on 4/1/14.
 */
class HasPatient {
    private String subjectId

    HasPatient(String subjectId) {
        this.subjectId = subjectId
    }

    HasPatientInTrial inTrial(String trialId) {
        return new HasPatientInTrial(trialId, subjectId)
    }
}
