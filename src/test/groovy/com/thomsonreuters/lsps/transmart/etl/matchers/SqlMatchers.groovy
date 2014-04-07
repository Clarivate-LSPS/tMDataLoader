package com.thomsonreuters.lsps.transmart.etl.matchers

/**
 * Created by bondarev on 4/1/14.
 */
class SqlMatchers {
    public static HasNode hasNode(String conceptPath) {
        return new HasNode(conceptPath)
    }

    public static HasPatient hasPatient(String subjectId) {
        return new HasPatient(subjectId)
    }

    public static HasSample hasSample(Map<String, Object> props = [:], String studyId, String sampleId) {
        return new HasSample(props, studyId, sampleId)
    }
}
