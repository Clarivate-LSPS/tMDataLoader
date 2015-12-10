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

    public static HasRecord hasPlatform(Map<String, Object> props = [:], String platform, String title, String markerType) {
        return new HasRecord('deapp.de_gpl_info', ['PLATFORM': platform], props + ['TITLE': title, 'MARKER_TYPE': markerType])
    }

    public static HasRecord hasRecord(Map<CharSequence, Object> keyAttrs = [:], CharSequence tableName) {
        return new HasRecord(tableName, keyAttrs, [:])
    }

    public static HasRecord hasRecord(CharSequence tableName, Map<CharSequence, Object> keyAttrs, Map<CharSequence, Object> valueAttrs) {
        return new HasRecord(tableName, keyAttrs, valueAttrs)
    }

    public static HasFact hasFact(String conceptPath, String subjectId, Object value) {
        return new HasFact(conceptPath, subjectId, value)
    }

    public static RowMatcher matchesRow(Map<CharSequence, Object> valueAttrs) {
        return new RowMatcher(valueAttrs)
    }
}
