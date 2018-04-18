package com.thomsonreuters.lsps.transmart.etl.matchers

import groovy.sql.GroovyRowResult
import groovy.transform.CompileStatic
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import org.hamcrest.BaseMatcher
import org.hamcrest.Description

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

    public
    static HasRecord hasPlatform(Map<String, Object> props = [:], String platform, String title, String markerType) {
        return new HasRecord('deapp.de_gpl_info', ['PLATFORM': platform], props + ['TITLE': title, 'MARKER_TYPE': markerType])
    }

    public static HasRecord hasRecord(Map<String, Object> keyAttrs = [:], String tableName) {
        return new HasRecord(tableName, keyAttrs, [:])
    }

    @CompileStatic
    public static HasRecord hasRecord(String tableName, Map<String, Object> keyAttrs, Map<String, Object> valueAttrs) {
        return new HasRecord(tableName, keyAttrs, (Map) valueAttrs)
    }

    public static HasRecord hasRecord(String tableName, Map<String, Object> keyAttrs,
                                      String matcherDescription = 'closure',
                                      @ClosureParams(value = SimpleType, options = 'groovy.sql.GroovyRowResult') final Closure<Boolean> matcher) {
        return new HasRecord(tableName, keyAttrs, new BaseMatcher<GroovyRowResult>() {
            @Override
            boolean matches(Object item) {
                matcher.call(item)
            }

            @Override
            void describeTo(Description description) {
                description.appendText(matcherDescription)
            }
        })
    }

    public static HasFact hasFact(String conceptPath, String subjectId, Object value) {
        return new HasFact(conceptPath, subjectId, value)
    }

    public static RowMatcher matchesRow(Map<String, Object> valueAttrs) {
        return new RowMatcher(valueAttrs)
    }

    public static HasFactAttribute hasFactDate(String sourcesystemCd, String conceptPath, Integer instanceNum, Map<String, Object> valueAttrs) {
        return new HasFactAttribute(sourcesystemCd, conceptPath, instanceNum, valueAttrs)
    }

    public static HasTrialVisitDimension hasTrialVisitDimension(String conceptPath, String sourcesystemCdAndPatientId, Map<String, Object> valueAttrs) {
        return new HasTrialVisitDimension(conceptPath, sourcesystemCdAndPatientId, valueAttrs)
    }

    public static HasVisitDimension hasVisitDimension(String patientId, String sourcesystemCd, Map<String, Object> valueAttrs){
        return new HasVisitDimension(patientId, sourcesystemCd, valueAttrs)
    }
}
