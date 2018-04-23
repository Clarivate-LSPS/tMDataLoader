package com.thomsonreuters.lsps.transmart.etl.matchers

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.Description
import org.hamcrest.Matcher

class HasTrialVisitDimension extends BaseMatcher<Sql> {
    private String conceptPath
    private String sourceSystemCdAndPatientId
    private String label
    private Matcher<? super GroovyRowResult> rowMatcher
    private List<GroovyRowResult> dimension

    HasTrialVisitDimension(String conceptPath, String sourcesystemCdAndPatientId, Map<String, Object> valueAttrs) {
        this.conceptPath = conceptPath
        this.sourceSystemCdAndPatientId = sourcesystemCdAndPatientId
        this.rowMatcher = new RowMatcher(valueAttrs)
    }

    HasTrialVisitDimension(String conceptPath, String sourcesystemCdAndPatientId, String label, Map<String, Object> valueAttrs) {
        this.conceptPath = conceptPath
        this.sourceSystemCdAndPatientId = sourcesystemCdAndPatientId
        this.rowMatcher = new RowMatcher(valueAttrs)
        this.label = label
    }

    @Override
    boolean matches(Object item) {
        def sql = item as Sql
        StringBuilder sb = new StringBuilder("""
            select * from i2b2demodata.trial_visit_dimension where trial_visit_num in (
            select trial_visit_num from i2b2demodata.observation_fact where concept_cd in (
              select c_basecode from i2b2metadata.i2b2 where c_fullname = ?
            ) 
            and patient_num in (
              select patient_num from i2b2demodata.patient_dimension where sourcesystem_cd = ?) 
            )
        """)
        if (label) {
            sb.append(""" and rel_time_label = ? """)
        }

        dimension = sql.rows(sb.toString(),
                (label ? [conceptPath, sourceSystemCdAndPatientId, label] : [conceptPath, sourceSystemCdAndPatientId]))

        if (!dimension) return false
        if (dimension.size() > 1) return false
        if (rowMatcher.matches(dimension)) return true
    }

    @Override
    void describeTo(Description description) {
        description.appendText('fact by path ').appendValue(conceptPath)
                .appendText(' for patient ').appendValue(sourceSystemCdAndPatientId)
                .appendText(' exists')
    }

    @Override
    void describeMismatch(Object item, Description description) {
        if (!dimension || dimension.size() == 0)
            description.appendText('dimension wasn\'t found')
        else if (dimension.size() > 1)
            description.appendText('found more 1 dimension')
    }
}
