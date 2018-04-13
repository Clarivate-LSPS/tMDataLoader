package com.thomsonreuters.lsps.transmart.etl.matchers

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.Description
import org.hamcrest.Matcher

class HasTrialVisitDimension extends BaseMatcher<Sql> {
    private String conceptPath
    private String sourceSystemCdAndPatientId
    private Matcher<? super GroovyRowResult> rowMatcher
    private List<GroovyRowResult> dimension

    HasTrialVisitDimension(String conceptPath, String sourcesystemCdAndPatientId, Map<String, Object> valueAttrs) {
        this.conceptPath = conceptPath
        this.sourceSystemCdAndPatientId = sourcesystemCdAndPatientId
        this.rowMatcher = new RowMatcher(valueAttrs)
    }

    @Override
    boolean matches(Object item) {
        def sql = item as Sql
        dimension = sql.rows("""
            select * from i2b2demodata.trial_visit_dimension where trial_visit_num in (
            select trial_visit_num from i2b2demodata.observation_fact where concept_cd in (
              select c_basecode from i2b2metadata.i2b2 where c_fullname = ?
            ) 
            and patient_num in (
              select patient_num from i2b2demodata.patient_dimension where sourcesystem_cd = ?) 
            )
        """, [conceptPath, sourceSystemCdAndPatientId])

        if (!dimension) return false

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
    }
}
