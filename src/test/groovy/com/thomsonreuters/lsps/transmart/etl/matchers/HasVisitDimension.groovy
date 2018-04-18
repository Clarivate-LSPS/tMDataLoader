package com.thomsonreuters.lsps.transmart.etl.matchers

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.Description
import org.hamcrest.Matcher

/**
 * Created by bondarev on 4/1/14.
 */
class HasVisitDimension extends BaseMatcher<Sql> {
    private String patiendId
    private String sourcesystemCd
    private Matcher<? super GroovyRowResult> rowMatcher

    private def trialDimensions

    HasVisitDimension(String patientId, String sourcesystemCd, Map<String, Object> valueAttrs) {
        this.patiendId = patientId
        this.sourcesystemCd = sourcesystemCd

        this.rowMatcher = new RowMatcher(valueAttrs)
    }

    @Override
    boolean matches(Object item) {
        def sql = item as Sql
        trialDimensions = sql.rows("""
            select * from i2b2demodata.visit_dimension where
            patient_num in (
                    select patient_num from i2b2demodata.patient_dimension where sourcesystem_cd = ?)
            and
            sourcesystem_Cd = ?
        """, ["${sourcesystemCd}:${patiendId}".toString(), sourcesystemCd])
        if (!trialDimensions)
            return false

        if (trialDimensions.size() == 0) return false

        for (def trialDimension : trialDimensions)
            if (rowMatcher.matches(trialDimension)) return true

        return false
    }

    @Override
    void describeTo(Description description) {
        description.appendText('patient ').appendValue(patiendId)
                .appendText(' for sourcesystem_cd ').appendValue(sourcesystemCd)
                .appendText(' exists')
    }

    @Override
    void describeMismatch(Object item, Description description) {
        if (trialDimensions.size() == 0)
            description.appendText('trial dimension wasn\'t found')
        else
            description.appendText('trial dimenstions have different values')
    }
}
