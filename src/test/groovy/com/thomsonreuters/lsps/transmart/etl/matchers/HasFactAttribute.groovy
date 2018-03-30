package com.thomsonreuters.lsps.transmart.etl.matchers

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.Description
import org.hamcrest.Matcher

/**
 * Created by bondarev on 4/1/14.
 */
class HasFactAttribute extends BaseMatcher<Sql> {
    private String conceptPath
    private String sourcesystemCd
    private Integer instanceNum
    private Map<String, Object> valueAttrs
    private Matcher<? super GroovyRowResult> rowMatcher

    private def node
    private def patient
    private List<GroovyRowResult> facts

    HasFactAttribute(String sourcesystemCd, String conceptPath, Integer instanceNum, Map<String, Object> valueAttrs) {
        this.conceptPath = conceptPath
        this.sourcesystemCd = sourcesystemCd
        this.instanceNum = instanceNum
        this.rowMatcher = new RowMatcher(valueAttrs)
    }

    @Override
    boolean matches(Object item) {
        def sql = item as Sql
        facts = sql.rows("""
            select * from i2b2demodata.observation_fact where
            patient_num in (
                    select patient_num from i2b2demodata.patient_dimension where sourcesystem_cd = ?)
            and
            concept_cd in (
                    select concept_cd from i2b2demodata.concept_dimension where concept_path = ?
            )
            and instance_num = ?
        """, [sourcesystemCd, conceptPath, instanceNum])
        if (!facts)
            return false

        return facts.size() == 1 && rowMatcher.matches(facts[0])
    }

    @Override
    void describeTo(Description description) {
        description.appendText('fact by path ').appendValue(conceptPath)
                .appendText(' for sourcesystem_cd ').appendValue(sourcesystemCd)
                .appendText(' with instance_num ').appendValue(instanceNum)
                .appendText(' exist')
    }

    @Override
    void describeMismatch(Object item, Description description) {
        if (facts.size() > 1)
            description.appendText('more than one fact was found: ').appendValue(facts.size())
        else if (facts.size() == 0)
            description.appendText('fact wasn\'t found')
    }
}
