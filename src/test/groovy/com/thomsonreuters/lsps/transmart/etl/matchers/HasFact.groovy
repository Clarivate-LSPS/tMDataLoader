package com.thomsonreuters.lsps.transmart.etl.matchers

import groovy.sql.GroovyRowResult
import groovy.sql.Sql
import org.hamcrest.BaseMatcher
import org.hamcrest.Description

import static org.hamcrest.CoreMatchers.both

/**
 * Created by bondarev on 4/1/14.
 */
class HasFact extends BaseMatcher<Sql> {
    private String conceptPath
    private String subjectId
    private Object value

    private def node
    private def patient
    private List<GroovyRowResult> facts

    HasFact(String conceptPath, String subjectId, Object value) {
        this.conceptPath = conceptPath
        this.subjectId = subjectId
        this.value = value
    }

    @Override
    boolean matches(Object item) {
        def sql = item as Sql
        node = sql.firstRow('select c_basecode, sourcesystem_cd from i2b2metadata.i2b2 where C_FULLNAME=?', [conceptPath])
        if (!node)
            return false

        patient = sql.firstRow('select patient_num from i2b2demodata.patient_dimension where sourcesystem_cd=?',
                ["${node.sourcesystem_cd}:$subjectId" as String])
        if (!patient)
            return false

        String field = value instanceof Number ? 'nval_num' : 'tval_char'
        facts = sql.rows("select $field as val from i2b2demodata.observation_fact where concept_cd=? and patient_num=?",
                [node.c_basecode, patient.patient_num])
        return facts.size() == 1 && facts[0].val == value
    }

    @Override
    void describeTo(Description description) {
        description.appendText('fact by path ').appendValue(conceptPath).
                appendText(' for patient ').appendValue(subjectId).
                appendText(' with value ').appendValue(value)
    }

    @Override
    void describeMismatch(Object item, Description description) {
        if (!node)
            description.appendText('node ').appendValue(conceptPath).appendText(' was not found')
        else if (!patient)
            description.appendText('subject ').appendValue(subjectId).appendText(' was not found')
        else if (!facts)
            description.appendText('fact entry wasn\'t found')
        else if (facts.size() > 1)
            description.appendText('more than one fact was found: ').appendValue(facts.size())
        else
            description.appendText('actual fact value does\'t match expected: ').
                    appendValue(facts[0].val).appendText(' != ').appendValue(value)
    }
}
