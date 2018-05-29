package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import com.thomsonreuters.lsps.transmart.fixtures.Study
import spock.lang.Specification

import static com.thomsonreuters.lsps.transmart.Fixtures.studyDir
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.*
import static org.junit.Assert.assertThat
import static org.junit.Assert.assertTrue

class CrossStudyTest extends Specification implements ConfigAwareTestCase {
    ClinicalData clinicalData = Fixtures.clinicalData
    private ClinicalDataProcessor _processor

    String studyName = 'Test Study'
    String studyId = 'GSE0'

    void setup() {
        ConfigAwareTestCase.super.setUp()
        runScript('INSERT_ADDITIONAL_DATA.sql')
        runScript('I2B2_FILL_IN_TREE.sql')
        runScript('I2B2_FILL_IN_TREE_CROSS.sql')
        runScript('I2B2_LOAD_CLINICAL_DATA.sql')
        runScript('I2B2_DELETE_ALL_DATA.sql')
        runScript('i2b2_create_security_for_trial.sql')
        runScript('I2B2_BUILD_METADATA_XML.sql')
        runScript('I2B2_CREATE_CONCEPT_COUNTS.sql')
    }

    ClinicalDataProcessor getProcessor() {
        _processor ?: (_processor = new ClinicalDataProcessor(config))
    }

    def 'Load sample data with concept_cd'() {
        given:
        def studyConceptId = "GSECONCEPTCD"
        def studyConceptName = 'Test Data With Concept_cd'
        Study.deleteById(config, studyConceptId)

        when:
        def load = processor.process(
                new File(studyDir(studyConceptName, studyConceptId), "ClinicalDataToUpload").toPath(),
                [name: studyConceptName, node: "Test Studies\\${studyConceptName}".toString()])
        then:
        assertTrue(load)

        // Check basic nodes
        assertThat(sql, hasNode("\\Test Studies\\${studyConceptName}\\Demographics\\Race\\").withPatientCount(4))
        assertThat(sql, hasFact("\\Test Studies\\${studyConceptName}\\Demographics\\Race\\", '-61', 'Caucasian'))
        assertThat(sql, hasFact("\\Test Studies\\${studyConceptName}\\Demographics\\Race\\", '-51', 'Latino'))
        assertThat(sql, hasNode("\\Test Studies\\${studyConceptName}\\Demographics\\Gender\\Male\\").withPatientCount(2))
        assertThat(sql, hasNode("\\Test Studies\\${studyConceptName}\\Demographics\\Gender\\Female\\").withPatientCount(1))
        assertThat(sql, hasNode("\\Test Studies\\${studyConceptName}\\Demographics\\Gender\\Unknown\\").withPatientCount(1))

        // Check concept_cd
        assertThat(sql, hasFactAttribute("${studyConceptId}:-51", "\\Test Studies\\${studyConceptName}\\Demographics\\Gender\\Male\\", 1, [
                'concept_cd': 'DM:GENDER:M'
        ]))

        assertThat(sql, hasFactAttribute("${studyConceptId}:-41", "\\Test Studies\\${studyConceptName}\\Demographics\\Gender\\Female\\", 1, [
                'concept_cd': 'DM:GENDER:F'
        ]))

        assertThat(sql, hasFactAttribute("${studyConceptId}:TESTME", "\\Test Studies\\${studyConceptName}\\Demographics\\Gender\\Unknown\\", 1, [
                'concept_cd': 'DM:GENDER:UNKNOWN'
        ]))
    }

    def 'Load data with cross studies'() {
        given:
        def studyConceptId = "GSECONCEPTCD"
        def studyConceptName = 'Test Data With Concept_cd'
        Study.deleteById(config, studyConceptId)

        when:
        def load = processor.process(
                new File(studyDir(studyConceptName, studyConceptId), "ClinicalDataToUpload").toPath(),
                [name: studyConceptName, node: "Test Studies\\${studyConceptName}".toString()])
        then:
        assertTrue(load)

        assertThat(sql, hasFactAttribute("${studyConceptId}:-41", "\\Test Studies\\${studyConceptName}\\Vital\\Flag\\", 1, [
                'modifier_cd': 'VSIGN:FLAG'
        ]))
    }
}
