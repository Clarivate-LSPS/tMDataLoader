package com.thomsonreuters.lsps.transmart.etl

import static org.hamcrest.CoreMatchers.equalTo
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 2/24/14.
 */
class ClinicalDataProcessorTest extends ConfigAwareTestCase {
    private ClinicalDataProcessor _processor

    String studyName = 'ClinicalSample'
    String studyId = 'CCLE_TEST'
    String platformId = 'GPL570'

    ClinicalDataProcessor getProcessor() {
        _processor ?: (_processor = new ClinicalDataProcessor(config))
    }

    void assertThatSampleIsPresent(String conceptPath, String cFullname, String attrName, String conceptPathForPatient, String subjId) {
        def query = sql.rows('select * from i2b2demodata.patient_trial where patient_num in ' +
                '(select patient_num from i2b2demodata.patient_dimension where SOURCESYSTEM_CD=?)', studyId +':' + subjId)
        assertThat(query?.size(), equalTo(1))

        query = sql.rows('select * from i2b2demodata.concept_dimension where CONCEPT_PATH=?', conceptPath)
        assertThat(query?.size(), equalTo(1))

        query = sql.rows('select c_name from i2b2metadata.i2b2 where C_FULLNAME=?', cFullname)
        assertEquals(query[0].c_name, attrName)

        query = sql.rows('select patient_count from i2b2demodata.concept_counts where CONCEPT_PATH=?', conceptPathForPatient);
        assertTrue(query[0].patient_count == 9)
    }

    void assertThatNewNodeIsAdded(conceptPathForAdded, attrName) {
        def query = sql.rows('select c_name from i2b2metadata.i2b2 where C_FULLNAME=?', conceptPathForAdded)
        assertEquals(query[0].c_name, attrName)

        query = sql.rows('select * from i2b2demodata.concept_dimension where CONCEPT_PATH=?', conceptPathForAdded)
        assertThat(query?.size(), equalTo(1))
    }

    void testItLoadsData() {
        processor.process(
                new File("fixtures/Public Studies/${studyName}_${studyId}/ClinicalDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        String conceptPath = "\\Test Studies\\ClinicalSample\\"
        String conceptPathForPatient = conceptPath + "Biomarker Data\\Mutations\\EGFR (Entrez ID: 1956)\\AA mutation\\"
        String attrName = "T790M"
        String cFullname = conceptPathForPatient + attrName + "\\"
        String subjId = "HCC2935"
        assertThatSampleIsPresent(conceptPath, cFullname, attrName, conceptPathForPatient, subjId)
    }

    void testItMergesData() {
        processor.process(
                new File("fixtures/Public Studies/${studyName}_${studyId}/ClinicalDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        String conceptPath = "\\Test Studies\\ClinicalSample\\"
        String conceptPathForPatient = conceptPath + "Biomarker Data\\Mutations\\EGFR (Entrez ID: 1956)\\AA mutation\\"
        String attrName = "T790M"
        String cFullname = conceptPathForPatient + attrName + "\\"
        String subjId = "HCC2935"
        assertThatSampleIsPresent(conceptPath, cFullname, attrName, conceptPathForPatient, subjId)

        processor.process(
                new File("fixtures/Additional Samples/${studyName}_${studyId}/ClinicalDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        attrName = "T790M TEST"
        cFullname = conceptPathForPatient + attrName + "\\"
        assertThatSampleIsPresent(conceptPath, cFullname, attrName, conceptPathForPatient, subjId)

        String conceptPathForAdded = conceptPath + 'Biomarker Data\\Mutations\\APC (Entrez ID: 324)\\'
        attrName = "APC (Entrez ID: 324)"
        assertThatNewNodeIsAdded(conceptPathForAdded,attrName)
    }

}
