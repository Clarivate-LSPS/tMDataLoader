package com.thomsonreuters.lsps.transmart.etl

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasPatient
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

    void testItLoadsData() {
        String conceptPath = "\\Test Studies\\ClinicalSample\\"
        String conceptPathForPatient = conceptPath + "Biomarker Data\\Mutations\\EGFR (Entrez ID: 1956)\\AA mutation\\"

        processor.process(
                new File("fixtures/Public Studies/${studyName}_${studyId}/ClinicalDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient('HCC2935').inTrial(studyId))
        assertThat(sql, hasNode(conceptPathForPatient).withPatientCount(9))
        assertThat(sql, hasNode(conceptPathForPatient + 'T790M\\'))
    }

    void testItMergesData() {
        String conceptPath = "\\Test Studies\\ClinicalSample\\"
        String conceptPathForPatient = conceptPath + "Biomarker Data\\Mutations\\EGFR (Entrez ID: 1956)\\AA mutation\\"
        String subjId = 'HCC2935'

        processor.process(
                new File("fixtures/Public Studies/${studyName}_${studyId}/ClinicalDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient(subjId).inTrial(studyId))
        assertThat(sql, hasNode(conceptPathForPatient).withPatientCount(9))
        assertThat(sql, hasNode(conceptPathForPatient + 'T790M\\'))

        processor.process(
                new File("fixtures/Additional Samples/${studyName}_${studyId}/ClinicalDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        assertThat(sql, hasPatient(subjId).inTrial(studyId))
        assertThat(sql, hasNode(conceptPathForPatient).withPatientCount(9))
        assertThat(sql, hasNode(conceptPathForPatient + 'T790M TEST\\'))

        assertThat(sql, hasNode($/${conceptPath}Biomarker Data\Mutations\APC (Entrez ID: 324)\/$))
    }

}
