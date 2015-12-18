package com.thomsonreuters.lsps.transmart.etl

import static com.thomsonreuters.lsps.transmart.Fixtures.getAdditionalStudiesDir
import static com.thomsonreuters.lsps.transmart.Fixtures.studyDir
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.*
import static org.junit.Assert.assertThat
/**
 * Created by bondarev on 2/24/14.
 */
class SNPDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private SNPDataProcessor _processor

    String studyName = 'Test Study'
    String studyId = 'GSE0'
    String platformId = 'Test SNP Platform'

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        runScript('I2B2_PROCESS_SNP_DATA.sql')
        new DeleteDataProcessor(config).process(['id': studyId])
    }

    SNPDataProcessor getProcessor() {
        _processor ?: (_processor = new SNPDataProcessor(config))
    }

    def hasCopyNumber(String sampleId, String snpName, double copyNumber) {
        hasRecord('deapp.de_sample_snp_data', [sample_id: sampleId, snp_name: snpName], [copy_number: copyNumber])
    }

    void testItLoadsData() {
        withErrorLogging {
            processor.process(new File(studyDir(studyName, studyId), "SNPDataToUpload").toPath(),
                    [name: studyName, node: "Test Studies\\${studyName}".toString()])
        }
        assertThat(db, hasSample(studyId, 'TST001', platform: 'SNP'))
        assertThat(db, hasPatient('Subject_0').inTrial(studyId))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\SNP\\${platformId}\\Unknown\\").withPatientCount(3))
        assertThat(db, hasRecord('deapp.de_sample_snp_data',
                [sample_id: 'TST001', snp_name: 'SNP_A-4265338'],
                [snp_calls: null, copy_number: 0.628913]))
        assertThat(db, hasRecord('deapp.de_sample_snp_data',
                [sample_id: 'TST001', snp_name: 'SNP_A-1984209'],
                [snp_calls: '2', copy_number: -0.187262]))
    }

    void testItMergeSamples() {
        withErrorLogging {
            processor.process(new File(studyDir(studyName, studyId), "SNPDataToUpload").toPath(),
                    [name: studyName, node: "Test Studies\\${studyName}".toString()])
        }
        assertThat(db, hasCopyNumber('TST001', 'SNP_A-4265338', 0.628913))
        assertThat(db, hasCopyNumber('TST002', 'CN_497981', 0.057206))
        assertThat(db, hasNode($/\Test Studies\${studyName}\SNP\${platformId}\Unknown\/$).withPatientCount(3))

        processor.process(new File(studyDir(studyName, studyId, additionalStudiesDir), "SNPDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(db, hasCopyNumber('TST001', 'SNP_A-4265338', 0.528913))
        assertThat(db, hasCopyNumber('TST002', 'CN_497981', 0.057206))
        assertThat(db, hasCopyNumber('TST001', 'SNP_A-2176913', 0.018677))
        assertThat(db, hasNode($/\Test Studies\${studyName}\SNP\${platformId}\Unknown\/$).withPatientCount(4))
    }
}
