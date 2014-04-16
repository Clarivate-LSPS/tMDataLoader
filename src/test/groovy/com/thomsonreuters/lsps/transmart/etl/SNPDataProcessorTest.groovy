package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.sql.SqlMethods

import static com.thomsonreuters.lsps.transmart.Fixtures.getAdditionalStudiesDir
import static com.thomsonreuters.lsps.transmart.Fixtures.studyDir
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.*
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 2/24/14.
 */
class SNPDataProcessorTest extends ConfigAwareTestCase {
    private SNPDataProcessor _processor

    String studyName = 'Test Study'
    String studyId = 'GSE0'

    @Override
    void setUp() {
        super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ?', studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        runScript('I2B2_PROCESS_SNP_DATA.sql')
    }

    SNPDataProcessor getProcessor() {
        _processor ?: (_processor = new SNPDataProcessor(config))
    }

    def findSample(String sampleId) {
        use(SqlMethods) {
            db.findRecord('deapp.de_subject_sample_mapping', trial_name: studyId, platform: 'SNP', sample_cd: sampleId)
        }
    }

    def hasCopyNumber(String sampleId, String snpName, int copyNumber) {
        def sample = findSample(sampleId)
        hasRecord('deapp.de_snp_copy_number',
                [patient_num: sample.patient_id, snp_name: snpName], [copy_number: copyNumber])
    }

    void testItLoadsData() {
        processor.process(new File(studyDir(studyName, studyId), "SNPDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(db, hasSample(studyId, 'TST001', platform: 'SNP'))
        assertThat(db, hasPatient('Subject_0').inTrial(studyId))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\SNP\\Test SNP Platform\\Unknown\\").withPatientCount(3))
        def sample = findSample('TST001')
        assertThat(db, hasRecord('deapp.de_snp_calls_by_gsm',
                [patient_num: sample.patient_id, gsm_num: 'TST001', snp_name: 'SNP_A-1984209'],
                [snp_calls: '2']))
        assertThat(db, hasRecord('deapp.de_snp_copy_number',
                [patient_num: sample.patient_id, snp_name: 'SNP_A-4265338'],
                [chrom: '1', chrom_pos: 216721255, copy_number: 2]))
    }

    void testItMergeSamples() {
        processor.process(new File(studyDir(studyName, studyId), "SNPDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(db, hasCopyNumber('TST001', 'SNP_A-4265338', 2))
        assertThat(db, hasCopyNumber('TST002', 'CN_497981', 1))
        assertThat(sql, hasNode($/\Test Studies\${studyName}\SNP\Test SNP Platform\Unknown\/$).withPatientCount(3))

        processor.process(new File(studyDir(studyName, studyId, additionalStudiesDir), "SNPDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(db, hasCopyNumber('TST001', 'SNP_A-4265338', 1))
        assertThat(db, hasCopyNumber('TST002', 'CN_497981', 1))
        assertThat(db, hasCopyNumber('TST001', 'SNP_A-2176913', 1))
        assertThat(sql, hasNode($/\Test Studies\${studyName}\SNP\Test SNP Platform\Unknown\/$).withPatientCount(4))
    }
}
