package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.fixtures.Study

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.*
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertThat

class MIRNAQpcrDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private MIRNADataProcessor _processor
    String studyName = 'Test MirnaQpcr Study'
    String studyId = 'TEST005'
    String platformId = 'GPL15467qpcrbased'
    String mirnaType = 'MIRNA_QPCR'

    MIRNADataProcessor getProcessor() {
        _processor ?: (_processor = new MIRNADataProcessor(config))
    }

    void clearAnnotation(String gpl_id) {
        sql.execute("delete from deapp.de_qpcr_mirna_annotation where gpl_id = ${gpl_id}")
    }

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        runScript('I2B2_PROCESS_QPCR_MIRNA_DATA.sql')
        runScript('I2B2_MIRNA_ZSCORE_CALC.sql')
        clearAnnotation(platformId) // force reload for a clean test
    }

    void assertThatSampleIsPresent(String sampleId, sampleData) {
        def sample = sql.firstRow('select * from deapp.de_subject_sample_mapping where trial_name = ? and subject_id = ?',
                studyId, sampleId)
        assertThat(sample, notNullValue())
        String suffix = '';
        sampleData.each { ref_id, value ->
            def rows = sql.rows("select d.raw_intensity, d.log_intensity, d.zscore from deapp.de_subject_mirna_data d" +
                    " inner join deapp.de_qpcr_mirna_annotation a on d.probeset_id = a.probeset_id " +
                    " where a.gpl_id = ? and d.assay_id = ? and a.id_ref = ?",
                    platformId, sample.assay_id, ref_id)
            assertThat(rows?.size(), equalTo(1))
            assertThat(rows[0], matchesRow(value as Map))
        }
    }


    void testItLoadsData() {
        Study.deleteById(config, studyId)
        processor.process(
                new File("fixtures/Test Studies/${studyName}/MIRNA_QPCRDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString(), base_datatype: mirnaType])
        assertThat(db, hasSample(studyId, 'GSM918938'))
        assertThat(db, hasPatient('2:S57024').inTrial(studyId))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\Biomarker Data\\Test MIRNAqpcr Platform\\Intestine\\Test\\").
                withPatientCount(2))

        assertThat(db, hasRecord('deapp.de_subject_sample_mapping',
                [trial_name: studyId, gpl_id: platformId, subject_id: 'S57023', sample_cd: 'GSM918938'],
                [tissue_type: 'Homo sapiens', platform: mirnaType, trial_name: studyId]))

        assertThat(db, hasRecord('deapp.de_subject_mirna_data',
                [trial_source: studyId + ':STD', trial_name: studyId], null))

        assertThatSampleIsPresent('S57023', ['1': [raw_intensity: 0.113408, log_intensity: -0.113408,
                                                   zscore: 0.70711]])
    }
}
