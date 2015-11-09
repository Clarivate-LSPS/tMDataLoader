package com.thomsonreuters.lsps.transmart.etl

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.*
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertThat

class MIRNASeqDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private MIRNADataProcessor _processor
    String studyName = 'Test MirnaSeq Study'
    String studyId = 'MIRNASEQBASED'
    String platformId = 'GPL15467seqbased'
    String mirnaType = 'MIRNA_SEQ'

    MIRNADataProcessor getProcessor() {
        _processor ?: (_processor = new MIRNADataProcessor(config))
    }

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ? or sourcesystem_cd = ?', studyId, studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        runScript('I2B2_PROCESS_QPCR_MIRNA_DATA.sql')
        runScript('I2B2_MIRNA_ZSCORE_CALC.sql')
    }

    void assertThatSampleIsPresent(String sampleId, sampleData) {
        def sample = sql.firstRow('select * from deapp.de_subject_sample_mapping where trial_name = ? and subject_id = ?',
                studyId, sampleId)
        assertThat(sample, notNullValue())
        String suffix = '';
        sampleData.each { ref_id, value ->
            def rows = sql.rows("select d.raw_intensity, d.log_intensity, d.zscore from deapp.de_subject_mirna_data d" +
                    " inner join deapp.de_qpcr_mirna_annotation a on d.probeset_id = a.probeset_id" +
                    " where a.gpl_id = ? and d.assay_id = ? and a.id_ref = ?",
                    platformId, sample.assay_id, ref_id)
            assertThat(rows?.size(), equalTo(1))
            if (!(value instanceof Map)) {
                value = [zscore: value]
            }
            assertThat(rows[0], matchesRow(value as Map))
        }
    }

    void testItLoadsData() {
        processor.process(
                new File("fixtures/Test Studies/${studyName}/MIRNA_SEQDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString(), base_datatype: mirnaType])
        assertThat(db, hasSample(studyId, 'GSM918946'))
        assertThat(db, hasPatient('GSM918943').inTrial(studyId))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\Biomarker Data\\Test MIRNAseq Platform\\Synovium\\").
                withPatientCount(8))

        assertThat(db, hasRecord('deapp.de_subject_sample_mapping',
                [trial_name: studyId, gpl_id: platformId, subject_id: 'GSM918944', sample_cd: 'GSM918944'],
                [tissue_type: 'Synovium', platform: mirnaType]))

        assertThat(db, hasRecord('deapp.de_subject_mirna_data',
                [trial_source: studyId + ':STD', trial_name: studyId], null))

        assertThatSampleIsPresent('GSM918944', ['16': [raw_intensity: 0.163313108, log_intensity: -2.61429,
                                                       zscore: 1.058280000]])
    }

    void testItLoadsLog2Data() {
        processor.process(
                new File("fixtures/Test Studies/${studyName}/MIRNA_SEQDataToUpload_Log2"),
                [name: studyName, node: "Test Studies\\${studyName}".toString(), base_datatype: mirnaType])
        assertThat(db, hasSample(studyId, 'GSM918946'))
        assertThat(db, hasPatient('GSM918943').inTrial(studyId))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\Biomarker Data\\Test MIRNAseq Platform\\Synovium\\").
                withPatientCount(8))

        assertThat(db, hasRecord('deapp.de_subject_sample_mapping',
                [trial_name: studyId, gpl_id: platformId, subject_id: 'GSM918944', sample_cd: 'GSM918944'],
                [tissue_type: 'Synovium', platform: mirnaType]))

        assertThat(db, hasRecord('deapp.de_subject_mirna_data',
                [trial_source: studyId + ':STD', trial_name: studyId], null))

        assertThatSampleIsPresent('GSM918944', ['16': [log_intensity: 0.163313108]])
    }
}
