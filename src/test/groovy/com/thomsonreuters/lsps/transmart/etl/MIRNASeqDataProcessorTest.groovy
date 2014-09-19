package com.thomsonreuters.lsps.transmart.etl

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasPatient
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertThat

class MIRNASeqDataProcessorTest extends ConfigAwareTestCase {
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
        super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ? or sourcesystem_cd = ?', studyId, studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        runScript('I2B2_PROCESS_QPCR_MIRNA_DATA.sql')
    }

    void assertThatSampleIsPresent(String sampleId, sampleData) {
        def sample = sql.firstRow('select * from deapp.de_subject_sample_mapping where trial_name = ? and subject_id = ?',
                studyId, sampleId)
        assertThat(sample, notNullValue())
        String suffix = '';
        sampleData.each { ref_id, value ->
            def rows = sql.rows("select d.zscore from deapp.de_subject_mirna_data d " +
                    "inner join deapp.de_qpcr_mirna_annotation a on d.probeset_id = a.probeset_id " +
                    "where a.gpl_id = ? and d.assay_id = ? and a.id_ref = ?",
                    platformId, sample.assay_id, ref_id)
            assertThat(rows?.size(), equalTo(1))
            assertEquals(rows[0].zscore as double, value as double, 0.001)
        }
    }


    void testItLoadsData() {
        processor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/MIRNASeqDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(db, hasSample(studyId, 'GSM918946'))
        assertThat(db, hasPatient('GSM918943').inTrial(studyId))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\Biomarker Data\\Test MIRNAseq Platform\\Synovium\\").
                withPatientCount(8))

        assertThat(db, hasRecord('deapp.de_subject_sample_mapping',
                [trial_name: studyId, gpl_id: platformId, subject_id: 'GSM918944', sample_cd: 'GSM918944'],
                [tissue_type: 'Synovium', platform: mirnaType, trial_name: studyId]))

        assertThat(db, hasRecord('deapp.de_subject_mirna_data',
                [trial_source: studyId + ':STD', trial_name: studyId], null))

        assertThatSampleIsPresent('GSM918944', ['16': 1.058280000])
    }
}
