package com.thomsonreuters.lsps.transmart.etl

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasPatient
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertThat


class RNASeqDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private RNASeqDataProcessor _processor
    String studyName = 'Test RNASeq Study'
    String studyId = 'GSE_A_37424'
    String platformId = 'RNASeq999'

    RNASeqDataProcessor getProcessor() {
        _processor ?: (_processor = new RNASeqDataProcessor(config))
    }

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ? or sourcesystem_cd = ?', studyId, studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        runScript('I2B2_PROCESS_RNA_SEQ_DATA.sql')
    }

    void assertThatSampleIsPresent(String sampleId, sampleData) {
        def sample = sql.firstRow('select * from deapp.de_subject_sample_mapping where trial_name = ? and sample_cd = ?',
                studyId, sampleId)
        assertThat(sample, notNullValue())
        String suffix = '';
        if (sample.hasProperty("partition_id")){
            suffix = sample.partition_id ? "_${sample.partition_id}" : ''
        }
        sampleData.each { probe_id, value ->
            def rows = sql.rows("select d.raw_intensity from deapp.de_subject_rna_data${suffix} d " +
                    "inner join deapp.de_rnaseq_annotation a on d.probeset_id = a.transcript_id " +
                    "where a.gpl_id = ? and d.assay_id = ? and a.gene_symbol = ?",
                    platformId, sample.assay_id, probe_id)
            assertThat(rows?.size(), equalTo(1))
            assertEquals(rows[0].raw_intensity as double, value as double, 0.001)
        }
    }


    void testItLoadsData() {
        processor.process(
                new File("fixtures/Test Studies/${studyName}/RNASeqDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(db, hasSample(studyId, 'S57024'))
        assertThat(db, hasPatient('0:1').inTrial(studyId))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\Biomarker Data\\Intestine\\Test\\").
                withPatientCount(2))

        assertThat(db, hasRecord('deapp.de_subject_sample_mapping',
                [trial_name: studyId, gpl_id: platformId, subject_id: '2', sample_cd: 'S57024'],
                [platform: 'RNA_AFFYMETRIX']))

        assertThatSampleIsPresent('S57023', ['ASCC1': 2])
    }
}
