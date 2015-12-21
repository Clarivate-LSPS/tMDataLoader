package com.thomsonreuters.lsps.transmart.etl
import com.thomsonreuters.lsps.transmart.fixtures.Study
import com.thomsonreuters.lsps.transmart.fixtures.StudyInfo

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.*
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertThat

class RNASeqDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private RNASeqDataProcessor _processor
    def studyInfo = new StudyInfo('GSE_A_37424', 'Test RNASeq Study')
    String studyName = studyInfo.name
    String studyId = studyInfo.id
    String platformId = 'RNASeq999'

    RNASeqDataProcessor getProcessor() {
        _processor ?: (_processor = new RNASeqDataProcessor(config))
    }

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        Study.deleteById(config, studyId)

        runScript('I2B2_RNA_SEQ_ANNOTATION.sql')
        runScript('I2B2_PROCESS_RNA_SEQ_DATA.sql')
    }

    void assertThatSampleIsPresent(String sampleId, sampleData) {
        def sample = sql.firstRow('select * from deapp.de_subject_sample_mapping where trial_name = ? and sample_cd = ?',
                studyId, sampleId)
        assertThat(sample, notNullValue())
        String suffix = '';
        if (sample.hasProperty("partition_id")) {
            suffix = sample.partition_id ? "_${sample.partition_id}" : ''
        }
        sampleData.each { probe_id, value ->
            def rows = sql.rows("select d.raw_intensity, d.log_intensity, d.zscore " +
                    "from deapp.de_subject_rna_data${suffix} d " +
                    "inner join deapp.de_rnaseq_annotation a on d.probeset_id = a.transcript_id " +
                    "where a.gpl_id = ? and d.assay_id = ? and a.gene_symbol = ?",
                    platformId, sample.assay_id, probe_id)
            assertThat(rows?.size(), equalTo(1))
            if (!(value instanceof Map)) {
                value = [raw: value]
            }
            [raw: 'raw_intensity', log: 'log_intensity', zscore: 'zscore'].each { field, col ->
                if (value.containsKey(field)) {
                    Double expected = value[field]
                    Double actual = rows[0][col]
                    def message = "Expected $field-value doesn't match actual: $actual != $expected"
                    if (expected != null && actual != null) {
                        assertEquals(message, expected, actual, 0.1)
                    } else {
                        assertEquals(message, (Object) expected, (Object) actual)
                    }
                }
            }
        }
    }

    void testItLoadsData() {
        Study.deleteById(config, studyId)
        processor.process(
                new File("fixtures/Test Studies/${studyName}/RNASeqDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(db, hasSample(studyId, 'S57024'))
        assertThat(db, hasPatient('0:1').inTrial(studyId))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\Biomarker Data\\${platformId}\\Intestine\\Test\\").
                withPatientCount(2))
        assertThat(db, hasRecord('deapp.de_subject_sample_mapping',
                [trial_name: studyId, gpl_id: platformId, subject_id: '2', sample_cd: 'S57024'],
                [platform: 'RNA_AFFYMETRIX']))
        assertThatSampleIsPresent('S57023', ['ASCC1': 2])
    }

    void testItLoadsLog2Data() {
        processor.process(
                new File("fixtures/Test Studies/${studyName}/RNASeqDataToUpload_Log2"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(db, hasSample(studyId, 'S57024'))
        assertThat(db, hasPatient('0:1').inTrial(studyId))
        assertThatSampleIsPresent('S57023', ['ASCC1': [log: 1.9108]])
    }
}
