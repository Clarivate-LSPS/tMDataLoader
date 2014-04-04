package com.thomsonreuters.lsps.transmart.etl

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasPatient
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 2/24/14.
 */
class ExpressionDataProcessorTest extends ConfigAwareTestCase {
    private ExpressionDataProcessor _processor

    String studyName = 'Test Study'
    String studyId = 'GSE0'
    String platformId = 'GEX_TST'

    ExpressionDataProcessor getProcessor() {
        _processor ?: (_processor = new ExpressionDataProcessor(config))
    }

    @Override
    void setUp() {
        super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ?', studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        runScript('I2B2_PROCESS_MRNA_DATA.sql')
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
            def rows = sql.rows("select d.raw_intensity from deapp.de_subject_microarray_data${suffix} d " +
                    "inner join deapp.de_mrna_annotation a on d.probeset_id = a.probeset_id " +
                    "where a.gpl_id = ? and d.assay_id = ? and a.probe_id = ?",
                    platformId, sample.assay_id, probe_id)
            assertThat(rows?.size(), equalTo(1))
            assertEquals(rows[0].raw_intensity as double, value as double, 0.001)
        }
    }

    void testItLoadsData() {
        processor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(db, hasSample(studyId, 'TST1000000719'))
        assertThat(db, hasPatient('996RMS').inTrial(studyId))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\Biomarker Data\\Test GEX Platform\\Blood\\").
                withPatientCount(28))
        assertThatSampleIsPresent('TST1000000719', ['1007_s_at': 6.624529839])
    }

    void testItMergeSamples() {
        processor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThatSampleIsPresent('TST1000000719', ['1007_s_at': 6.624529839])
        assertThatSampleIsPresent('TST1000000722', ['1007_s_at': 6.374219894])
        assertThatSampleIsPresent('TST1000000723', ['1007_s_at': 6.653120041])
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\Biomarker Data\\Test GEX Platform\\Blood\\").
                withPatientCount(29))

        processor.process(
                new File("fixtures/Additional Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThatSampleIsPresent('TST1000000719', ['1007_s_at': 6.624529839])
        assertThatSampleIsPresent('TST2000000719', ['1007_s_at': 7.624529839])
        assertThatSampleIsPresent('TST1000000722', ['1007_s_at': 5.374219894])
        assertThatSampleIsPresent('TST1000000723', ['1007_s_at': 6.653120041])
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\Biomarker Data\\Test GEX Platform\\Blood\\").
                withPatientCount(30))
    }
}
