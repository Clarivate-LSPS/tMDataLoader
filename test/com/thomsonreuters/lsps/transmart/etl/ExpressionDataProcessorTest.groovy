package com.thomsonreuters.lsps.transmart.etl

import groovy.sql.Sql

import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 2/24/14.
 */
class ExpressionDataProcessorTest extends DataProcessorTestCase {
    private ExpressionDataProcessor _processor

    String studyName = 'TestSample'
    String studyId = 'GSE0'
    String platformId = 'GPL96'

    ExpressionDataProcessor getProcessor() {
        _processor ?: (_processor = new ExpressionDataProcessor([
                logger        : new Logger([isInteractiveMode: true]),
                db            : connectionSettings,
                controlSchema : 'tm_cz',
                securitySymbol: 'N'
        ]))
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
                new File("fixtures/Public Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThatSampleIsPresent('GSM1000000719', ['1007_s_at': 6.624529839])
    }

    void testItMergeSamples() {
        processor.process(
                new File("fixtures/Public Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThatSampleIsPresent('GSM1000000719', ['1007_s_at': 6.624529839])
        assertThatSampleIsPresent('GSM1000000722', ['1007_s_at': 6.374219894])
        processor.process(
                new File("fixtures/Additional Samples/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThatSampleIsPresent('GSM2000000719', ['1007_s_at': 6.624529839])
        assertThatSampleIsPresent('GSM1000000719', ['1007_s_at': 6.624529839])
        assertThatSampleIsPresent('GSM1000000722', ['1007_s_at': 5.374219894])
    }
}
