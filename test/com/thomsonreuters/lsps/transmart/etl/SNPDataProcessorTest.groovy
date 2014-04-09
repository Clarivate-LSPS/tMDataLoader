package com.thomsonreuters.lsps.transmart.etl

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 2/24/14.
 */
class SNPDataProcessorTest extends ConfigAwareTestCase {
    private SNPDataProcessor _processor

    String studyName = 'Q4_SNP'
    String studyId = 'GSE0'
    String platformId = 'TST_SNP'

    @Override
    void setUp() {
        super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ?', studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
    }

    SNPDataProcessor getProcessor() {
        _processor ?: (_processor = new SNPDataProcessor(config))
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
            def rows = sql.rows("select d.log_intensity from deapp.de_subject_microarray_data${suffix} d " +
                    "inner join deapp.de_mrna_annotation a on d.probeset_id = a.probeset_id " +
                    "where a.gpl_id = ? and d.assay_id = ? and a.probe_id = ?",
                    platformId, sample.assay_id, probe_id)
            assertThat(rows?.size(), equalTo(1))
            assertEquals(rows[0].log_intensity as double, value as double, 0.001)
        }
    }

    void testItLoadsData() {
        sql.withTransaction {
            processor.process(
                    new File("fixtures/Public Studies/${studyName}_${studyId}/SNPDataToUpload"),
                    [name: studyName, node: "Test Studies\\${studyName}".toString()])
            assertThatSampleIsPresent('TST001', ['SNP_A-4265338': 0.628913])
            sql.rollback()
        }
    }

    void testItMergeSamples() {
        processor.process(
                new File("fixtures/Public Studies/${studyName}_${studyId}/SNPDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThatSampleIsPresent('TST001', ['SNP_A-4265338': 0.628913])
        assertThatSampleIsPresent('TST002', ['CN_497981': 0.057206])
        assertThat(sql, hasNode($/\Test Studies\${studyName}\SNP\Test SNP Platform\Unknown\/$).withPatientCount(3))

        processor.process(
                new File("fixtures/Additional Samples/${studyName}_${studyId}/SNPDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThatSampleIsPresent('TST001', ['SNP_A-4265338': 0.528913])
        assertThatSampleIsPresent('TST002', ['CN_497981': 0.057206])
        assertThatSampleIsPresent('TST001', ['SNP_A-2176913': 0.018677])
        assertThat(sql, hasNode($/\Test Studies\${studyName}\SNP\Test SNP Platform\Unknown\/$).withPatientCount(4))
    }
}
