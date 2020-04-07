package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.db.core.DatabaseType

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.*
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertThat

class ExpressionSerialHDDDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private ExpressionSerialHDDDataProcessor _processor

    String studyName = 'Test ExpressionSerialHDD Study'
    String studyId = 'TSTSHDD'
    String platformId = 'HDD999'

    ExpressionSerialHDDDataProcessor getProcessor() {
        _processor ?: (_processor = new ExpressionSerialHDDDataProcessor(config))
    }

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ?', studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        runScript('I2B2_PROCESS_MRNA_DATA.sql')
        runScript('I2B2_PROCESS_SERIAL_HDD_DATA.sql')
        if (database?.databaseType == DatabaseType.Postgres) {
            runScript('I2B2_LOAD_SAMPLES.sql')
        }
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
            def rows = sql.rows("select d.raw_intensity from deapp.de_subject_microarray_data${suffix} d " +
                    "inner join deapp.de_mrna_annotation a on d.probeset_id = a.probeset_id " +
                    "where a.gpl_id = ? and d.assay_id = ? and a.probe_id = ?",
                    platformId, sample.assay_id, probe_id)
            assertThat(rows?.size(), equalTo(1))
            assertEquals(rows[0].raw_intensity as double, value as double, 0.01)
        }
    }

    void assertThatSampleCdIsLoaded(String studyId, String sampleCd, String conceptPath) {
        def sourcesystemCd = studyId + ":" + sampleCd
        def rows = sql.rows("select obf.sample_cd from i2b2demodata.observation_fact obf " +
                "inner join i2b2demodata.patient_dimension pd on obf.patient_num = pd.patient_num " +
                "inner join i2b2demodata.concept_dimension cd on obf.concept_cd = cd.concept_cd " +
                "where pd.sourcesystem_cd = ? and cd.concept_path = ?", sourcesystemCd, conceptPath)
        assertThat(rows?.size(), equalTo(1))
        assertEquals(rows[0].sample_cd, sampleCd)
    }

    void testItLoadsData() {
        processor.process(
                new File("fixtures/Test Studies/${studyName}/ExpressionSerialHDDDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        def testNodeName = "\\Test Studies\\${studyName}\\Sample Factors Week 1\\"
        def testMetadata = """<?xml version="1.0"?>
                        <ValueMetadata>
                            <Oktousevalues>Y</Oktousevalues>
                            <SeriesMeta>
                                <Value>7</Value>
                                <Unit>days</Unit>
                                <DisplayName>Week 1</DisplayName>
                            </SeriesMeta>
                    </ValueMetadata>"""

        assertThat(db, hasSample(studyId, 'A204'))
        assertThat(db, hasPatient('A673').inTrial(studyId))
        assertThat(db, hasNode(testNodeName).withPatientCount(22))

        assertThatSampleIsPresent('A204', ['221356_x_at': 6.69])

        assertThat(db, hasRecord('deapp.de_subject_sample_mapping',
                [trial_name: studyId, gpl_id: platformId, subject_id: 'CGTHW1'],
                [sample_cd: 'CGTHW1', timepoint: null, tissue_type: 'Blood', sample_type: null]))

        assertThat(db, hasRecord('i2b2metadata.i2b2',
                [c_fullname: testNodeName, c_name: 'Sample Factors Week 1', sourcesystem_cd: studyId],
                [c_visualattributes: 'LAH', c_metadataxml: testMetadata]))

        assertThat(db, hasRecord('i2b2demodata.sample_dimension', [sample_cd: 'A204'], null))
        assertThat(db, hasRecord('i2b2demodata.sample_dimension', [sample_cd: 'U2OS'], null))

        assertThatSampleCdIsLoaded(studyId, 'A204', testNodeName)
    }
}
