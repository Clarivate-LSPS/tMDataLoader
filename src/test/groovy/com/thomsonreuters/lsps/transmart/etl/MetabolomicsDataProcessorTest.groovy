package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.fixtures.Study
import com.thomsonreuters.lsps.db.core.DatabaseType

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasPatient
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertThat

class MetabolomicsDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private MetabolomicsDataProcessor _processor
    String studyName = 'Test Metabolomics Study'
    String studyId = 'GSE37427'
    String platformId = 'MET998'

    MetabolomicsDataProcessor getProcessor() {
        _processor ?: (_processor = new MetabolomicsDataProcessor(config))
    }

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ? or sourcesystem_cd = ?', studyId, studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        if (database?.databaseType == DatabaseType.Postgres) {
            runScript('I2B2_LOAD_METABOLOMICS_ANNOT.sql')
            runScript('I2B2_PROCESS_METABOLOMIC_DATA.sql')
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
            def rows = sql.rows("select d.raw_intensity from deapp.de_subject_metabolomics_data${suffix} d " +
                    "inner join deapp.de_metabolite_annotation a on d.metabolite_annotation_id = a.id " +
                    "where a.gpl_id = ? and d.assay_id = ? and a.hmdb_id = ?",
                    platformId, sample.assay_id, probe_id)
            assertThat(rows?.size(), equalTo(1))
            assertEquals(rows[0].raw_intensity as double, value as double, 0.001)
        }
    }

    void testItLoadsData() {
        Study.deleteById(config, studyId)
        processor.process(
                new File("fixtures/Test Studies/${studyName}/MetabolomicsDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(db, hasSample(studyId, '18PP'))
        assertThat(db, hasPatient('null:GSM918960').inTrial(studyId))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\Biomarker Data\\Test Metabolomics Platform\\").
                withPatientCount(10))

        assertThat(db, hasRecord('deapp.de_subject_sample_mapping',
                [trial_name: studyId, gpl_id: platformId, subject_id: 'GSM918965', sample_cd: '14PP'],
                [platform: 'METABOLOMICS']))

        assertThatSampleIsPresent('14PP', ['HMDB0TEST': 5095])
    }
}
