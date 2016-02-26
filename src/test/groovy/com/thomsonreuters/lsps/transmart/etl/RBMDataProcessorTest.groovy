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

class RBMDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private RBMDataProcessor _processor
    String studyName = 'Test RBM Study'
    String studyId = 'TESTRBM'
    String platformId = 'RBM100'

    RBMDataProcessor getProcessor() {
        _processor ?: (_processor = new RBMDataProcessor(config))
    }

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ? or sourcesystem_cd = ?', studyId, studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        if (database?.databaseType == DatabaseType.Postgres) {
            runScript('I2B2_LOAD_RBM_DATA.sql')
            runScript('I2B2_RBM_ZSCORE_CALC_NEW.sql')
        } else if (database?.databaseType == DatabaseType.Oracle) {
            runScript('I2B2_RBM_ZSCORE_CALC.sql')
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
            def rows = sql.rows("select d.zscore from deapp.de_subject_rbm_data${suffix} d " +
                    "inner join deapp.de_rbm_annotation a on d.antigen_name = a.antigen_name " +
                    "where a.gpl_id = ? and d.assay_id = ? and a.uniprot_id = ?",
                    platformId, sample.assay_id, probe_id)
            assertThat(rows?.size(), equalTo(1))
            // TODO Check: oracle: -0.7071, postgres: -1
            assertEquals(rows[0].zscore as double, value as double, 0.3)
        }
    }


    void testItLoadsData() {
        Study.deleteById(config, studyId)
        processor.process(
                new File("fixtures/Test Studies/${studyName}/RBMDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(db, hasSample(studyId, 'GA8015ZS-06'))
        assertThat(db, hasPatient('1:S57023').inTrial(studyId))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\Biomarker Data\\Test RBM Platform\\Intestine\\Test\\").
                withPatientCount(2))

        assertThat(db, hasRecord('deapp.de_subject_sample_mapping',
                [trial_name: studyId, gpl_id: platformId, subject_id: 'S57024', sample_cd: 'GA8015ZS-06'],
                [platform: 'RBM', site_id: '2']))

        assertThatSampleIsPresent('GA8015ZS-06', ['P15514': -1])
    }
}
