package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasPatient
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 4/3/14.
 */
class VCFDataProcessorTest extends ConfigAwareTestCase {
    String studyName = 'Test Study'
    String studyId = 'GSE0'

    @Override
    void setUp() {
        super.setUp()
        runScript('VCF_CREATE_TABLES.sql')
        runScript('I2B2_PROCESS_VCF_DATA.sql')
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ?', studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        sql.execute('delete from deapp.de_variant_subject_idx where dataset_id = ?', studyId)
        sql.execute('delete from deapp.de_variant_dataset where dataset_id = ?', studyId)
    }

    private VCFDataProcessor _dataProcessor

    VCFDataProcessor getDataProcessor() {
        _dataProcessor ?: (_dataProcessor = new VCFDataProcessor(config))
    }

    Calendar today() {
        Calendar cal = Calendar.getInstance()
        cal.clear(Calendar.HOUR)
        cal.clear(Calendar.MINUTE)
        cal.clear(Calendar.SECOND)
        cal.clear(Calendar.MILLISECOND)
        cal
    }

    void testItLoadsVCFFile() {
        assertTrue(dataProcessor.process(Fixtures.vcfData, [name: studyName, node: $/Test Studies\${studyName}/$]))
        assertThat(db, hasSample(studyId, 'VCF_TST001', platform: 'VCF'))
        assertThat(db, hasSample(studyId, 'VCF_TST002', platform: 'VCF'))
        assertThat(db, hasPatient('Subject_0').inTrial(studyId))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\VCF\\VCFTest\\").withPatientCount(2))
        assertThat(db, hasRecord('deapp.de_variant_dataset',
                dataset_id: studyId, etl_id: 'tMDataLoader', genome: 'hg19',
                etl_date: today()))
        assertThat(db, hasRecord('deapp.de_variant_subject_idx', dataset_id: studyId, subject_id: 'VCF_TST001', position: 1))
        assertThat(db, hasRecord('deapp.de_variant_subject_idx', dataset_id: studyId, subject_id: 'VCF_TST002', position: 2))
    }
}
