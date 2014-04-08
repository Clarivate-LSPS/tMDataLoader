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
        sql.execute('delete from deapp.de_variant_subject_summary where dataset_id = ?', studyId)
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

    void testItLoadsVCFFileWithSNVData() {
        assertTrue(dataProcessor.process(Fixtures.vcfData, [name: studyName, node: $/Test Studies\${studyName}/$]))
        assertThat(db, hasSample(studyId, 'VCF_TST001', platform: 'VCF'))
        assertThat(db, hasSample(studyId, 'VCF_TST002', platform: 'VCF'))
        assertThat(db, hasPatient('Subject_0').inTrial(studyId))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\VCF\\VCFTest\\").withPatientCount(2))
        // verify deapp.de_variant_dataset
        assertThat(db, hasRecord('deapp.de_variant_dataset',
                dataset_id: studyId, etl_id: 'tMDataLoader', genome: 'hg19',
                etl_date: today()))
        // verify deapp.de_variant_subject_idx
        assertThat(db, hasRecord('deapp.de_variant_subject_idx', dataset_id: studyId, subject_id: 'VCF_TST001', position: 1))
        assertThat(db, hasRecord('deapp.de_variant_subject_idx', dataset_id: studyId, subject_id: 'VCF_TST002', position: 2))
        // verify deapp.de_variant_subject_summary
        //FIXME: samples for '/' & no separator
        //FIXME: check for allele eq to .
        //FIXME: check for multiple alternatives
        assertThat(db, hasRecord('deapp.de_variant_subject_summary', dataset_id: studyId, subject_id: 'VCF_TST001',
                chr: '22', pos: 16050408, rs_id: 'rs149201999', variant_type: 'SNV',
                reference: true, variant: 'T|T', variant_format: 'R|R', allele1: 0, allele2: 0))
        assertThat(db, hasRecord('deapp.de_variant_subject_summary', dataset_id: studyId, subject_id: 'VCF_TST002',
                chr: '22', pos: 16050408, rs_id: 'rs149201999', variant_type: 'SNV',
                reference: false, variant: 'T|C', variant_format: 'R|V', allele1: 0, allele2: 1))
        assertThat(db, hasRecord('deapp.de_variant_subject_summary', dataset_id: studyId, subject_id: 'VCF_TST001',
                chr: '22', pos: 16050612, rs_id: 'rs146752890', variant_type: 'SNV',
                reference: false, variant: 'G/C', variant_format: 'V/R', allele1: 1, allele2: 0))
        assertThat(db, hasRecord('deapp.de_variant_subject_summary', dataset_id: studyId, subject_id: 'VCF_TST002',
                chr: '22', pos: 16050612, rs_id: 'rs146752890', variant_type: 'SNV',
                reference: true, variant: '/C', variant_format: '/R', allele1: null, allele2: 0))
    }
}
