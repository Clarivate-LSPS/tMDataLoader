package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.sql.SqlMethods

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasPatient
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.hamcrest.CoreMatchers.equalTo
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

    void assertSampleAssociated(CharSequence sampleId, CharSequence probesetId) {
        use(SqlMethods) {
            def variant = db.findRecord('deapp.de_variant_subject_summary',
                    dataset_id: studyId, subject_id: sampleId, rs_id: probesetId)
            def sample = db.findRecord('deapp.de_subject_sample_mapping',
                    trial_name: studyId, sample_cd: sampleId)
            assertThat(variant.assay_id, equalTo(sample.assay_id))
        }
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
        //TODO: samples for '/' & no separator
        //TODO: check for allele eq to .
        //TODO: check for multiple alternatives
        //TODO: check for missing GT format
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
        assertSampleAssociated('VCF_TST001', 'rs149201999')
        assertSampleAssociated('VCF_TST001', 'rs146752890')
        assertSampleAssociated('VCF_TST002', 'rs149201999')
        assertSampleAssociated('VCF_TST002', 'rs146752890')
        // verify deapp.de_variant_subject_detail
        assertThat(db, hasRecord('deapp.de_variant_subject_detail', [dataset_id: studyId,
                chr: '22', pos: 16050408, rs_id: 'rs149201999'], [ref: 'T', alt: 'C', qual: '100', filter: 'PASS',
                info: 'LDAF=0.0649;RSQ=0.8652;AN=2184;ERATE=0.0046;VT=SNP;AA=.;AVGPOST=0.9799;THETA=0.0149;SNPSOURCE=LOWCOV;AC=134;AF=0.06;ASN_AF=0.04;AMR_AF=0.05;AFR_AF=0.10;EUR_AF=0.06',
                format: 'GT:DS:GL', variant_value: '0|0:0.050:-0.03,-1.17,-5.00\t0|1:0.900:-0.71,-0.09,-5.00']))
        // verify deapp.de_variant_population_info
        assertThat(db, hasRecord('deapp.de_variant_population_info', dataset_id: studyId,
                info_name: 'LDAF', description: 'MLE Allele Frequency Accounting for LD', type: 'Float', number: 1))
        // verify deapp.de_variant_population_data
        //TODO: check for Flag info field type
        //TODO: check for non-existent field
        assertThat(db, hasRecord('deapp.de_variant_population_data', dataset_id: studyId,
                chr: '22', pos:  16050408, info_name: 'LDAF', info_index: 0,
                integer_value: null, float_value: 0.0649, text_value: null))
        assertThat(db, hasRecord('deapp.de_variant_population_data', dataset_id: studyId,
                chr: '22', pos:  16050408, info_name: 'AN', info_index: 0,
                integer_value: 2184, float_value: null, text_value: null))
        assertThat(db, hasRecord('deapp.de_variant_population_data', dataset_id: studyId,
                chr: '22', pos:  16050408, info_name: 'VT', info_index: 0,
                integer_value: null, float_value: null, text_value: 'SNP'))
    }
}
