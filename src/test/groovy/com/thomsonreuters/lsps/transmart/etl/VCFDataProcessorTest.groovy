package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.sql.SqlMethods

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.*
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.not
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
        runScript('I2B2_PROCESS_VCF_DATA.sql')
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

    void assertSampleAssociated(String dataSetId, CharSequence sampleId, CharSequence probesetId) {
        use(SqlMethods) {
            def variant = db.findRecord('deapp.de_variant_subject_summary',
                    dataset_id: dataSetId, subject_id: sampleId, rs_id: probesetId)
            def sample = db.findRecord('deapp.de_subject_sample_mapping',
                    trial_name: studyId, sample_cd: sampleId)
            assertThat(variant.assay_id, equalTo(sample.assay_id))
        }
    }

    void assertThatVcfDataForSubject1IsLoaded(String dataSetId) {
        // verify deapp.de_variant_dataset
        assertThat(db, hasRecord('deapp.de_variant_dataset',
                dataset_id: dataSetId, etl_id: 'tMDataLoader', genome: 'hg19',
                etl_date: today()))
        // verify deapp.de_variant_subject_idx
        assertThat(db, hasRecord('deapp.de_variant_subject_idx', dataset_id: dataSetId, subject_id: 'VCF_TST001', position: 1))

        // verify deapp.de_variant_subject_summary
        assertThat(db, hasRecord('deapp.de_variant_subject_summary', dataset_id: dataSetId, subject_id: 'VCF_TST001',
                chr: '22', pos: 16050408, rs_id: 'rs149201999', variant_type: 'SNV',
                reference: true, variant: 'T|T', variant_format: 'R|R', allele1: 0, allele2: 0))
        assertThat(db, hasRecord('deapp.de_variant_subject_summary', dataset_id: dataSetId, subject_id: 'VCF_TST001',
                chr: '22', pos: 16050612, rs_id: 'rs146752890', variant_type: 'SNV',
                reference: false, variant: 'G/C', variant_format: 'V/R', allele1: 1, allele2: 0))
        assertThat(db, hasRecord('deapp.de_variant_subject_summary',
                [dataset_id: dataSetId, subject_id: 'VCF_TST001', chr: '22', pos: 16050616, rs_id: 'rs146752889'],
                [variant_type: 'SNV', reference: false, variant: 'G', variant_format: 'V', allele1: 1, allele2: null]))
        assertThat(db, hasRecord('deapp.de_variant_subject_summary',
                [dataset_id: dataSetId, subject_id: 'VCF_TST001', chr: '22', pos: 16050620, rs_id: 'rs146752880'],
                [variant_type: 'DIV', reference: false, variant: 'T/G', variant_format: 'V/V', allele1: 2, allele2: 1]))
        assertThat(db, not(hasRecord('deapp.de_variant_subject_summary',
                dataset_id: dataSetId, subject_id: 'VCF_TST001', chr: '22', pos: 16050624, rs_id: 'rs146752879')))

        assertSampleAssociated(dataSetId, 'VCF_TST001', 'rs149201999')
        assertSampleAssociated(dataSetId, 'VCF_TST001', 'rs146752890')
        assertSampleAssociated(dataSetId, 'VCF_TST001', 'rs146752889')
        assertSampleAssociated(dataSetId, 'VCF_TST001', 'rs146752880')

        // verify deapp.de_variant_population_info
        assertThat(db, hasRecord('deapp.de_variant_population_info',
                [dataset_id: dataSetId, info_name: 'LDAF'],
                [description: 'MLE Allele Frequency Accounting for LD', type: 'Float', number: '1']))
        // verify deapp.de_variant_population_data
        assertThat(db, not(hasRecord('deapp.de_variant_population_data',
                [dataset_id: dataSetId, chr: '22', pos:  16050620, info_name: 'UNKNW', info_index: 0],
                [integer_value: 42, float_value: null, text_value: null])))
        assertThat(db, hasRecord('deapp.de_variant_population_data',
                [dataset_id: dataSetId, chr: '22', pos:  16050624, info_name: 'TST_FLAG', info_index: 0],
                [integer_value: 0, float_value: null, text_value: null]))
        assertThat(db, hasRecord('deapp.de_variant_population_data',
                [dataset_id: dataSetId, chr: '22', pos:  16050408, info_name: 'LDAF', info_index: 0],
                [integer_value: null, float_value: 0.0649, text_value: null]))
        assertThat(db, hasRecord('deapp.de_variant_population_data',
                [dataset_id: dataSetId, chr: '22', pos:  16050408, info_name: 'AN', info_index: 0],
                [integer_value: 2184, float_value: null, text_value: null]))
        assertThat(db, hasRecord('deapp.de_variant_population_data',
                [dataset_id: dataSetId, chr: '22', pos:  16050408, info_name: 'VT', info_index: 0],
                [integer_value: null, float_value: null, text_value: 'SNP']))
    }

    void assertThatVcfDataForSubject2IsLoaded(String dataSetId, int position = 1) {
        // verify deapp.de_variant_dataset
        assertThat(db, hasRecord('deapp.de_variant_dataset',
                dataset_id: dataSetId, etl_id: 'tMDataLoader', genome: 'hg19',
                etl_date: today()))
        // verify deapp.de_variant_subject_idx
        assertThat(db, hasRecord('deapp.de_variant_subject_idx', dataset_id: dataSetId, subject_id: 'VCF_TST002', position: position))

        // verify deapp.de_variant_subject_summary
        assertThat(db, hasRecord('deapp.de_variant_subject_summary', dataset_id: dataSetId, subject_id: 'VCF_TST002',
                chr: '22', pos: 16050408, rs_id: 'rs149201999', variant_type: 'SNV',
                reference: false, variant: 'T|C', variant_format: 'R|V', allele1: 0, allele2: 1))
        assertThat(db, hasRecord('deapp.de_variant_subject_summary', dataset_id: dataSetId, subject_id: 'VCF_TST002',
                chr: '22', pos: 16050612, rs_id: 'rs146752890', variant_type: 'SNV',
                reference: true, variant: '/C', variant_format: '/R', allele1: null, allele2: 0))
        assertThat(db, hasRecord('deapp.de_variant_subject_summary',
                [dataset_id: dataSetId, subject_id: 'VCF_TST002', chr: '22', pos: 16050616, rs_id: 'rs146752889'],
                [variant_type: 'SNV', reference: true, variant: 'C', variant_format: 'R', allele1: 0, allele2: null]))
        assertThat(db, not(hasRecord('deapp.de_variant_subject_summary',
                dataset_id: dataSetId, subject_id: 'VCF_TST002', chr: '22', pos: 16050624, rs_id: 'rs146752879')))

        assertSampleAssociated(dataSetId, 'VCF_TST002', 'rs149201999')
        assertSampleAssociated(dataSetId, 'VCF_TST002', 'rs146752890')
        assertSampleAssociated(dataSetId, 'VCF_TST002', 'rs146752889')
        assertSampleAssociated(dataSetId, 'VCF_TST002', 'rs146752880')

        // verify deapp.de_variant_population_info
        assertThat(db, hasRecord('deapp.de_variant_population_info',
                [dataset_id: dataSetId, info_name: 'LDAF'],
                [description: 'MLE Allele Frequency Accounting for LD', type: 'Float', number: '1']))
        // verify deapp.de_variant_population_data
        assertThat(db, not(hasRecord('deapp.de_variant_population_data',
                [dataset_id: dataSetId, chr: '22', pos:  16050620, info_name: 'UNKNW', info_index: 0],
                [integer_value: 42, float_value: null, text_value: null])))
        assertThat(db, hasRecord('deapp.de_variant_population_data',
                [dataset_id: dataSetId, chr: '22', pos:  16050624, info_name: 'TST_FLAG', info_index: 0],
                [integer_value: 0, float_value: null, text_value: null]))
        assertThat(db, hasRecord('deapp.de_variant_population_data',
                [dataset_id: dataSetId, chr: '22', pos:  16050408, info_name: 'LDAF', info_index: 0],
                [integer_value: null, float_value: 0.0649, text_value: null]))
        assertThat(db, hasRecord('deapp.de_variant_population_data',
                [dataset_id: dataSetId, chr: '22', pos:  16050408, info_name: 'AN', info_index: 0],
                [integer_value: 2184, float_value: null, text_value: null]))
        assertThat(db, hasRecord('deapp.de_variant_population_data',
                [dataset_id: dataSetId, chr: '22', pos:  16050408, info_name: 'VT', info_index: 0],
                [integer_value: null, float_value: null, text_value: 'SNP']))
    }

    void testItLoadsVCFFileWithSNVData() {
        assertTrue(dataProcessor.process(Fixtures.vcfData, [name: studyName, node: $/Test Studies\${studyName}/$]))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\VCF\\VCFTest\\").withPatientCount(2))
        assertThat(db, hasSample(studyId, 'VCF_TST001', platform: 'VCF'))
        assertThat(db, hasSample(studyId, 'VCF_TST002', platform: 'VCF'))
        assertThat(db, hasPatient('Subject_0').inTrial(studyId))

        def dataSetId = "${studyId}:VCFTEST"
        assertThatVcfDataForSubject1IsLoaded(dataSetId)
        assertThatVcfDataForSubject2IsLoaded(dataSetId, 2)

        // verify deapp.de_variant_subject_detail
        assertThat(db, hasRecord('deapp.de_variant_subject_detail',
                [dataset_id: dataSetId, chr: '22', pos: 16050408, rs_id: 'rs149201999'],
                [ref: 'T', alt: 'C', qual: '100', filter: 'PASS',
                 info: 'LDAF=0.0649;RSQ=0.8652;AN=2184;ERATE=0.0046;VT=SNP;AA=.;AVGPOST=0.9799;THETA=0.0149;SNPSOURCE=LOWCOV;AC=134;AF=0.06;ASN_AF=0.04;AMR_AF=0.05;AFR_AF=0.10;EUR_AF=0.06',
                 format: 'GT:DS:GL', variant_value: '0|0:0.050:-0.03,-1.17,-5.00\t0|1:0.900:-0.71,-0.09,-5.00']))

        assertThat(db, hasRecord('deapp.de_variant_subject_detail',
                [dataset_id: dataSetId, chr: '22', pos: 16050620, rs_id: 'rs146752880'],
                [ref: 'C', alt: 'G,T', qual: '100', filter: 'PASS',
                 info: 'UNKNW=42;AC=184;RSQ=0.8228;AVGPOST=0.9640;AN=2184;ERATE=0.0031;VT=SNP;AA=.;THETA=0.0127;LDAF=0.0902;SNPSOURCE=LOWCOV;AF=0.08;ASN_AF=0.08;AMR_AF=0.14;AFR_AF=0.08;EUR_AF=0.07',
                 format: 'GT:DS:GL', variant_value: '2/1:1.000:-2.05,-0.01,-1.71\t./0:1.000:-0.86,-0.06,-5.00']))

        assertThat(db, hasRecord('deapp.de_variant_subject_detail',
                [dataset_id: dataSetId, chr: '22', pos: 16050624, rs_id: 'rs146752879'],
                [ref: 'C', alt: 'G', qual: '100', filter: 'PASS',
                 info: 'TST_FLAG=0;AC=184;RSQ=0.8228;AVGPOST=0.9640;AN=2184;ERATE=0.0031;VT=SNP;AA=.;THETA=0.0127;LDAF=0.0902;SNPSOURCE=LOWCOV;AF=0.08;ASN_AF=0.08;AMR_AF=0.14;AFR_AF=0.08;EUR_AF=0.07',
                 format: 'DS:GL', variant_value: '1.000:-2.05,-0.01,-1.71\t1.000:-0.86,-0.06,-5.00']))
    }

    void testItLoadsMultipleVcfFiles() {
        assertTrue(dataProcessor.process(Fixtures.multipleVcfData, [name: studyName, node: $/Test Studies\${studyName}/$]))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\VCF\\VCFTest1\\").withPatientCount(1))
        assertThat(db, hasNode("\\Test Studies\\${studyName}\\VCF\\VCFTest2\\").withPatientCount(1))
        assertThat(db, hasSample(studyId, 'VCF_TST001', platform: 'VCF'))
        assertThat(db, hasSample(studyId, 'VCF_TST002', platform: 'VCF'))
        assertThat(db, hasPatient('Subject_0').inTrial(studyId))

        def dataSet1Id = "${studyId}:VCFTEST1"
        assertThatVcfDataForSubject1IsLoaded(dataSet1Id)

        // verify deapp.de_variant_subject_detail
        assertThat(db, hasRecord('deapp.de_variant_subject_detail',
                [dataset_id: dataSet1Id, chr: '22', pos: 16050408, rs_id: 'rs149201999'],
                [ref: 'T', alt: 'C', qual: '100', filter: 'PASS',
                 info: 'LDAF=0.0649;RSQ=0.8652;AN=2184;ERATE=0.0046;VT=SNP;AA=.;AVGPOST=0.9799;THETA=0.0149;SNPSOURCE=LOWCOV;AC=134;AF=0.06;ASN_AF=0.04;AMR_AF=0.05;AFR_AF=0.10;EUR_AF=0.06',
                 format: 'GT:DS:GL', variant_value: '0|0:0.050:-0.03,-1.17,-5.00']))

        assertThat(db, hasRecord('deapp.de_variant_subject_detail',
                [dataset_id: dataSet1Id, chr: '22', pos: 16050620, rs_id: 'rs146752880'],
                [ref: 'C', alt: 'G,T', qual: '100', filter: 'PASS',
                 info: 'UNKNW=42;AC=184;RSQ=0.8228;AVGPOST=0.9640;AN=2184;ERATE=0.0031;VT=SNP;AA=.;THETA=0.0127;LDAF=0.0902;SNPSOURCE=LOWCOV;AF=0.08;ASN_AF=0.08;AMR_AF=0.14;AFR_AF=0.08;EUR_AF=0.07',
                 format: 'GT:DS:GL', variant_value: '2/1:1.000:-2.05,-0.01,-1.71']))

        assertThat(db, hasRecord('deapp.de_variant_subject_detail',
                [dataset_id: dataSet1Id, chr: '22', pos: 16050624, rs_id: 'rs146752879'],
                [ref: 'C', alt: 'G', qual: '100', filter: 'PASS',
                 info: 'TST_FLAG=0;AC=184;RSQ=0.8228;AVGPOST=0.9640;AN=2184;ERATE=0.0031;VT=SNP;AA=.;THETA=0.0127;LDAF=0.0902;SNPSOURCE=LOWCOV;AF=0.08;ASN_AF=0.08;AMR_AF=0.14;AFR_AF=0.08;EUR_AF=0.07',
                 format: 'DS:GL', variant_value: '1.000:-2.05,-0.01,-1.71']))


        def dataSet2Id = "${studyId}:VCFTEST2"
        assertThatVcfDataForSubject2IsLoaded(dataSet2Id)

        // verify deapp.de_variant_subject_detail
        assertThat(db, hasRecord('deapp.de_variant_subject_detail',
                [dataset_id: dataSet2Id, chr: '22', pos: 16050408, rs_id: 'rs149201999'],
                [ref: 'T', alt: 'C', qual: '100', filter: 'PASS',
                 info: 'LDAF=0.0649;RSQ=0.8652;AN=2184;ERATE=0.0046;VT=SNP;AA=.;AVGPOST=0.9799;THETA=0.0149;SNPSOURCE=LOWCOV;AC=134;AF=0.06;ASN_AF=0.04;AMR_AF=0.05;AFR_AF=0.10;EUR_AF=0.06',
                 format: 'GT:DS:GL', variant_value: '0|1:0.900:-0.71,-0.09,-5.00']))

        assertThat(db, hasRecord('deapp.de_variant_subject_detail',
                [dataset_id: dataSet2Id, chr: '22', pos: 16050620, rs_id: 'rs146752880'],
                [ref: 'C', alt: 'G,T', qual: '100', filter: 'PASS',
                 info: 'UNKNW=42;AC=184;RSQ=0.8228;AVGPOST=0.9640;AN=2184;ERATE=0.0031;VT=SNP;AA=.;THETA=0.0127;LDAF=0.0902;SNPSOURCE=LOWCOV;AF=0.08;ASN_AF=0.08;AMR_AF=0.14;AFR_AF=0.08;EUR_AF=0.07',
                 format: 'GT:DS:GL', variant_value: './0:1.000:-0.86,-0.06,-5.00']))

        assertThat(db, hasRecord('deapp.de_variant_subject_detail',
                [dataset_id: dataSet2Id, chr: '22', pos: 16050624, rs_id: 'rs146752879'],
                [ref: 'C', alt: 'G', qual: '100', filter: 'PASS',
                 info: 'TST_FLAG=0;AC=184;RSQ=0.8228;AVGPOST=0.9640;AN=2184;ERATE=0.0031;VT=SNP;AA=.;THETA=0.0127;LDAF=0.0902;SNPSOURCE=LOWCOV;AF=0.08;ASN_AF=0.08;AMR_AF=0.14;AFR_AF=0.08;EUR_AF=0.07',
                 format: 'DS:GL', variant_value: '1.000:-0.86,-0.06,-5.00']))
    }
}
