package com.thomsonreuters.lsps.transmart.etl

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasPatient
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 4/3/14.
 */
class I2B2LoadSamplesTest extends ConfigAwareTestCase {
    String trialId = 'GSE_TST_LDR'
    String platform = 'LDR_TST'

    def samples = [
            [
                    trialId, // TRIAL_NAME
                    '', // SITE_ID
                    'LDR_TST_SUBJ_001', // SUBJECT_ID
                    'LDR_TST_SMP_001', // SAMPLE_CD
                    platform, // PLATFORM
                    '', // TISSUE_TYPE
                    '', // ATTRIBUTE_1
                    '', // ATTRIBUTE_2
                    'LDR+PLATFORM+TISSUETYPE', // CATEGORY_CD
            ]
    ]

    @Override
    void setUp() {
        super.setUp()
        // reload procedure
        runScript('I2B2_LOAD_SAMPLES.sql')
    }

    void testItLoadSamples() {
        def sampleLoader = new SamplesLoader(trialId)
        sampleLoader.addSample('LDR+PLATFORM+TISSUETYPE', 'LDR_TST_SUBJ_001', 'LDR_TST_SMP_001', platform)
        sampleLoader.loadSamples(db)

        insertIfNotExists('deapp.de_gpl_info', [platform: platform, title: 'Loader Test Platform',
                                                organism: 'Homo Sapiens', marker_type: 'Gene Expression'])
        withAudit('testItLoadSamples') { jobId ->
            callProcedure("${config.controlSchema}.i2b2_load_samples",
                    trialId, 'Test Studies\\Loader Test', 'LDR', 'STD', 'N', jobId)
        }
        assertThat(db, hasSample(trialId, 'LDR_TST_SMP_001'))
        assertThat(db, hasPatient('LDR_TST_SUBJ_001').inTrial(trialId))
        assertThat(db, hasNode('\\Test Studies\\Loader Test\\LDR\\Loader Test Platform\\').withPatientCount(1))
    }
}
