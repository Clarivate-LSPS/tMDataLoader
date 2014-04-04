package com.thomsonreuters.lsps.transmart.etl

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

    void testItLoadSamples() {
        db.execute('delete from tm_lz.lt_src_mrna_subj_samp_map')
        db.withBatch(
                """
                INSERT into tm_lz.lt_src_mrna_subj_samp_map
                (TRIAL_NAME, SITE_ID, SUBJECT_ID, SAMPLE_CD, PLATFORM, TISSUE_TYPE,
                 ATTRIBUTE_1, ATTRIBUTE_2, CATEGORY_CD, SOURCE_CD)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'STD')
                """
        ) { batch ->
            samples.each { batch.addBatch(it) }
        }
        insertIfNotExists('deapp.de_gpl_info', [platform: platform, title: 'Loader Test Platform',
                                                organism: 'Homo Sapiens', marker_type: 'Gene Expression'])

        runScript('I2B2_LOAD_SAMPLES.sql')
        withAudit('testItLoadSamples') { jobId ->
            callProcedure("${config.controlSchema}.i2b2_load_samples",
                    trialId, 'Test Studies', 'LDR', 'STD', 'N', jobId)
        }
        assertThat(db, hasSample(trialId, 'LDR_TST_SMP_001'))
    }
}
