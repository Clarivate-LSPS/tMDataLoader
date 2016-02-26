package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.db.core.DatabaseType

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.*
import static org.hamcrest.CoreMatchers.not
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 4/3/14.
 */
class I2B2LoadSamplesTest extends GroovyTestCase implements ConfigAwareTestCase {
    String trialId = 'GSE_TST_LDR'
    String platform = 'LDR_TST'

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        // reload procedure
        if (database.databaseType != DatabaseType.Oracle) {
            runScript('I2B2_LOAD_SAMPLES.sql')
        }
    }

    void testItLoadSamples() {
        if (database.databaseType == DatabaseType.Oracle)
            return;

        def samplesLoader = new SamplesLoader(trialId)
        samplesLoader.addSample('LDR+PLATFORM+TISSUETYPE', 'LDR_TST_SUBJ_001', 'LDR_TST_SMP_001', platform)
        samplesLoader.addSample('LDR+PLATFORM+TISSUETYPE', 'LDR_TST_SUBJ_002', 'LDR_TST_SMP_002', null, tissueType: 'Blood')
        samplesLoader.loadSamples(database, db)

        insertIfNotExists('deapp.de_gpl_info', [platform: platform, title: 'Loader Test Platform',
                                                organism: 'Homo Sapiens', marker_type: 'Gene Expression'])
        withAudit('testItLoadSamples') { jobId ->
            callProcedure("${config.controlSchema}.i2b2_load_samples",
                    trialId, 'Test Studies\\Loader Test', 'LDR', 'STD', 'N', jobId)
        }
        assertThat(db, hasSample(trialId, 'LDR_TST_SMP_001'))
        assertThat(db, hasPatient('LDR_TST_SUBJ_001').inTrial(trialId))
        assertThat(db, hasNode('\\Test Studies\\Loader Test\\LDR\\Loader Test Platform\\').withPatientCount(1))
        assertThat(db, hasNode('\\Test Studies\\Loader Test\\LDR\\Blood\\').withPatientCount(1))
        assertThat(db, not(hasNode('\\Test Studies\\Loader Test\\LDR\\Loader Test Platform\\Blood\\')))
    }
}
