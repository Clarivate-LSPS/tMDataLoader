package com.thomsonreuters.lsps.transmart.etl

import static com.thomsonreuters.lsps.transmart.Fixtures.studyDir
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasPatient
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.junit.Assert.assertThat
import static org.junit.Assert.assertThat
import static org.junit.Assert.assertThat
import static org.junit.Assert.assertThat
import static org.junit.Assert.assertThat

/**
 * Created by transmart on 3/10/15.
 */
class ACGHDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private ACGHDataProcessor _processor

    String studyName = 'Test Study'
    String studyId = 'GSE0'
    String trialName = 'TSTA'

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        runScript('I2B2_PROCESS_ACGH_DATA.sql')
        runScript('I2B2_LOAD_CHROM_REGION.sql')
    }

    ACGHDataProcessor getProcessor() {
        _processor ?: (_processor = new ACGHDataProcessor(config))
    }

    void testItLoadsData() {
        withErrorLogging {
            processor.process(new File(studyDir(studyName, studyId), "ACGHDataToUpload"),
                    [name: studyName, node: "Test Studies\\${studyName}".toString()])
        }
        assertThat(db, hasSample(trialName, 'TSGA-04-1530', platform: 'ACGH'))
        assertThat(db, hasRecord('deapp.de_subject_acgh_data',
                [trial_name: trialName, chip: 0.097],
                [segmented: 0.097, flag: 0]))
    }
}
