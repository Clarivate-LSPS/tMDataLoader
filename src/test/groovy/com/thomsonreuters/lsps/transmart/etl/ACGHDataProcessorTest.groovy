package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.fixtures.Study

import static com.thomsonreuters.lsps.transmart.Fixtures.studyDir
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.junit.Assert.assertThat

/**
 * Created by transmart on 3/10/15.
 */
class ACGHDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private ACGHDataProcessor _processor

    String studyName = 'Test Study'
    String studyFolderName = studyName + ' ACGH'
    String studyId = 'TSTA'

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
        Study.deleteById(config, studyId)
        withErrorLogging {
            processor.process(new File(studyDir(studyName, 'GSE0'), "ACGHDataToUpload").toPath(),
                    [name: studyName, node: "Test Studies\\${studyFolderName}".toString()])
        }
        assertThat(db, hasSample(studyId, 'TSGA-04-1530', platform: 'ACGH'))
        assertThat(db, hasRecord('deapp.de_subject_acgh_data',
                [trial_name: studyId, chip: 0.097],
                [segmented: 0.097, flag: 0]))
    }
}
