package com.thomsonreuters.lsps.transmart.etl

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.*
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertThat

/**
 * Created by bondarev on 2/24/14.
 */
class SerialHDDDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private SerialHDDDataProcessor _processor

    String studyName = 'Test SerialHDD Study'
    String studyId = 'BSI201RICERCA'
    String platformId = 'HDD999'

    SerialHDDDataProcessor getProcessor() {
        _processor ?: (_processor = new SerialHDDDataProcessor(config))
    }

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ?', studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        runScript('I2B2_PROCESS_MRNA_DATA.sql')
        runScript('I2B2_PROCESS_SERIAL_HDD_DATA.sql')
        runScript('I2B2_LOAD_SAMPLES.sql')
    }

    void testItLoadsData() {
        processor.process(
                new File("fixtures/Test Studies/${studyName}/SerialHDDDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

    }
}
