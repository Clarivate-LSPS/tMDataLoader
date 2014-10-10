package com.thomsonreuters.lsps.transmart.etl

class RBMDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private RBMDataProcessor _processor
    String studyName = 'Test RBM Study'
    String studyId = 'TESTRBM'
    String platformId = 'RBM100'

    RBMDataProcessor getProcessor() {
        _processor ?: (_processor = new RBMDataProcessor(config))
    }

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ? or sourcesystem_cd = ?', studyId, studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        runScript('I2B2_LOAD_RBM_DATA.sql')
        runScript('I2B2_RBM_ZSCORE_CALC_NEW.sql')
    }


    void testItLoadsData() {
        processor.process(
                new File("fixtures/Test Studies/${studyName}/RBMDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
    }
}
