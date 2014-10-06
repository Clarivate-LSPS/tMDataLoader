package com.thomsonreuters.lsps.transmart.etl

class MetabolomicsDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private MetabolomicsDataProcessor _processor
    String studyName = 'Test Metabolomics Study'
    String studyId = 'GSE37427'
    String platformId = 'MET998'

    MetabolomicsDataProcessor getProcessor() {
        _processor ?: (_processor = new MetabolomicsDataProcessor(config))
    }

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ? or sourcesystem_cd = ?', studyId, studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        runScript('I2B2_LOAD_METABOLOMICS_ANNOT.sql')
        runScript('I2B2_PROCESS_METABOLOMIC_DATA.sql')
    }


    void testItLoadsData() {
        processor.process(
                new File("fixtures/Test Studies/${studyName}/MetabolomicsDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
    }
}
