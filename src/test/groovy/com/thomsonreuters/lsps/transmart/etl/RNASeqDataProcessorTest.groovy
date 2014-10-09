package com.thomsonreuters.lsps.transmart.etl


class RNASeqDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private RNASeqDataProcessor _processor
    String studyName = 'Test RNASeq Study'
    String studyId = 'GSE_A_37424'
    String platformId = 'RNASeq999'

    RNASeqDataProcessor getProcessor() {
        _processor ?: (_processor = new RNASeqDataProcessor(config))
    }

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ? or sourcesystem_cd = ?', studyId, studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        runScript('I2B2_PROCESS_RNA_SEQ_DATA.sql')
    }


    void testItLoadsData() {
        processor.process(
                new File("fixtures/Test Studies/${studyName}/RNASeqDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
    }
}
