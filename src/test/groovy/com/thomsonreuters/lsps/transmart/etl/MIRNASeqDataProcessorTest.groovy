package com.thomsonreuters.lsps.transmart.etl

class MIRNASeqDataProcessorTest extends ConfigAwareTestCase {
    private MIRNADataProcessor _processor
    String studyName = 'Test MirnaSeq Study'
    String studyId = 'mirnaseqbased'
    String platformId = 'GPL15467seqbased'

    MIRNADataProcessor getProcessor() {
        _processor ?: (_processor = new MIRNADataProcessor(config, 'MIRNA_SEQ'))
    }

    @Override
    void setUp() {
        super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ?', studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        runScript('I2B2_PROCESS_QPCR_MIRNA_DATA.sql')
    }


    void testItLoadsData() {
        processor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/MIRNASeqDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
    }
}
