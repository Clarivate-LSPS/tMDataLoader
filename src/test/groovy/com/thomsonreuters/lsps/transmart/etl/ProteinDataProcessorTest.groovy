package com.thomsonreuters.lsps.transmart.etl

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasPatient
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertThat

class ProteinDataProcessorTest extends ConfigAwareTestCase {
    private ProteinDataProcessor _processor
    String studyName = 'Test Protein Study'
    String studyId = 'GSE37425'
    String platformId = 'RBM999'

    ProteinDataProcessor getProcessor() {
        _processor ?: (_processor = new ProteinDataProcessor(config))
    }

    @Override
    void setUp() {
        super.setUp()
        sql.execute('delete from i2b2demodata.observation_fact where modifier_cd = ? or sourcesystem_cd = ?', studyId, studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
        runScript('I2B2_PROCESS_PROTEOMICS_DATA.sql')
    }


    void testItLoadsData() {
        processor.process(
                new File("fixtures/Test Studies/${studyName}/ProteinDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
    }

     /*INSERT INTO biomart.bio_marker(
     bio_marker_name, bio_marker_description, organism, primary_external_id, bio_marker_type)
     VALUES ('P51659', 'P51659', 'HOMO SAPIENS', 'P51659', 'PROTEIN'),
     ('O00231', 'O00231', 'HOMO SAPIENS', 'O00231', 'PROTEIN'),
     ('P50440', 'P50440', 'HOMO SAPIENS', 'P50440', 'PROTEIN'),
     ('P37802', 'P37802', 'HOMO SAPIENS', 'P37802', 'PROTEIN'),
     ('P02647', 'P02647', 'HOMO SAPIENS', 'P02647', 'PROTEIN');*/
}
