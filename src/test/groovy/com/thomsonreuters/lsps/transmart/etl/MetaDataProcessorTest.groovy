package com.thomsonreuters.lsps.transmart.etl

import static com.thomsonreuters.lsps.transmart.Fixtures.studyDir
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static org.junit.Assert.assertThat

class MetaDataProcessorTest extends GroovyTestCase implements ConfigAwareTestCase {
    private MetaDataProcessor _processor;

    String studyName = 'Test Study'
    String studyFolderName = studyName + 'Meta'
    String studyId = 'GSE0'

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        runScript('i2b2_load_study_metadata.sql')
    }

    MetaDataProcessor getProcessor() {
        _processor ?: (_processor = new MetaDataProcessor(config))
    }

    void testItLoadsData() {
        withErrorLogging {
            processor.process(new File(studyDir(studyName, studyId), "MetaDataToUpload"),
                    [name: studyName, node: "Test Studies\\${studyFolderName}".toString()])
        }

        assertThat(db, hasRecord('biomart.bio_experiment',
                [accession: studyId], [primary_investigator : 'owner']))
    }
}
