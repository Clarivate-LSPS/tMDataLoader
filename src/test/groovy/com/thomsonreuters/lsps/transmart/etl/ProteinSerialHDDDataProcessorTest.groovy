package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.fixtures.Study
import spock.lang.Specification

import static com.thomsonreuters.lsps.transmart.Fixtures.studyDir
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static org.junit.Assert.assertThat

class ProteinSerialHDDDataProcessorTest extends Specification implements ConfigAwareTestCase {

    String studyName = 'Test Protein Study'
    String studyId = 'GSE37425'

    void setup() {
        ConfigAwareTestCase.super.setUp()
        Study.deleteById(config, studyId)
        sql.execute('delete from deapp.de_subject_sample_mapping where trial_name = ?', studyId)
    }

    def 'it should load serial hdd data'() {
        when:
            def processor = new ProteinSerialHDDDataProcessor(config)
            processor.process(new File(studyDir(studyName, studyId), 'ProteinSerialHDDDataToUpload').toPath(),
                    [name: studyName, node: "Test Studies\\${studyName}".toString()])
        then:
        def testNodeName = "\\Test Studies\\Test Protein Study\\Sample Factors Week 1\\"
        def testMetadata = """<?xml version="1.0"?>
                        <ValueMetadata>
                            <Oktousevalues>Y</Oktousevalues>
                            <SeriesMeta>
                                <Value>7</Value>
                                <Unit>days</Unit>
                                <DisplayName>Week 1</DisplayName>
                            </SeriesMeta>
                    </ValueMetadata>"""

        assertThat(db, hasRecord('i2b2metadata.i2b2',
                [c_fullname: testNodeName, c_name: 'Sample Factors Week 1', sourcesystem_cd: studyId],
                [c_visualattributes: 'LAH', c_metadataxml: testMetadata]))

        assertThat(db, hasRecord('i2b2demodata.sample_dimension', [sample_cd: 'O002311'], null))

        assertThat(db, hasRecord('i2b2demodata.observation_fact', [sample_cd: 'P516591', sourcesystem_cd: studyId], null))
        assertThat(db, hasRecord('i2b2demodata.observation_fact', [sample_cd: 'O002311', sourcesystem_cd: studyId], null))
        assertThat(db, hasRecord('i2b2demodata.observation_fact', [sample_cd: 'P504401', sourcesystem_cd: studyId], null))
        assertThat(db, hasRecord('i2b2demodata.observation_fact', [sample_cd: 'P378021', sourcesystem_cd: studyId], null))
        assertThat(db, hasRecord('i2b2demodata.observation_fact', [sample_cd: 'P026471', sourcesystem_cd: studyId], null))
    }
}
