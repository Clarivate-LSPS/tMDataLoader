package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.fixtures.Study
import spock.lang.Specification

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static org.junit.Assert.assertThat

class MetabolomicsSerialHDDDataProcessorTest extends Specification implements ConfigAwareTestCase {

    String studyName = 'Test Metabolomics Study'
    String studyId = 'GSE37427'

    void setup() {
        ConfigAwareTestCase.super.setUp()
        Study.deleteById(config, studyId)
    }

    def 'it should load metabolics serial hdd data'() {
        when:
        def processor = new MetabolomicsSerialHDDDataProcessor(config)
        processor.process(new File("fixtures/Test Studies/${studyName}/MetabolomicsSerialHDDDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        then:
        def testNodeName = "\\Test Studies\\${studyName}\\Sample Factors Week 1\\"
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

        assertThat(db, hasRecord('i2b2demodata.sample_dimension', [sample_cd: '10CP'], null))
        assertThat(db, hasRecord('i2b2demodata.sample_dimension', [sample_cd: '12PP'], null))

        assertThat(db, hasRecord('i2b2demodata.observation_fact', [sample_cd: '10CP', sourcesystem_cd: studyId], null))
        assertThat(db, hasRecord('i2b2demodata.observation_fact', [sample_cd: '12PP', sourcesystem_cd: studyId], null))
        assertThat(db, hasRecord('i2b2demodata.observation_fact', [sample_cd: '18PP', sourcesystem_cd: studyId], null))
    }
}
