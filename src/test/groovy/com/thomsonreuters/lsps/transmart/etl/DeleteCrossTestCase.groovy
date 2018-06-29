package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.Study
import spock.lang.Specification

import static com.thomsonreuters.lsps.transmart.Fixtures.studyDir
import static org.hamcrest.CoreMatchers.equalTo
import static org.junit.Assert.*

class DeleteCrossTestCase extends Specification implements ConfigAwareTestCase {

    private ClinicalDataProcessor _clinicalProcessor
    private DeleteCrossProcessor _deleteCrossProcessor
    private DeleteDataProcessor _deleteDataProcessor

    def studyConceptId = "GSECONCEPTCD"
    def studyConceptName = 'Test Data With Concept_cd'

    DeleteDataProcessor getDeleteDataProcessor() {
        _deleteDataProcessor ?: (_deleteDataProcessor = new DeleteDataProcessor(config))
    }

    DeleteCrossProcessor getDeleteCrossProcessor() {
        _deleteCrossProcessor ?: (_deleteCrossProcessor = new DeleteCrossProcessor(config))
    }

    ClinicalDataProcessor getClinicalProcessor() {
        _clinicalProcessor ?: (_clinicalProcessor = new ClinicalDataProcessor(config))
    }

    void setup() {
        ConfigAwareTestCase.super.setUp()
        runScript('I2B2_DELETE_CROSS_DATA.sql')
        runScript('I2B2_DELETE_ALL_DATA.sql')
        runScript('I2B2_DELETE_ALL_NODES.sql')

        Study.deleteById(config, studyConceptId)
    }

    def 'it should not delete cross node from i2b2 schema'() {
        given:
        Fixtures.clinicalDataWithCrossNode.load(config, "Test Studies")

        when:
        def data = [
                path            : "\\Vital\\",
                isDeleteConcepts: false
        ]

        def operation = deleteCrossProcessor.process(data)

        then:
        assertThat("Should check procedure result", operation, equalTo(false))
        assertThatCrossNodeDelete(data.path, false)
        assertThatCrossNodeDelete(data.path + "Node 1\\", false)
        assertThatConceptDelete(data.path + "Node 1\\", false)
        assertThatConceptDelete(data.path + "Node 1\\Node 2\\Flag\\", false)
    }

    def 'it should delete cross node from i2b2 schema'() {
        given:
        Fixtures.clinicalDataWithCrossNode.load(config, "Test Studies")

        when:
        // Remove study before removing cross node
        deleteDataProcessor.process([id: studyConceptId])

        def data = [
                path            : "\\Vital\\",
                isDeleteConcepts: false
        ]

        def operation = deleteCrossProcessor.process(data)

        then:
        assertTrue(operation)

        assertThatCrossNodeDelete(data.path)
        assertThatCrossNodeDelete(data.path + "Node 1\\Node 2\\Flag\\")
        assertThatConceptDelete(data.path + "Node 1\\", false)
        assertThatConceptDelete(data.path + "Node 1\\Node 2\\Flag\\", false)
    }

    def 'it should delete cross node from i2b2 schema and concept dimension'() {
        given:
        Fixtures.clinicalDataWithCrossNode.load(config, "Test Studies")

        when:
        // Remove study before removing cross node
        deleteDataProcessor.process([id: studyConceptId])

        def data = [
                path            : "\\Vital\\",
                isDeleteConcepts: true
        ]

        def operation = deleteCrossProcessor.process(data)

        then:
        assertTrue(operation)

        assertThatCrossNodeDelete(data.path)
        assertThatCrossNodeDelete(data.path + "Node 1\\Node 2\\Flag\\")
        assertThatConceptDelete(data.path + "Node 1\\")
        assertThatConceptDelete(data.path + "Node 1\\Node 2\\Flag\\")
    }

    def 'it should delete concepts after deleting tree and study'() {
        given:
        Fixtures.clinicalDataWithCrossNode.load(config, "Test Studies")

        // Remove study before removing cross node
        deleteDataProcessor.process([id: studyConceptId])

        // Remove cross tree
        def data = [
                path            : "\\Vital\\",
                isDeleteConcepts: false
        ]

        deleteCrossProcessor.process(data)
        assertThatConceptDelete(data.path + "Node 1\\", false)
        assertThatConceptDelete(data.path + "Node 1\\Node 2\\Flag\\", false)

        when:
        // Remove cross tree
        data = [
                path            : "\\Vital\\",
                isDeleteConcepts: true
        ]
        def operation = deleteCrossProcessor.process(data)

        then:
        assertTrue(operation)

        assertThatCrossNodeDelete(data.path)
        assertThatCrossNodeDelete(data.path + "Node 1\\Node 2\\Flag\\")
        assertThatConceptDelete(data.path + "Node 1\\")
        assertThatConceptDelete(data.path + "Node 1\\Node 2\\Flag\\")
    }

    def 'it should check throw exception then cross node delete by path'() {
        given:
        Fixtures.clinicalDataWithCrossNode.load(config, "Test Studies")

        when:
        def data = [path: '\\Vital\\']
        def operation = deleteDataProcessor.process(data)

        then:
        assertFalse(operation)

        assertThatCrossNodeDelete(data.path, false)
        assertThatCrossNodeDelete(data.path + "Node 1\\Node 2\\Flag\\", false)
        assertThatConceptDelete(data.path + "Node 1\\", false)
        assertThatConceptDelete(data.path + "Node 1\\Node 2\\Flag\\", false)
    }

    def 'it should check remove data from observation_fact table'() {
        given:
        def clinical = Fixtures.clinicalDataWithCrossNode
        clinical.load(config, "Test Studies")
        Fixtures.clinicalDataWithCrossNodeOnSomePath.load(config, "Test Studies")

        when:
        def operation = deleteDataProcessor.process([path: "\\Test Studies\\${clinical.studyName}\\"])

        then:
        assertTrue(operation)

        def res = sql.firstRow('SELECT count(*) FROM i2b2demodata.observation_fact where sourcesystem_cd = ?', [clinical.studyId])
        assertEquals(0, (Integer) res[0])

        cleanup:
        deleteDataProcessor.process([id: Fixtures.clinicalDataWithCrossNodeOnSomePath.studyId])
        deleteCrossProcessor.process([path: '\\Vital\\', isDeleteConcepts: true])
    }

    void assertThatCrossNodeDelete(String path, isDelete = true) {
        def i2b2Count = sql.firstRow('select count(*) from i2b2metadata.i2b2 where c_fullname = ?', path)
        isDelete ?
                assertEquals('Row deleted from i2b2 table', 0, (Integer) i2b2Count[0]) :
                assertEquals('Row didn\'t delete from i2b2 table', 1, (Integer) i2b2Count[0])

    }

    void assertThatConceptDelete(String path, isDelete = true) {
        def concept = sql.firstRow('select count(*) from i2b2demodata.concept_dimension where concept_path = ?', path)
        isDelete ?
                assertEquals('Row deleted from concept_dimension table', 0, (Integer) concept[0]) :
                assertEquals('Row didn\'t delete from concept_dimension table', 1, (Integer) concept[0])

    }
}
