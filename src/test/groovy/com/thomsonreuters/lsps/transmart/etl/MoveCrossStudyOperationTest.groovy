package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.db.core.DatabaseType
import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import com.thomsonreuters.lsps.transmart.fixtures.Study
import spock.lang.Specification

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static org.hamcrest.CoreMatchers.not
import static org.junit.Assert.*

class MoveCrossStudyOperationTest extends Specification implements ConfigAwareTestCase {
    private MoveStudyProcessor _moveStudyProcessor
    private DeleteCrossProcessor _deleteCrossProcessor

    ClinicalData clinicalData = Fixtures.clinicalData.copyWithSuffix('MV')
    ClinicalData otherClinicalData = clinicalData.copyWithSuffix('2')

    String rootName = 'Test Studies Move Test'
    String studyName = clinicalData.studyName
    String studyId = clinicalData.studyId
    String originalPath = "\\$rootName\\$studyName\\"
    def parentNode = '\\Test Studies\\'

    MoveStudyProcessor getMoveStudyProcessor() {
        _moveStudyProcessor ?: (_moveStudyProcessor = new MoveStudyProcessor(config))
    }

    DeleteCrossProcessor getDeleteCrossProcessor() {
        _deleteCrossProcessor ?: (_deleteCrossProcessor = new DeleteCrossProcessor(config))
    }

    void setup() {
        ConfigAwareTestCase.super.setUp()
        runScript('I2B2_MOVE_STUDY_BY_PATH.sql')
        runScript('I2B2_MOVE_CROSS_BY_PATH.sql')
        runScript('I2B2_DELETE_ALL_DATA.sql')
        runScript('I2B2_DELETE_ALL_NODES.sql')
        runScript('I2B2_DELETE_CROSS_DATA.sql')

        Study.deleteById(config, Fixtures.clinicalDataWithCrossNode.studyId)
        Study.deleteById(config, Fixtures.clinicalDataWithCrossNodeOnAnotherConceptCD.studyId)
        Study.deleteCross(config, "\\Vital\\")
        Study.deleteCross(config, "\\Vital Move\\")
    }

    def assertNewLevelWasDeleted(String newPath) {

        def tablesToAttr = ['i2b2metadata.i2b2'             : 'c_fullname',
                            'i2b2metadata.i2b2_secure'      : 'c_fullname',
                            'i2b2demodata.concept_counts'   : 'concept_path',
                            'i2b2demodata.concept_dimension': 'concept_path']

        checkPaths(tablesToAttr, 'Old level node was found in ', newPath, 0);
        true
    }

    private boolean checkPaths(Map tablesToAttr, String errorMessage, String checkedPath, int correctCount) {
        checkedPath = checkedPath.charAt(checkedPath.length() - 1) != '\\' ? checkedPath + '\\' : checkedPath
        for (t in tablesToAttr) {
            def c = sql.firstRow('select count(*) from ' + t.key + ' where ' + t.value + ' = ?', checkedPath as String)
            assertEquals("$errorMessage$t.key ($checkedPath)", correctCount, c[0] as Integer)
        }
        true
    }

    private boolean moveStudy(oldPath, newPath) {
        def studyNode = newPath[newPath.length() - 1] == '\\' ? newPath : (newPath + '\\')
        moveStudy(oldPath, newPath, true, studyNode)
    }

    private boolean moveStudy(oldPath, newPath, String studyNode) {
        moveStudy(oldPath, newPath, true, studyNode)
    }

    private boolean moveStudy(oldPath, newPath, boolean checkResult = true, studyNode) {
        def result = moveStudyProcessor.process(old_path: oldPath, new_path: newPath, keepSecurity: false)
        if (checkResult) {
            assert result, "Moving study from '${oldPath}' to '$newPath' failed"
            if (studyNode) {
                def c = sql.firstRow('select count(*) from i2b2metadata.i2b2 where c_fullname = ? ', studyNode as String)
                assertEquals("Head node is wrong ($studyNode)", 1, c[0] as Integer)
            }
        }
        result
    }

    def 'it should move top folder for cross node'(){
        given:
        def originalPath = "\\Vital\\"
        def newPath = "\\Vital Move\\"

        when:
        Fixtures.clinicalDataWithCrossNode.load(config, parentNode)

        then:
        moveStudy(originalPath, newPath)
        assertNewLevelWasDeleted('\\Vital\\')
    }

    def 'it should move cross node in level up'(){
        given:
        def originalPath = "\\Vital\\Node 1\\Node 2\\"
        def newPath = "\\Vital\\Node 1\\"

        when:
        Fixtures.clinicalDataWithCrossNode.load(config, parentNode)

        then:
        moveStudy(originalPath, newPath)
    }

    def 'it should move cross node in top level up'(){
        given:
        def originalPath = "\\Vital\\Node 1\\Node 2\\"
        def newPath = "\\Vital\\"

        when:
        Fixtures.clinicalDataWithCrossNode.load(config, parentNode)

        then:
        moveStudy(originalPath, newPath)
    }

    def 'it should move cross node (leaf)'(){
        given:
        def originalPath = "\\Vital\\Node 1\\Node 2\\Flag"
        def newPath = "\\Vital\\A\\Flag"

        when:
        Fixtures.clinicalDataWithCrossNode.load(config, parentNode)

        then:
        moveStudy(originalPath, newPath)
    }

    def 'it should not move cross node to top tree level'(){
        given:
        def originalPath = "\\Vital\\Node 1\\Node 2\\Flag\\"
        def newPath = "\\Flag\\"

        when:
        Fixtures.clinicalDataWithCrossNode.load(config, parentNode)

        then:
        assertFalse(moveStudy(originalPath, newPath, false, '\\Flag\\'))

        def c0 = sql.firstRow('select count(*) from i2b2metadata.i2b2 where c_fullname = ? ', originalPath as String)
        assertEquals("Head node is deleted ($originalPath)", 1, c0[0] as Integer)

        def c1 = sql.firstRow('select count(*) from i2b2metadata.i2b2 where c_fullname = ? ', newPath as String)
        assertEquals("New path is created ($newPath)", 0, c1[0] as Integer)
    }

    def 'it should move cross study into exist folder'() {
        given:
        def originalPath = "\\Vital New\\Node 1\\Node 2\\Flag\\"
        def newPath = "\\Vital\\Node 1\\Node 2\\Flag 2\\"

        when:
        Fixtures.clinicalDataWithCrossNode.load(config, parentNode)
        Fixtures.clinicalDataWithCrossNodeOnAnotherConceptCD.load(config, parentNode)

        then:
        moveStudy(originalPath, newPath)
    }

    def 'it should not move cross study into exist cross study'(){
        given:
        def originalPath = "\\Vital New\\Node 1\\Node 2\\Flag\\"
        def newPath = "\\Vital\\Node 1\\Node 2\\Flag\\"

        when:
        Fixtures.clinicalDataWithCrossNode.load(config, parentNode)
        Fixtures.clinicalDataWithCrossNodeOnAnotherConceptCD.load(config, parentNode)

        then:
        assertFalse('Show error',moveStudy(originalPath, newPath, false, originalPath))
    }

    def 'it should move cross study into studies folder'(){
        given:
        def originalPath = "\\Vital\\Node 1\\Node 2\\Flag\\"
        def newPath = "\\Test Studies\\Flag\\"

        when:
        Fixtures.clinicalDataWithCrossNode.load(config, parentNode)

        then:
        moveStudy(originalPath, newPath)

        cleanup:
        Study.deleteCross(config,"\\Test Studies\\Flag\\")
    }


}
