package com.thomsonreuters.lsps.transmart.etl
import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import com.thomsonreuters.lsps.transmart.fixtures.Study
import com.thomsonreuters.lsps.transmart.sql.DatabaseType

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertThat

class MoveStudyOperationTest extends GroovyTestCase implements ConfigAwareTestCase {
    private MoveStudyProcessor _moveStudyProcessor

    ClinicalData clinicalData = Fixtures.clinicalData.copyWithSuffix('MV')
    ClinicalData otherClinicalData = clinicalData.copyWithSuffix('2')

    String rootName = 'Test Studies Move Test'
    String studyName = clinicalData.studyName
    String studyId = clinicalData.studyId
    String originalPath = "\\$rootName\\$studyName\\"

    MoveStudyProcessor getMoveStudyProcessor() {
        _moveStudyProcessor ?: (_moveStudyProcessor = new MoveStudyProcessor(config))
    }


    @Override
    public void setUp() {
        ConfigAwareTestCase.super.setUp()
        runScript('I2B2_MOVE_STUDY_BY_PATH.sql')
        Study.deleteById(config, clinicalData.studyId)
        Study.deleteById(config, otherClinicalData.studyId)
        clinicalData.load(config, rootName)
    }

    void testMoveStudyInOneRootNode() {
        def newPath = "\\$rootName\\Test Study Update\\"

        moveStudy(originalPath, newPath)

        assertMovement(originalPath, newPath)
    }

    void testMoveStudyWithCreatingNewRoot() {
        def newPath = "\\Test Studies Move Test Update\\Test Study Update\\"

        moveStudy(originalPath, newPath)

        assertMovement(originalPath, newPath)
        assertRootNodeExisting(newPath)
    }

    void testMoveStudyWithCreatingNewLevel() {
        def newPath = "\\$rootName\\New Level\\Test Study\\"

        moveStudy(originalPath, newPath)

        assertNewLevelIsAdded(newPath)
        assertConceptCounts(newPath)
    }

    void assertConceptCounts(String newPath) {
        assertThat(sql, hasNode(newPath).withPatientCount(9))
    }

    void testMoveStudyWithDeletingNewLevel() {
        def newPath = "\\$rootName\\New Level\\Test Study\\"
        def newPathShort = "\\$rootName\\Test Study\\"

        moveStudy(originalPath, newPath)
        moveStudy(newPath, newPathShort)

        assertNewLevelWasDeleted(newPath)
    }

    void testMoveStudyWithoutTrailingSlash() {
        def oldPathWoSlash = originalPath.substring(0, originalPath.length() - 1);
        def newPathWoSlash = "\\$rootName\\Test Study Wo Slash"
        def newPath = "\\$rootName\\Test Study With Slash\\"

        moveStudy(oldPathWoSlash, newPathWoSlash)
        assertMovement(oldPathWoSlash, newPathWoSlash)

        moveStudy(newPathWoSlash, newPath)
        assertMovement(newPathWoSlash, newPath)

        moveStudy(newPath, newPathWoSlash)
        assertMovement(newPath, newPathWoSlash)
    }


    void testMoveStudyToExistingNode() {
        def otherStudyPath = "\\$rootName\\${otherClinicalData.studyName}\\"
        otherClinicalData.load(config, rootName)

        // Expect error of trying addition to exists node
        assertFalse("Shouldn't move to existing path", moveStudyProcessor.process(old_path: otherStudyPath, new_path: originalPath))

        /*def errStudyPath1 = "\\$rootName\\New level\\Test Study 2\\"
        input = ['old_path': oldPath,
                'new_path': errStudyPath1];
        moveStudyProcessor.process(input)
        // Expect error of trying addition to studies subnode*/

        def errStudyPath2 = "\\$rootName\\"
        // Expect error of trying addition to root node
        assertFalse("Shouldn't move to invalid path", moveStudyProcessor.process(old_path: originalPath, new_path: errStudyPath2))

        assertThat(db, hasNode(originalPath).withPatientCount(9))
        assertThat(db, hasNode(otherStudyPath).withPatientCount(9))
    }


    void testMoveStudyWithFewLevels() {
        def path1 = "\\$rootName\\A\\B\\Test Study"
        moveStudy(originalPath, path1)
        assertMovement(originalPath, path1)

        def path2 = "\\$rootName\\A\\B\\${otherClinicalData.studyName}"
        otherClinicalData.load(config, "$rootName\\A\\B\\")

        def path3 = "\\$rootName\\A\\C\\Another Test Study"
        moveStudy(path2, path3)

        assertMovement(path2, path3)
        assertMovement(path2, path1)

        def path4 = "\\$rootName\\A\\C\\Test Study"
        moveStudy(path1, path4)

        assertMovement(path1, path3)
        assertMovement(path1, path4)
    }

    def assertRootNodeExisting(String newPath) {
        def newRootNode = '\\' + newPath.split('\\\\')[1] + "\\"
        def oldRootNode = '\\' + originalPath.split('\\\\')[1] + "\\"

        def tablesToAttr = ['i2b2metadata.table_access': 'c_fullname', 'i2b2metadata.i2b2': 'c_fullname',
                            'i2b2metadata.i2b2_secure' : 'c_fullname', 'i2b2demodata.concept_counts': 'parent_concept_path']


        checkPaths(tablesToAttr, "New root node (${newRootNode}) was not added to ", newRootNode, 1);
        checkPaths(tablesToAttr, "Old root node (${oldRootNode}) was not updated in ", oldRootNode, 0);
    }

    def assertNewLevelIsAdded(String newPath) {
        def secondLevelNode = '\\' + newPath.split('\\\\')[1] + "\\" + newPath.split('\\\\')[2] + "\\"

        def tablesToAttr = ['i2b2metadata.i2b2'             : 'c_fullname',
//                            'i2b2metadata.i2b2_secure'      : 'c_fullname',
                            'i2b2demodata.concept_dimension': 'concept_path']

        checkPaths(tablesToAttr, 'Second level node was not found in ', secondLevelNode, 1);
    }

    def assertNewLevelWasDeleted(String newPath) {

        def tablesToAttr = ['i2b2metadata.i2b2'             : 'c_fullname',
                            'i2b2metadata.i2b2_secure': 'c_fullname',
                            'i2b2demodata.concept_counts'   : 'concept_path',
                            'i2b2demodata.concept_dimension': 'concept_path']

        checkPaths(tablesToAttr, 'Old level node was found in ', newPath, 0);

    }

    private void assertMovement(String oldPath, String newPath) {
        def tablesToAttr = ['i2b2metadata.i2b2'             : 'c_fullname',
                            'i2b2demodata.concept_dimension': 'concept_path',
                            'i2b2demodata.concept_counts'   : 'concept_path']

        checkPaths(tablesToAttr, 'New paths were not added to ', newPath, 1);
        checkChildNodes(tablesToAttr, 'Child nodes was not added to ', newPath)
        if (oldPath != null) {
            checkPaths(tablesToAttr, "Old path (${oldPath}) were not updated in ", oldPath, 0);
        }
    }

    private void assertConceptcounts(String topPath, Map subPathAndCount){
        subPathAndCount.each{
            def query = 'select patient_count from i2b2demodata.concept_counts where concept_path = ?'
            def checkPath = topPath + it.key
            def c = sql.firstRow(query, checkPath)
            assertEquals(c[0] as Integer, it.value)
        }
    }

    private void checkPaths(Map tablesToAttr, String errorMessage, String checkedPath, int correctCount) {
        checkedPath = checkedPath.charAt(checkedPath.length() - 1) != '\\' ? checkedPath + '\\' : checkedPath
        for (t in tablesToAttr) {
            def c = sql.firstRow('select count(*) from ' + t.key + ' where ' + t.value + ' = ?', checkedPath)
            assertEquals(errorMessage + t.key, correctCount, c[0] as Integer)
        }
    }

    private void checkChildNodes(Map tablesToAttr, String errorMessage, String checkedPath) {
        for (t in tablesToAttr) {
            def query = 'select count(*) from i2b2metadata.i2b2 where c_fullname LIKE ? || \'%\''
            if (database?.databaseType == DatabaseType.Postgres) {
                query += ' ESCAPE \'`\''
            }
            def c = sql.firstRow(query, checkedPath)
            assertTrue(errorMessage + t.key, (c[0] as Integer) > 0)
        }
    }

    private boolean moveStudy(oldPath, newPath) {
        def result = moveStudyProcessor.process(old_path: oldPath, new_path: newPath)
        assert result, "Moving study from '${oldPath}' to '$newPath' failed"
        result
    }

    void testMoveSubfolder(){
        def oldPath = "\\$rootName\\$studyName\\Subjects\\Demographics\\Language\\"
        def newPath = "\\$rootName\\$studyName\\Subjects\\Demographics new\\Language\\"

        moveStudy(oldPath, newPath)

        assertMovement(oldPath, newPath)
        def m = ['Demographics new\\':3,
                 'Demographics new\\Language\\':3,
                 'Demographics new\\Language\\English\\':2,
                 'Demographics new\\Language\\Spain\\':1,
                ]
        assertConceptcounts("\\$rootName\\$studyName\\Subjects\\", m)
    }

    void testMoveSubfolder2(){
        def oldPath = "\\$rootName\\$studyName\\Subjects\\Demographics\\Language\\"
        def newPath = "\\$rootName\\$studyName\\Subjects new\\Demographics\\Language\\"

        moveStudy(oldPath, newPath)

        assertMovement(oldPath, newPath)
        def m = ['Subjects new\\Demographics\\':3,
                 'Subjects new\\Demographics\\Language\\':3,
                 'Subjects new\\Demographics\\Language\\English\\':2,
                 'Subjects new\\Demographics\\Language\\Spain\\':1,
                 'Subjects\\Demographics\\Sex (SEX)\\Female\\':5,
                 'Subjects\\Demographics\\Sex (SEX)\\Male\\':2
        ]
        assertConceptcounts("\\$rootName\\$studyName\\", m)
    }

    void testMoveSubfolder3(){
        def oldPath = "\\$rootName\\$studyName\\Subjects\\Demographics\\Language\\"
        def newPath = "\\$rootName\\$studyName\\Subjects new\\Demographics new\\Language\\"

        moveStudy(oldPath, newPath)

        assertMovement(oldPath, newPath)
        def m = ['Subjects new\\Demographics new\\':3,
                 'Subjects new\\Demographics new\\Language\\':3,
                 'Subjects new\\Demographics new\\Language\\English\\':2,
                 'Subjects new\\Demographics new\\Language\\Spain\\':1,
                 'Subjects\\Demographics\\Sex (SEX)\\Female\\':5,
                 'Subjects\\Demographics\\Sex (SEX)\\Male\\':2
        ]
        assertConceptcounts("\\$rootName\\$studyName\\", m)
    }
}
