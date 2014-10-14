package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.sql.DatabaseType

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.Fixtures.studyDir
import static org.junit.Assert.assertThat

class MoveStudyOperationTest extends GroovyTestCase implements ConfigAwareTestCase {
    private MoveStudyProcessor _moveStudyProcessor
    private ClinicalDataProcessor _clinicalDataProcessor
    private DeleteDataProcessor _processorDelete

    String studyName = 'Test Study'
    String studyId = 'GSE0'
    String oldPath = "\\Test Studies Move Test\\${studyName}\\"

    MoveStudyProcessor getMoveStudyProcessor() {
        _moveStudyProcessor ?: (_moveStudyProcessor = new MoveStudyProcessor(config))
    }

    ClinicalDataProcessor getClinicalDataProcessor() {
        _clinicalDataProcessor ?: (_clinicalDataProcessor = new ClinicalDataProcessor(config))
    }

    DeleteDataProcessor getProcessorDelete() {
        _processorDelete ?: (_processorDelete = new DeleteDataProcessor(config))
    }


    @Override
    public void setUp() {
        ConfigAwareTestCase.super.setUp()
        clinicalDataProcessor.process(
                new File(studyDir(studyName, studyId), "ClinicalDataToUpload"),
                [name: studyName, node: "Test Studies Move Test\\${studyName}".toString()])
        runScript('I2B2_MOVE_STUDY_BY_PATH.sql')
    }

    void testMoveStudyInOneRootNode() {
        def newPath = "\\Test Studies Move Test\\Test Study Update\\"

        moveStudy(oldPath, newPath)

        assertMovement(oldPath, newPath)
        removeStudy(newPath)
    }

    void testMoveStudyWithCreatingNewRoot() {
        def newPath = "\\Test Studies Move Test Update\\Test Study Update\\"

        moveStudy(oldPath, newPath)

        assertMovement(oldPath, newPath)
        assertRootNodeExisting(newPath)
        removeStudy(newPath)
    }

    void testMoveStudyWithCreatingNewLevel() {
        def newPath = "\\Test Studies Move Test\\New Level\\Test Study\\"

        moveStudy(oldPath, newPath)

        assertNewLevelIsAdded(newPath)
        assertConceptCounts(newPath)
        removeStudy(newPath)
    }

    void assertConceptCounts(String newPath) {
        assertThat(sql, hasNode(newPath).withPatientCount(9))
    }

    void testMoveStudyWithDeletingNewLevel() {
        def newPath = "\\Test Studies Move Test\\New Level\\Test Study\\"
        def newPathShort = "\\Test Studies Move Test\\Test Study\\"

        moveStudy(oldPath, newPath)
        moveStudy(newPath, newPathShort)

        assertNewLevelWasDeleted(newPath)
        removeStudy(newPathShort)
    }

    void testMoveStudyWithoutTrailingSlash() {
        def oldPathWoSlash = oldPath.substring(0, oldPath.length() - 1);
        def newPathWoSlash = "\\Test Studies Move Test\\Test Study Wo Slash"
        def newPath = "\\Test Studies Move Test\\Test Study With Slash\\"

        moveStudy(oldPathWoSlash, newPathWoSlash)
        assertMovement(oldPathWoSlash, newPathWoSlash)

        moveStudy(newPathWoSlash, newPath)
        assertMovement(newPathWoSlash, newPath)

        moveStudy(newPath, newPathWoSlash)
        assertMovement(newPath, newPathWoSlash)

        removeStudy(newPathWoSlash)
    }


    void testMoveStudyToExistNode() {
        def errStudyPath = "\\Test Studies Move Test\\Test Study 2\\"
        clinicalDataProcessor.process(
                new File(studyDir(studyName, studyId), "ClinicalDataToUpload"),
                [name: studyName, node: errStudyPath.toString()])

        def input = ['old_path': errStudyPath,
                     'new_path': oldPath];
        moveStudyProcessor.process(input)
        // Expect error of trying addition to exists node

        /*def errStudyPath1 = "\\Test Studies Move Test\\New level\\Test Study 2\\"
        input = ['old_path': oldPath,
                'new_path': errStudyPath1];
        moveStudyProcessor.process(input)
        // Expect error of trying addition to studies subnode*/

        def errStudyPath2 = "\\Test Studies Move Test\\"
        input = ['old_path': oldPath,
                 'new_path': errStudyPath2];
        moveStudyProcessor.process(input)
        // Expect error of trying addition to root node

        assertThat(db, hasNode(oldPath).withPatientCount(9))
        assertThat(db, hasNode(errStudyPath).withPatientCount(9))

        removeStudy(errStudyPath)
        removeStudy(oldPath)
    }


    void testMoveStudyWithFewLevels() {
        def path1 = "\\Test Studies Move Test\\A\\B\\Test Study"
        def input = ['old_path': oldPath,
                     'new_path': path1];
        moveStudyProcessor.process(input)
        assertMovement(oldPath, path1)

        def path2 = "\\Test Studies Move Test\\A\\B\\Another Test Study"
        clinicalDataProcessor.process(
                new File(studyDir(studyName, studyId), "ClinicalDataToUpload"),
                [name: studyName, node: path2.toString()])

        def path3 = "\\Test Studies Move Test\\A\\C\\Another Test Study"
        input = ['old_path': path2,
                 'new_path': path3];
        moveStudyProcessor.process(input)

        assertMovement(path2, path3)
        assertMovement(path2, path1)

        def path4 = "\\Test Studies Move Test\\A\\C\\Test Study"
        input = ['old_path': path1,
                 'new_path': path4];
        moveStudyProcessor.process(input)

        assertMovement(path1, path3)
        assertMovement(path1, path4)

        removeStudy(path3)
        removeStudy(path4)
    }

    def assertRootNodeExisting(String newPath) {
        def newRootNode = '\\' + newPath.split('\\\\')[1] + "\\"
        def oldRootNode = '\\' + oldPath.split('\\\\')[1] + "\\"

        def tablesToAttr = ['i2b2metadata.table_access': 'c_fullname', 'i2b2metadata.i2b2': 'c_fullname',
                            'i2b2metadata.i2b2_secure' : 'c_fullname', 'i2b2demodata.concept_counts': 'parent_concept_path']


        checkPaths(tablesToAttr, "New root node (${newRootNode}) was not added to ", newRootNode, 1);
        checkPaths(tablesToAttr, "Old root node (${oldRootNode}) was not updated in ", oldRootNode, 0);
    }

    def assertNewLevelIsAdded(String newPath) {
        def secondLevelNode = '\\' + newPath.split('\\\\')[1] + "\\" + newPath.split('\\\\')[2] + "\\"

        def tablesToAttr = ['i2b2metadata.i2b2'             : 'c_fullname', 'i2b2metadata.i2b2_secure': 'c_fullname',
                            'i2b2demodata.concept_dimension': 'concept_path']

        checkPaths(tablesToAttr, 'Second level node was not found in ', secondLevelNode, 1);

    }

    def assertNewLevelWasDeleted(String newPath) {

        def tablesToAttr = ['i2b2metadata.i2b2'             : 'c_fullname', 'i2b2metadata.i2b2_secure': 'c_fullname',
                            'i2b2demodata.concept_counts'   : 'concept_path',
                            'i2b2demodata.concept_dimension': 'concept_path']

        checkPaths(tablesToAttr, 'Old level node was found in ', newPath, 0);

    }

    private void assertMovement(String oldPath, String newPath) {
        def tablesToAttr = ['i2b2metadata.i2b2'             : 'c_fullname',
                            'i2b2demodata.concept_dimension': 'concept_path',
                            'i2b2demodata.concept_counts': 'concept_path']

        checkPaths(tablesToAttr, 'New paths were not added to ', newPath, 1);
        checkChildNodes(tablesToAttr, 'Child nodes was not added to ', newPath)
        if (oldPath != null) {
            checkPaths(tablesToAttr, "Old path (${oldPath}) were not updated in ", oldPath, 0);
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

    private void moveStudy(oldPath, newPath) {
        removeStudy(newPath)
        moveStudyProcessor.process(old_path: oldPath, new_path: newPath)
    }

    private void removeStudy(pathToRemove) {
        def input = ['id': null, 'path': pathToRemove];
        processorDelete.process(input);
    }
}
