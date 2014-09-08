package com.thomsonreuters.lsps.transmart.etl


import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.Fixtures.studyDir
import static org.junit.Assert.assertThat

class MoveStudyOperationTest extends ConfigAwareTestCase {
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
        super.setUp()
        clinicalDataProcessor.process(
                new File(studyDir(studyName, studyId), "ClinicalDataToUpload"),
                [name: studyName, node: "Test Studies Move Test\\${studyName}".toString()])
        runScript('I2B2_MOVE_STUDY_BY_PATH.sql')
    }

    void testMoveStudyInOneRootNode() {
        def newPath = "\\Test Studies Move Test\\Test Study Update\\"
        def input = ['old_path': oldPath,
                'new_path': newPath];
        moveStudyProcessor.process(input)

        assertPathsExisting(newPath, oldPath)
        removeStudy(newPath)
    }

    void testMoveStudyWithCreatingNewRoot() {
        def newPath = "\\Test Studies Move Test Update\\Test Study Update\\"
        def input = ['old_path': oldPath,
                'new_path': newPath];
        moveStudyProcessor.process(input)

        assertPathsExisting(newPath, oldPath)
        assertRootNodeExisting(newPath)
        removeStudy(newPath)
    }

    void testMoveStudyWithCreatingNewLevel() {
        def newPath = "\\Test Studies Move Test\\Test Study\\New Level\\"
        def input = ['old_path': oldPath,
                'new_path': newPath];
        moveStudyProcessor.process(input)

        assertNewLevelIsAdded(newPath)
        assertConceptCounts(newPath)
        removeStudy(newPath)
    }

    void assertConceptCounts(String newPath) {
        assertThat(sql, hasNode(newPath).withPatientCount(9))
    }

    void testMoveStudyWithDeletingNewLevel() {
        def newPath = "\\Test Studies Move Test\\Test Study\\New Level\\"
        def input = ['old_path': oldPath,
                'new_path': newPath];
        moveStudyProcessor.process(input)

        def newPathShort = "\\Test Studies Move Test\\Test Study\\"
        input = ['old_path': newPath,
                'new_path': newPathShort];
        moveStudyProcessor.process(input)

        assertNewLevelWasDeleted(newPath)
        removeStudy(newPathShort)
    }

    void testMoveStudyWithoutTrailingSlash() {
        def oldPathWoSlash = oldPath.substring(0, oldPath.length()-1);
        def newPathWoSlash = "\\Test Studies Move Test\\Test Study Wo Slash"
        def input = ['old_path': oldPathWoSlash,
                'new_path': newPathWoSlash];
        moveStudyProcessor.process(input)
        assertPathsExisting(newPathWoSlash, oldPathWoSlash)

        def newPath = "\\Test Studies Move Test\\Test Study With Slash\\"
        input = ['old_path': newPathWoSlash,
                'new_path': newPath];
        moveStudyProcessor.process(input)
        assertPathsExisting(newPath, newPathWoSlash)

        input = ['old_path': newPath,
                'new_path': newPathWoSlash];
        moveStudyProcessor.process(input)
        assertPathsExisting(newPathWoSlash, newPath)

        removeStudy(newPathWoSlash)
    }

   /* void testMoveStudyToExistNode() {
        def errStudyPath = "\\Test Studies Move Test\\Test Study Update 2\\"
        clinicalDataProcessor.process(
                new File(studyDir(studyName, studyId), "ClinicalDataToUpload"),
                [name: studyName, node: errStudyPath.toString()])

        def input = ['old_path': errStudyPath,
                'new_path': oldPath];
        moveStudyProcessor.process(input)
        // Expect error of trying addition to exists node

        errStudyPath = "\\Test Studies Move Test\\Test Study Update 2\\New level\\"
        input = ['old_path': oldPath,
                'new_path': errStudyPath];
        moveStudyProcessor.process(input)
        // Expect error of trying addition to exists node

        errStudyPath = "\\Test Studies Move Test\\"
        input = ['old_path': oldPath,
                'new_path': errStudyPath];
        moveStudyProcessor.process(input)
        // Expect error of trying addition to root node

        removeStudy(oldPath)
    }*/


    def assertRootNodeExisting(String newPath) {
        def newRootNode = '\\' + newPath.split('\\\\')[1] + "\\"
        def oldRootNode = '\\' + oldPath.split('\\\\')[1] + "\\"

        def tablesToAttr = ['i2b2metadata.table_access': 'c_fullname', 'i2b2metadata.i2b2': 'c_fullname',
                'i2b2metadata.i2b2_secure': 'c_fullname', 'i2b2demodata.concept_counts': 'parent_concept_path',
                'i2b2demodata.concept_dimension': 'concept_path']


        checkPaths(tablesToAttr, 'New root node was not added to ', newRootNode, 1);
        checkPaths(tablesToAttr, 'Old root node was not updated in ', oldRootNode, 0);
    }

    def assertNewLevelIsAdded(String newPath) {
        def secondLevelNode = '\\' + newPath.split('\\\\')[1] + "\\" + newPath.split('\\\\')[2] + "\\"

        def tablesToAttr = ['i2b2metadata.i2b2': 'c_fullname', 'i2b2metadata.i2b2_secure': 'c_fullname',
                'i2b2demodata.concept_dimension': 'concept_path']

        checkPaths(tablesToAttr, 'Second level node was not found in ', secondLevelNode, 1);

    }

    def assertNewLevelWasDeleted(String newPath) {

        def tablesToAttr = ['i2b2metadata.i2b2': 'c_fullname', 'i2b2metadata.i2b2_secure': 'c_fullname',
                'i2b2demodata.concept_counts': 'concept_path',
                'i2b2demodata.concept_dimension': 'concept_path']

        checkPaths(tablesToAttr, 'Old level node was found in ', newPath, 0);

    }

    private void assertPathsExisting(String newPath, String oldPath) {
        def tablesToAttr = ['i2b2metadata.i2b2': 'c_fullname', 'i2b2metadata.i2b2_secure': 'c_fullname',
                'i2b2demodata.concept_dimension': 'concept_path', 'i2b2demodata.concept_counts': 'concept_path']

        checkPaths(tablesToAttr, 'New paths were not added to ', newPath, 1);
        checkPaths(tablesToAttr, 'Old paths were not updated in ', oldPath, 0);
    }

    private void checkPaths(Map tablesToAttr, String errorMessage, String checkedPath, int correctCount) {
        checkedPath = checkedPath.charAt(checkedPath.length() - 1) != '\\' ? checkedPath + '\\' : checkedPath
        for (t in tablesToAttr) {
            def c = sql.firstRow('select count(*) from ' + t.key + ' where ' + t.value + ' = ?', checkedPath)
            assertEquals(errorMessage + t.key, c[0] as Integer, correctCount)
        }
    }

    /*
    void testMoveStudyWithNewLevelAdding() {

    }

    void testMoveStudyWithNewLevelRemoving() {

    }*/

    private void removeStudy(pathToRemove) {
        def input = ['id': null, 'path': pathToRemove];
        processorDelete.process(input);
    }

}
