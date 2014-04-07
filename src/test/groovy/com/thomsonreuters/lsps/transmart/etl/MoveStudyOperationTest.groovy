package com.thomsonreuters.lsps.transmart.etl

import org.junit.Before
import org.junit.Test

import static com.thomsonreuters.lsps.transmart.Fixtures.studyDir

class MoveStudyOperationTest extends ConfigAwareTestCase {
    private MoveStudyProcessor _moveStudyProcessor
    private ClinicalDataProcessor _clinicalDataProcessor
    private DeleteDataProcessor _processorDelete

    String studyName = 'Test Study'
    String studyId = 'GSE0'
    String oldPath = "\\Test Studies\\${studyName}\\"

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
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
    }

    void testMoveStudyInOneRootNode() {
        def newPath = "\\Test Studies\\Test Study Update\\"
        def input = ['old_path': oldPath,
                'new_path': newPath];
        moveStudyProcessor.process(input)

        assertPathsExisting(newPath)
        removeStudy(newPath)
    }

    void testMoveStudyWithCreatingNewRoot() {
        def newPath = "\\Test Studies Update\\Test Study Update\\"
        def input = ['old_path': oldPath,
                'new_path': newPath];
        moveStudyProcessor.process(input)

        assertPathsExisting(newPath)
        assertRootNodeExisting(newPath)
        removeStudy(newPath)
    }

    void testMoveStudyWithCreatingNewLevel() {
        def newPath = "\\Test Studies\\Test Study\\New Level\\"
        def input = ['old_path': oldPath,
                'new_path': newPath];
        moveStudyProcessor.process(input)

        assertNewLevelIsAdded(newPath)
        removeStudy(newPath)
    }

    void testMoveStudyWithDeletingNewLevel() {
        def newPath = "\\Test Studies\\Test Study\\New Level\\"
        def input = ['old_path': oldPath,
                'new_path': newPath];
        moveStudyProcessor.process(input)

        def newPathShort = "\\Test Studies\\Test Study\\"
        input = ['old_path': newPath,
                'new_path': newPathShort];
        moveStudyProcessor.process(input)

        assertNewLevelWasDeleted(newPath)
        removeStudy(newPathShort)
    }


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
                'i2b2demodata.concept_counts': 'concept_path',
                'i2b2demodata.concept_dimension': 'concept_path']

        checkPaths(tablesToAttr, 'Second level node was not found in ', secondLevelNode, 1);

    }

    def assertNewLevelWasDeleted(String newPath) {

        def tablesToAttr = ['i2b2metadata.i2b2': 'c_fullname', 'i2b2metadata.i2b2_secure': 'c_fullname',
                'i2b2demodata.concept_counts': 'concept_path',
                'i2b2demodata.concept_dimension': 'concept_path']

        checkPaths(tablesToAttr, 'Old level node was found in ', newPath, 0);

    }

    private void assertPathsExisting(String newPath) {
        def tablesToAttr = ['i2b2metadata.i2b2': 'c_fullname', 'i2b2metadata.i2b2_secure': 'c_fullname',
                'i2b2demodata.concept_dimension': 'concept_path', 'i2b2demodata.concept_counts': 'concept_path']

        checkPaths(tablesToAttr, 'New paths were not added to ', newPath, 1);
        checkPaths(tablesToAttr, 'Old paths were not updated in ', oldPath, 0);
    }

    private void checkPaths(Map tablesToAttr, String errorMessage, String checkedPath, int correctCount) {
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
