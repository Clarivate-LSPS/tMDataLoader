package com.thomsonreuters.lsps.transmart.etl
import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import com.thomsonreuters.lsps.transmart.fixtures.Study
import com.thomsonreuters.lsps.transmart.sql.DatabaseType

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static org.hamcrest.CoreMatchers.not
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
        assertConceptCounts(newPath,['':9])
    }

    private void assertConceptCounts(String topPath, Map subPathAndCount){
        subPathAndCount.each {
            assertThat(sql, hasNode(topPath + it.key).withPatientCount(it.value))
        }
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

        moveStudy(oldPathWoSlash, newPathWoSlash, "$newPathWoSlash\\")
        assertMovement(oldPathWoSlash, newPathWoSlash)

        moveStudy(newPathWoSlash, newPath)
        assertMovement(newPathWoSlash, newPath)

        moveStudy(newPath, newPathWoSlash, "$newPathWoSlash\\")
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
        moveStudy(originalPath, path1, "$path1\\")
        assertMovement(originalPath, path1)

        def path2 = "\\$rootName\\A\\B\\${otherClinicalData.studyName}"
        otherClinicalData.load(config, "$rootName\\A\\B\\")

        def path3 = "\\$rootName\\A\\C\\Another Test Study"
        moveStudy(path2, path3, "$path3\\")

        assertMovement(path2, path3, "\\$rootName\\A\\B\\")
        assertMovement(path2, path1)

        def path4 = "\\$rootName\\A\\C\\Test Study"
        moveStudy(path1, path4, "$path4\\")

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
                            'i2b2metadata.i2b2_secure'      : 'c_fullname',
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

    private static List<String> collectPaths(String path) {
        def pathHierarchy = []
        def parts = path.split("\\\\")
        def basePath = '\\'
        for (def part : parts) {
            if (!part) {
                continue
            }
            pathHierarchy << (basePath = "${basePath}${part}\\" as String)
        }
        pathHierarchy
    }

    private void assertMovement(String oldPath, String newPath, String keepPath = null) {
        def i2b2TablesToAttr = ['i2b2metadata.i2b2'       : 'c_fullname',
                                'i2b2metadata.i2b2_secure': 'c_fullname']
        def conceptTablesToAttr = ['i2b2demodata.concept_dimension': 'concept_path',
                                   'i2b2demodata.concept_counts'   : 'concept_path']

        List<String> newPaths = collectPaths(newPath)
        for (def path : newPaths) {
            checkPaths(i2b2TablesToAttr, 'New paths were not added to ', path, 1)
        }
        checkPaths(conceptTablesToAttr, 'New paths were not added to ', newPath, 1)
        checkChildNodes(i2b2TablesToAttr + conceptTablesToAttr, 'Child nodes was not added to ', newPath)
        if (oldPath != null) {
            def oldPaths = collectPaths(oldPath) - newPaths
            if (keepPath) {
                oldPaths -= collectPaths(keepPath)
            }
            for (def path : oldPaths) {
                checkPaths(i2b2TablesToAttr, "Old path (${oldPath}) were not updated in ", path, 0)
            }
            checkPaths(i2b2TablesToAttr, "Old path (${oldPath}) were not updated in ", oldPath, 0);
        }
    }



    private void checkPaths(Map tablesToAttr, String errorMessage, String checkedPath, int correctCount) {
        checkedPath = checkedPath.charAt(checkedPath.length() - 1) != '\\' ? checkedPath + '\\' : checkedPath
        for (t in tablesToAttr) {
            def c = sql.firstRow('select count(*) from ' + t.key + ' where ' + t.value + ' = ?', checkedPath as String)
            assertEquals("$errorMessage$t.key ($checkedPath)", correctCount, c[0] as Integer)
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
        moveStudy(oldPath, newPath, true, newPath)
    }

    private boolean moveStudy(oldPath, newPath, String headNode) {
        moveStudy(oldPath, newPath, true, headNode)
    }

    private boolean moveStudy(oldPath, newPath, boolean checkResult = true, headNode) {
        def result = moveStudyProcessor.process(old_path: oldPath, new_path: newPath)
        if (checkResult) {
            assert result, "Moving study from '${oldPath}' to '$newPath' failed"
            if (headNode){
                def c = sql.firstRow('select count(*) from i2b2metadata.i2b2 where c_fullname = ? and c_visualattributes = \'FAS\'', headNode as String)
                assertEquals("Head node is wrong ($headNode)", 1, c[0] as Integer)
            }
        }
        result
    }

    void testMoveSubfolder(){
        def oldPath = "\\$rootName\\$studyName\\Subjects\\Demographics\\Language\\"
        def newPath = "\\$rootName\\$studyName\\Subjects\\Demographics new\\Language\\"

        moveStudy(oldPath, newPath, "\\$rootName\\$studyName\\")

        assertMovement(oldPath, newPath, "\\$rootName\\$studyName\\Subjects\\Demographics\\")
        def m = ['Demographics new\\':3,
                 'Demographics new\\Language\\':3,
                 'Demographics new\\Language\\English\\':2,
                 'Demographics new\\Language\\Spain\\':1,
                 'Demographics\\Sex (SEX)\\Female\\':5,
                 'Demographics\\Sex (SEX)\\':7,
                 'Demographics\\Assessment Date\\':9
                ]
        assertConceptCounts("\\$rootName\\$studyName\\Subjects\\", m)
    }

    void testMoveSubfolder4(){
        def oldPath = "\\$rootName\\$studyName\\Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\ELREA746del\\"
        def newPath = "\\$rootName\\$studyName\\test\\ELREA746del\\"

        moveStudy(oldPath, newPath, "\\$rootName\\$studyName\\")

        def m = ['Biomarker Data\\':6
        ]
        assertConceptCounts("\\$rootName\\$studyName\\", m)
    }

    void testMoveSubfolder2(){
        def oldPath = "\\$rootName\\$studyName\\Subjects\\Demographics\\Language\\"
        def newPath = "\\$rootName\\$studyName\\Subjects new\\Demographics\\Language\\"

        moveStudy(oldPath, newPath, "\\$rootName\\$studyName\\")

        assertMovement(oldPath, newPath, "\\$rootName\\$studyName\\Subjects\\Demographics\\")
        def m = ['Subjects new\\Demographics\\':3,
                 'Subjects new\\Demographics\\Language\\':3,
                 'Subjects new\\Demographics\\Language\\English\\':2,
                 'Subjects new\\Demographics\\Language\\Spain\\':1,
                 'Subjects\\Demographics\\Sex (SEX)\\Female\\':5,
                 'Subjects\\Demographics\\Sex (SEX)\\Male\\':2
        ]
        assertConceptCounts("\\$rootName\\$studyName\\", m)
    }

    void testMoveSubfolder3(){
        def oldPath = "\\$rootName\\$studyName\\Subjects\\Demographics\\Language\\"
        def newPath = "\\$rootName\\$studyName\\Subjects new\\Demographics new\\Language\\"

        moveStudy(oldPath, newPath,"\\$rootName\\$studyName\\")

        assertMovement(oldPath, newPath, "\\$rootName\\$studyName\\Subjects\\Demographics\\")
        def m = ['Subjects new\\Demographics new\\':3,
                 'Subjects new\\Demographics new\\Language\\':3,
                 'Subjects new\\Demographics new\\Language\\English\\':2,
                 'Subjects new\\Demographics new\\Language\\Spain\\':1,
                 'Subjects\\Demographics\\Sex (SEX)\\Female\\':5,
                 'Subjects\\Demographics\\Sex (SEX)\\Male\\':2
        ]
        assertConceptCounts("\\$rootName\\$studyName\\", m)
    }

    void testItDoesntMoveSubfolderOutsideOfStudy() {
        def oldPath = "\\$rootName\\$studyName\\Subjects\\Demographics\\Language\\"
        def newPath = "\\$rootName\\Other Study\\Subjects\\Demographics\\Language\\"

        assertFalse("Shouldn't move subfolder outside of study", moveStudy(oldPath, newPath, false, ''))
    }

    void testItCheckUpdateConceptCounts(){
        def newPath = "\\$rootName\\Test Study Update\\"

        moveStudy(originalPath, newPath)

        assertNewLevelWasDeleted(originalPath)
        assertThat(db, not(hasRecord('i2b2demodata.concept_counts',[parent_concept_path:"${originalPath}Subjects\\"], [concept_path: "${originalPath}Subjects\\Demographics\\"])))
        assertThat(db, hasRecord('i2b2demodata.concept_counts',[parent_concept_path:"${newPath}Subjects\\"], [concept_path: "${newPath}Subjects\\Demographics\\"]))
    }

    void testItCheckUpdateConceptCountsWIthDoubleChangeTop(){
        Study.deleteById(config, clinicalData.studyId)
        def oldPath = "\\${rootName}\\A\\"
        def newPath = "\\${rootName} Update\\C\\D\\"

        clinicalData.load(config, oldPath)

        moveStudy("${oldPath}${studyName}\\", newPath)

        assertNewLevelWasDeleted(originalPath)
        assertThat(db, not(hasRecord('i2b2demodata.concept_counts',[parent_concept_path:"${oldPath}Subjects\\"], [concept_path: "${oldPath}Subjects\\Demographics\\"])))
        assertThat(db, hasRecord('i2b2demodata.concept_counts',[parent_concept_path:"${newPath}Subjects\\"], [concept_path: "${newPath}Subjects\\Demographics\\"]))

        def checkPath = "\\${rootName} Update\\C\\"
        assertThat(db, not(hasRecord('i2b2demodata.concept_counts',[concept_path:"${checkPath}"], [:])))
    }

    void testItCheckUpdateConceptCountsWithAddHierarchyLevel(){
        Study.deleteById(config, clinicalData.studyId)
        def oldPath = "\\${rootName}\\A\\"
        def newPath = "\\${rootName} Update\\C\\D\\E\\"

        clinicalData.load(config, oldPath)

        moveStudy("${oldPath}${studyName}\\", newPath)

        assertNewLevelWasDeleted(originalPath)
        assertThat(db, not(hasRecord('i2b2demodata.concept_counts',[parent_concept_path:"${oldPath}Subjects\\"], [concept_path: "${oldPath}Subjects\\Demographics\\"])))
        assertThat(db, hasRecord('i2b2demodata.concept_counts',[parent_concept_path:"${newPath}Subjects\\"], [concept_path: "${newPath}Subjects\\Demographics\\"]))
        assertThat(db, hasRecord('i2b2demodata.concept_counts',[parent_concept_path:"${newPath}Subjects\\"], [concept_path: "${newPath}Subjects\\Demographics\\"]))

        def checkPath = "\\${rootName} Update\\C\\D\\"
        assertThat(db, not(hasRecord('i2b2demodata.concept_counts',[concept_path:"${checkPath}"], [:])))
    }

    void testItCheckUpdateConceptCountsWithRemoveHierarchyLevel(){
        Study.deleteById(config, clinicalData.studyId)
        def oldPath = "\\${rootName}\\A\\B\\C\\"
        def newPath = "\\${rootName}\\A\\B\\${studyName}\\"

        clinicalData.load(config, oldPath)

        moveStudy("${oldPath}${studyName}\\", newPath)

        assertNewLevelWasDeleted(originalPath)
        assertThat(db, not(hasRecord('i2b2demodata.concept_counts',[parent_concept_path:"${oldPath}Subjects\\"], [concept_path: "${oldPath}Subjects\\Demographics\\"])))
        assertThat(db, hasRecord('i2b2demodata.concept_counts',[parent_concept_path:"${newPath}Subjects\\"], [concept_path: "${newPath}Subjects\\Demographics\\"]))
        assertThat(db, hasRecord('i2b2demodata.concept_counts',[parent_concept_path:"${newPath}Subjects\\"], [concept_path: "${newPath}Subjects\\Demographics\\"]))

        def checkPath = "\\${rootName} Update\\C\\D\\"
        assertThat(db, not(hasRecord('i2b2demodata.concept_counts',[concept_path:"${checkPath}"], [:])))
    }
}
