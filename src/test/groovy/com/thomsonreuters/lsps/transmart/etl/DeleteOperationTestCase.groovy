package com.thomsonreuters.lsps.transmart.etl

import org.hamcrest.core.IsNull

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.hamcrest.CoreMatchers.equalTo
import static org.hamcrest.CoreMatchers.notNullValue
import static org.junit.Assert.assertThat
import static org.junit.Assert.assertTrue
import static org.junit.Assert.assertTrue

class DeleteOperationTestCase extends ConfigAwareTestCase {
    private ExpressionDataProcessor _processorLoad
    private DeleteDataProcessor _processorDelete

    ExpressionDataProcessor getProcessorLoad() {
        _processorLoad ?: (_processorLoad = new ExpressionDataProcessor(config))
    }

    DeleteDataProcessor getProcessorDelete() {
        _processorDelete ?: (_processorDelete = new DeleteDataProcessor(config))
    }

    String studyName = 'TestDeleteStudy'
    String studyId = 'GSE0'

    void assertThatDataDeleted(inpData, boolean isDelete) {
        String fullName = inpData['path'].toString();
        String trialId = inpData['id'].toString();
        Integer i2b2CountExpect = (isDelete ? 0 : 1);

        def i2b2Count = sql.firstRow('select count(*) from i2b2 where c_fullname = ?', fullName)
        assertEquals(i2b2Count[0] as Integer, i2b2CountExpect)

        def i2b2SecureCount = sql.firstRow('select count(*) from i2b2_secure where c_fullname = ?', fullName)
        assertEquals(i2b2SecureCount[0] as Integer, i2b2CountExpect)

        def sample = sql.firstRow('select * from de_subject_sample_mapping where trial_name = ? and sample_cd = ?',
                trialId, 'STD')
        if (isDelete)
            assertThat(sample, IsNull.nullValue())
        else
            assertThat(sample, IsNull.notNullValue())

    }

    void assertThatTopNodeDelete(String pathTopNode){
        def i2b2Count = sql.firstRow('select count(*) from i2b2 where c_fullname = ?', pathTopNode)
        assertEquals(i2b2Count[0] as Integer, 0)

        def i2b2SecureCount = sql.firstRow('select count(*) from i2b2_secure where c_fullname = ?', pathTopNode)
        assertEquals(i2b2SecureCount[0] as Integer, 0)

        def tableAccessCount = sql.firstRow('select count(*) from table_access where c_fullname = ?', pathTopNode)
        assertEquals(tableAccessCount[0] as Integer, 0)
    }
    /**
     * Remove data by Id and don't understand full path to study.
     * If system exist study with trialId equals GSE0 test is down
     */
    void testItDeleteDataById() {
        processorLoad.process(
                new File("fixtures/Public Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        def inpData = ['id'  : studyId,
                       'path': null];
        processorDelete.process(inpData);
        def testData = [
                'id'  : studyId,
                'path': "\\Test Studies\\${studyName}\\"];

        assertThatDataDeleted(testData, true);
    }

    /**
     * Remove data by full path study and don't understand trialId.
     */
    void testItDeleteDataByName() {
        processorLoad.process(
                new File("fixtures/Public Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        def inpData = ['id'  : null,
                       'path': "\\Test Studies\\${studyName}\\"];
        processorDelete.process(inpData);
        def testData = [
                'id'  : studyId,
                'path': "\\Test Studies\\${studyName}\\"];

        assertThatDataDeleted(testData, true);
    }

    /**
     * Remove data by trial Id and full path study.
     */
    void testItDeleteDataByIdAndName() {
        processorLoad.process(
                new File("fixtures/Public Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        def inpData = ['id'  : studyId,
                       'path': "\\Test Studies\\${studyName}\\"];
        processorDelete.process(inpData);
        def testData = [
                'id'  : studyId,
                'path': "\\Test Studies\\${studyName}\\"];

        assertThatDataDeleted(testData, true);
    }

    void testIdDeleteTopNode(){
        processorLoad.process(
                new File("fixtures/Public Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        def inpData = ['id'  : studyId,
                'path': "\\Test Studies\\${studyName}\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Test Studies\\")
    }

}
