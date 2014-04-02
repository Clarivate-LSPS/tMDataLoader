package com.thomsonreuters.lsps.transmart.etl

import org.hamcrest.core.IsNull

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
        _processorDelete ?: (_processorDelete = new DeleteDataProcessor([
                logger: new Logger([isInteractiveMode: true]),
                db: connectionSettings,
                controlSchema: 'tm_cz',
                securitySymbol: 'N'
        ]))
    }

    String studyName = 'TestDeleteStudy'
    String studyId = 'GSE0'
    String platformId = 'GEX_TST'

    void isDataLoad(String sampleId, sampleData) {
        def sample = sql.firstRow('select * from deapp.de_subject_sample_mapping where trial_name = ? and sample_cd = ?',
                studyId, sampleId)
        assertThat(sample, notNullValue())
    }

    void assertThatDataDeleted(inpData, boolean isDelete) {
        String fullName = inpData['path'].toString();
        String trialId = inpData['id'].toString();
        Integer i2b2CountExpect = (isDelete?0:1);

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
    /**
     * Remove data by Id and don't understand full path to study.
     * If system exist study with trialId equals GSE0 test is down
     */
    void testItDeleteDataById() {
        processorLoad.process(
                new File("fixtures/Public Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        isDataLoad('TST1000000719', ['1007_s_at': 6.624529839]);
        def inpData = ['id': studyId,
                       'path': null];
        processorDelete.process(inpData);
        def testData = [
                'id': studyId,
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
        isDataLoad('TST1000000719', ['1007_s_at': 6.624529839]);
        def inpData = ['id': null,
                'path': "\\Test Studies\\${studyName}\\"];
        processorDelete.process(inpData);
        def testData = [
                'id': studyId,
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
        isDataLoad('TST1000000719', ['1007_s_at': 6.624529839]);
        def inpData = ['id': studyId,
                'path': "\\Test Studies\\${studyName}\\"];
        processorDelete.process(inpData);
        def testData = [
                'id': studyId,
                'path': "\\Test Studies\\${studyName}\\"];

        assertThatDataDeleted(testData, true);
    }


}
