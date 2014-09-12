package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import org.hamcrest.core.IsNull

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.junit.Assert.assertThat

class DeleteOperationTestCase extends ConfigAwareTestCase {
    private ExpressionDataProcessor _processorLoad
    private SNPDataProcessor _processorLoadSNP
    private ClinicalDataProcessor _processorLoadClinical
    private DeleteDataProcessor _processorDelete
    private VCFDataProcessor _processorLoadVCF

    VCFDataProcessor getDataProcessor() {
        _processorLoadVCF ?: (_processorLoadVCF = new VCFDataProcessor(config))
    }

    ExpressionDataProcessor getExpressionDataProcessor() {
        _processorLoad ?: (_processorLoad = new ExpressionDataProcessor(config))
    }

    DeleteDataProcessor getProcessorDelete() {
        _processorDelete ?: (_processorDelete = new DeleteDataProcessor(config))
    }

    SNPDataProcessor getProcessorLoadSNP() {
        _processorLoadSNP ?: (_processorLoadSNP = new SNPDataProcessor(config))
    }

    ClinicalDataProcessor getProcessorLoadClinical() {
        _processorLoadClinical ?: (_processorLoadClinical = new ClinicalDataProcessor(config))
    }

    @Override
    void setUp() {
        super.setUp()
        runScript('I2B2_DELETE_ALL_DATA.sql')
    }

    String studyName = 'Test Study'
    String studyId = 'GSE0'
    String studyNameSNP = 'Test Study'
    String studyNameClinical = 'Test Study'
    String studyPath = "\\Test Studies\\${studyName}\\"

    void assertThatDataDeleted(inpData, boolean isDelete) {
        String fullName = inpData['path'].toString();
        String trialId = inpData['id'].toString();
        Integer i2b2CountExpect = (isDelete ? 0 : 1);

        def i2b2Count = sql.firstRow('select count(*) from i2b2metadata.i2b2 where c_fullname = ?', fullName)
        assertEquals(i2b2CountExpect, i2b2Count[0] as Integer)

        def i2b2SecureCount = sql.firstRow('select count(*) from i2b2metadata.i2b2_secure where c_fullname = ?', fullName)
        assertEquals(i2b2CountExpect, i2b2SecureCount[0] as Integer)

        def sample = sql.firstRow('select * from deapp.de_subject_sample_mapping where trial_name = ? and sample_cd = ?',
                trialId, 'STD')
        if (isDelete)
            assertThat(sample, IsNull.nullValue())
        else
            assertThat(sample, IsNull.notNullValue())

    }

    void assertThatDataDeletedFromDeVariantSubSum(inpData){
        String trialId = inpData['id'].toString();
        def sample = sql.firstRow('select VARIANT_SUBJECT_SUMMARY_ID from deapp.de_variant_subject_summary where assay_id in (\n' +
                'select assay_id\n' +
                '\tfrom deapp.de_subject_sample_mapping \n' +
                '\twhere trial_name = ?)',
                trialId)
        assertThat(sample, IsNull.nullValue())
    }

    void assertThatTopNodeDelete(String pathTopNode, isDelete){
        Integer i2b2CountExpect = (isDelete ? 0 : 1);
        def i2b2Count = sql.firstRow('select count(*) from i2b2metadata.i2b2 where c_fullname = ?', pathTopNode)
        assertEquals(i2b2Count[0] as Integer, i2b2CountExpect)

        def i2b2SecureCount = sql.firstRow('select count(*) from i2b2metadata.i2b2_secure where c_fullname = ?', pathTopNode)
        assertEquals(i2b2SecureCount[0] as Integer, i2b2CountExpect)

        def tableAccessCount = sql.firstRow('select count(*) from i2b2metadata.table_access where c_fullname = ?', pathTopNode)
        assertEquals(tableAccessCount[0] as Integer, i2b2CountExpect)
    }

    void assertThatSubTopNodeDelete(String pathTopNode, isDelete){
        Integer i2b2CountExpect = (isDelete ? 0 : 1);
        def i2b2Count = sql.firstRow('select count(*) from i2b2metadata.i2b2 where c_fullname = ?', pathTopNode)
        assertEquals(i2b2Count[0] as Integer, i2b2CountExpect)

        def i2b2SecureCount = sql.firstRow('select count(*) from i2b2metadata.i2b2_secure where c_fullname = ?', pathTopNode)
        assertEquals(i2b2SecureCount[0] as Integer, i2b2CountExpect)
        Integer index =pathTopNode.indexOf('\\');
        pathTopNode=pathTopNode.substring(index, pathTopNode.indexOf('\\', index+1)+1);
        def tableAccessCount = sql.firstRow('select count(*) from i2b2metadata.table_access where c_fullname = ?', pathTopNode)
        assertEquals(tableAccessCount[0] as Integer, i2b2CountExpect)
    }
    /**
     * Remove data by Id and don't understand full path to study.
     * If system exist study with trialId equals GSE0 test is down
     */
    void testItDeleteDataById() {
        expressionDataProcessor.process(Fixtures.getExpressionData(studyName, studyId),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        assertThat(sql, hasNode(studyPath))
        processorDelete.process('id': studyId, 'path': null);

        def testData = ['id'  : studyId, 'path': studyPath];
        assertThatDataDeleted(testData, true);
    }

    /**
     * Remove data by full path study and don't understand trialId.
     */
    void testItDeleteDataByName() {
        expressionDataProcessor.process(Fixtures.getExpressionData(studyName, studyId),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        assertThat(sql, hasNode(studyPath))
        processorDelete.process(id: null, path: studyPath)
        def testData = ['id'  : studyId, 'path': studyPath]
        assertThatDataDeleted(testData, true);
    }

    /*
     * Check slash in the end of path name to remove
     */
    void testDeleteDataByNameWOSlash() {
        expressionDataProcessor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        def inpData = ['id'  : null,
                'path': "\\Test Studies\\${studyName}"];
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
        expressionDataProcessor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
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
        expressionDataProcessor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        def inpData = ['id'  : studyId,
                'path': "\\Test Studies\\${studyName}\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Test Studies\\", true)
    }

    void testItNotDeleteTopNode(){
        expressionDataProcessor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}_1".toString()])
        expressionDataProcessor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}_2".toString()])
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        def inpData = ['id'  : studyId,
                'path': "\\Test Studies\\${studyName}_2\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Test Studies\\", false)

        inpData = ['id'  : studyId,
                'path': "\\Test Studies\\${studyName}_1\\"];
        processorDelete.process(inpData);
        assertThatTopNodeDelete("\\Test Studies\\", true)

    }

    void testItDeleteSNPData(){
        processorLoadSNP.process(
                new File("fixtures/Test Studies/${studyNameSNP}_${studyId}/SNPDataToUpload"),
                [name: studyName, node: "\\Test Studies\\${studyNameSNP}".toString()])
        def inpData = ['id'  : studyId,
                'path': "\\Test Studies\\${studyNameSNP}\\SNP\\"];
        processorDelete.process(inpData);


        assertThatTopNodeDelete("\\Test Studies\\Test Studies_1\\", true);
    }

    void testItDeleteSubNode(){
        expressionDataProcessor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        processorLoadSNP.process(
                new File("fixtures/Test Studies/${studyNameSNP}_${studyId}/SNPDataToUpload"),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])

        def inpData = ['id'  : studyId,
                'path': "\\Test Studies\\${studyName}\\SNP\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Test Studies\\", false);
    }

    void testItDeleteClinicalData(){
        String conceptPath = "\\Test Studies\\${studyNameClinical}\\"
        String conceptPathForPatient = conceptPath + "Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\"

        processorLoadClinical.process(
                new File("fixtures/Test Studies/${studyNameClinical}_${studyId}/ClinicalDataToUpload"),
                [name: studyNameClinical, node: "\\Test Studies\\${studyNameClinical}\\".toString()])

        def inpData = ['id'  : null,
                'path': "\\Test Studies\\${studyNameClinical}\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Test Studies\\", true);
    }

    void testItDeleteSubNodeClinicalData(){
        String conceptPath = "\\Test Studies\\${studyNameClinical}\\"
        String conceptPathForPatient = conceptPath + "Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\"

        processorLoadClinical.process(
                new File("fixtures/Test Studies/${studyNameClinical}_${studyId}/ClinicalDataToUpload"),
                [name: studyNameClinical, node: "Test Studies\\${studyNameClinical}".toString()])

        def inpData = ['id'  : null,
                'path': "\\Test Studies\\${studyNameClinical}\\Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\T790M\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Test Studies\\", false);
    }

    void testItDeleteVCFData(){
        assertTrue(dataProcessor.process(Fixtures.vcfData, [name: studyName, node: $/Test Studies\${studyName}_VCF/$]))

        def inpData = ['id'  : 'GSE0',
                'path': "\\Test Studies\\${studyName}_VCF\\"];
        processorDelete.process(inpData);

        assertThatDataDeleted(inpData, true)
        assertThatDataDeletedFromDeVariantSubSum(inpData)
    }

    void testItDeleteTopEmptyNode()
    {
        expressionDataProcessor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\Test Study\\${studyName}_1".toString()])
        expressionDataProcessor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Test Studies\\Test Study\\${studyName}_2".toString()])
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        def inpData = ['id'  : studyId,
                'path': "\\Test Studies\\Test Study\\${studyName}_2\\"];
        processorDelete.process(inpData);

        assertThatSubTopNodeDelete("\\Test Studies\\Test Study\\",false)

        inpData = ['id'  : studyId,
                'path': "\\Test Studies\\Test Study\\${studyName}_1\\"];
        processorDelete.process(inpData);
        assertThatSubTopNodeDelete("\\Test Studies\\Test Study\\", true)
        assertThatTopNodeDelete("\\Test Studies\\", true)
    }
}
