package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import org.hamcrest.core.IsNull

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.junit.Assert.assertThat

class DeleteOperationTestCase extends GroovyTestCase implements ConfigAwareTestCase {
    private ExpressionDataProcessor _processorLoad
    private SNPDataProcessor _processorLoadSNP
    private ClinicalDataProcessor _processorLoadClinical
    private DeleteDataProcessor _processorDelete
    private VCFDataProcessor _processorLoadVCF
    private ProteinDataProcessor _processorLoadProtein
    private MIRNADataProcessor _processorLoadMIRNA
    private MetabolomicsDataProcessor _processorLoadMetabolomics
    private RBMDataProcessor _processorLoadRBM
    private RNASeqDataProcessor _processorLoadRNASeq

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

    ProteinDataProcessor getProteinDataProcessor() {
        _processorLoadProtein ?: (_processorLoadProtein = new ProteinDataProcessor(config))
    }

    MIRNADataProcessor getMirnaDataProcessor() {
        _processorLoadMIRNA ?: (_processorLoadMIRNA = new MIRNADataProcessor(config))
    }

    MetabolomicsDataProcessor getMetabolomicsDataProcessor() {
        _processorLoadMetabolomics ?: (_processorLoadMetabolomics = new MetabolomicsDataProcessor(config))
    }

    RBMDataProcessor getRBMDataProcessor() {
        _processorLoadRBM ?: (_processorLoadRBM = new RBMDataProcessor(config))
    }

    RNASeqDataProcessor getRNASeqProcessor() {
        _processorLoadRNASeq ?: (_processorLoadRNASeq = new RNASeqDataProcessor(config))
    }

    @Override
    void setUp() {
        ConfigAwareTestCase.super.setUp()
        runScript('I2B2_DELETE_PARTITION.sql')
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

    void assertThatDataDeletedFromDeVariantSubSum(inpData) {
        String trialId = inpData['id'].toString();
        def sample = sql.firstRow('select VARIANT_SUBJECT_SUMMARY_ID from deapp.de_variant_subject_summary where assay_id in (\n' +
                'select assay_id\n' +
                '\tfrom deapp.de_subject_sample_mapping \n' +
                '\twhere trial_name = ?)',
                trialId)
        assertThat(sample, IsNull.nullValue())
    }

    void assertThatTopNodeDelete(String pathTopNode, isDelete) {
        Integer i2b2CountExpect = (isDelete ? 0 : 1);
        def i2b2Count = sql.firstRow('select count(*) from i2b2metadata.i2b2 where c_fullname = ?', pathTopNode)
        assertEquals(i2b2CountExpect, i2b2Count[0] as Integer)

        def i2b2SecureCount = sql.firstRow('select count(*) from i2b2metadata.i2b2_secure where c_fullname = ?', pathTopNode)
        assertEquals(i2b2CountExpect, i2b2SecureCount[0] as Integer)

        def tableAccessCount = sql.firstRow('select count(*) from i2b2metadata.table_access where c_fullname = ?', pathTopNode)
        assertEquals(i2b2CountExpect, tableAccessCount[0] as Integer)
    }

    void assertThatSubTopNodeDelete(String pathTopNode, isDelete) {
        Integer i2b2CountExpect = (isDelete ? 0 : 1);
        def i2b2Count = sql.firstRow('select count(*) from i2b2metadata.i2b2 where c_fullname = ?', pathTopNode)
        assertEquals(i2b2CountExpect, i2b2Count[0] as Integer)

        Integer index = pathTopNode.indexOf('\\');
        pathTopNode = pathTopNode.substring(index, pathTopNode.indexOf('\\', index + 1) + 1);
        def tableAccessCount = sql.firstRow('select count(*) from i2b2metadata.table_access where c_fullname = ?', pathTopNode)
        assertEquals(i2b2CountExpect, tableAccessCount[0] as Integer)
    }

    void assertRecordsWasDeleted(tableName, trialId) {
        def count = sql.firstRow('select count(*) from ' + tableName + ' where trial_name = ?', trialId)
        assertEquals(0, count[0] as Integer)
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

        def testData = ['id': studyId, 'path': studyPath];
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
        def testData = ['id': studyId, 'path': studyPath]
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

    void testItDeleteTopNode() {
        expressionDataProcessor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Delete Operation Test\\${studyName}".toString()])
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        def inpData = ['id'  : studyId,
                       'path': "\\Delete Operation Test\\${studyName}\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Delete Operation Test\\", true)
    }

    void testItNotDeleteTopNode() {
        expressionDataProcessor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Delete Operation Test\\${studyName}_1".toString()])
        expressionDataProcessor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Delete Operation Test\\${studyName}_2".toString()])
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        def inpData = ['id'  : studyId,
                       'path': "\\Delete Operation Test\\${studyName}_2\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Delete Operation Test\\", false)

        inpData = ['id': studyId, 'path': "\\Delete Operation Test\\${studyName}_1\\"];
        processorDelete.process(inpData);
        assertThatTopNodeDelete("\\Delete Operation Test\\", true)

    }

    void testItDeleteSNPData() {
        processorLoadSNP.process(
                new File("fixtures/Test Studies/${studyNameSNP}_${studyId}/SNPDataToUpload"),
                [name: studyName, node: "\\Test Studies\\${studyNameSNP}".toString()])
        def inpData = ['id'  : studyId,
                       'path': "\\Test Studies\\${studyNameSNP}\\SNP\\"];
        processorDelete.process(inpData);


        assertThatTopNodeDelete("\\Test Studies\\Test Studies_1\\", true);
    }

    void testItDeleteSubNode() {
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

    void testItDeleteClinicalData() {
        String conceptPath = "\\Delete Operation Test\\${studyNameClinical}\\"
        String conceptPathForPatient = conceptPath + "Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\"

        processorLoadClinical.process(
                new File("fixtures/Test Studies/${studyNameClinical}_${studyId}/ClinicalDataToUpload"),
                [name: studyNameClinical, node: "\\Delete Operation Test\\${studyNameClinical}\\".toString()])

        def inpData = ['id'  : null,
                       'path': "\\Delete Operation Test\\${studyNameClinical}\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Delete Operation Test\\", true);
    }

    void testItDeleteSubNodeClinicalData() {
        processorLoadClinical.process(
                new File("fixtures/Test Studies/${studyNameClinical}_${studyId}/ClinicalDataToUpload"),
                [name: studyNameClinical, node: "Test Studies\\${studyNameClinical}".toString()])

        def inpData = ['id'  : null,
                       'path': "\\Test Studies\\${studyNameClinical}\\Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\T790M\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Test Studies\\", false);
    }

    void testItDeleteVCFData() {
        assertTrue(dataProcessor.process(Fixtures.vcfData, [name: studyName, node: $/Test Studies\${studyName}_VCF/$]))

        def inpData = ['id'  : 'GSE0',
                       'path': "\\Test Studies\\${studyName}_VCF\\"];
        processorDelete.process(inpData);

        assertThatDataDeleted(inpData, true)
        assertThatDataDeletedFromDeVariantSubSum(inpData)
    }

    void testItDeleteTopEmptyNode() {
        expressionDataProcessor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Delete Operation Test\\Test Study\\${studyName}_1".toString()])
        expressionDataProcessor.process(
                new File("fixtures/Test Studies/${studyName}_${studyId}/ExpressionDataToUpload"),
                [name: studyName, node: "Delete Operation Test\\Test Study\\${studyName}_2".toString()])
        assertThat(sql, hasSample(studyId, 'TST1000000719'))

        def inpData = [id: studyId, path: "\\Delete Operation Test\\Test Study\\${studyName}_2\\"];
        processorDelete.process(inpData);

        assertThatSubTopNodeDelete("\\Delete Operation Test\\Test Study\\", false)

        inpData = [id: studyId, path: "\\Delete Operation Test\\Test Study\\${studyName}_1\\"];
        processorDelete.process(inpData);
        assertThatSubTopNodeDelete("\\Delete Operation Test\\Test Study\\", true)
        assertThatTopNodeDelete("\\Delete Operation Test\\", true)
    }

    void testItDeletesProteinData() {
        def proteinStudyName = 'Test Protein Study'
        def proteinStudyId = 'GSE37425'

        proteinDataProcessor.process(
                new File("fixtures/Test Studies/${proteinStudyName}/ProteinDataToUpload"),
                [name: proteinStudyName, node: "Test Studies\\${proteinStudyName}".toString()])

        def inpData = ['id'  : proteinStudyId,
                       'path': "\\Test Studies\\${proteinStudyName}\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Test Studies\\${proteinStudyName}\\", true);
        assertRecordsWasDeleted('deapp.de_subject_protein_data', proteinStudyId);
    }

    void testItDeletesMIRNAData() {
        def mirnaStudyName = 'Test MirnaQpcr Study'
        def mirnaStudyId = 'TEST005'
        def mirnaType = 'MIRNA_QPCR'

        mirnaDataProcessor.process(
                new File("fixtures/Test Studies/${mirnaStudyName}/MIRNA_QPCRDataToUpload"),
                [name: mirnaStudyName, node: "Test Studies\\${mirnaStudyName}".toString(), base_datatype: mirnaType])

        def inpData = ['id'  : mirnaStudyId,
                       'path': "\\Test Studies\\${mirnaStudyName}\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Test Studies\\${mirnaStudyName}\\", true);
        assertRecordsWasDeleted('deapp.de_subject_mirna_data', mirnaStudyId);

        mirnaStudyName = 'Test MirnaSeq Study'
        mirnaStudyId = 'MIRNASEQBASED'
        mirnaType = 'MIRNA_SEQ'

        mirnaDataProcessor.process(
                new File("fixtures/Test Studies/${mirnaStudyName}/MIRNA_SEQDataToUpload"),
                [name: mirnaStudyName, node: "Test Studies\\${mirnaStudyName}".toString(), base_datatype: mirnaType])
        inpData = ['id'  : mirnaStudyId,
                   'path': "\\Test Studies\\${mirnaStudyName}\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Test Studies\\${mirnaStudyName}\\", true);
        assertRecordsWasDeleted('deapp.de_subject_mirna_data', mirnaStudyId);
    }

    void testItDeletesMetabolomicsData() {
        def metStudyName = 'Test Metabolomics Study'
        def metStudyId = 'GSE37427'

        metabolomicsDataProcessor.process(
                new File("fixtures/Test Studies/${metStudyName}/MetabolomicsDataToUpload"),
                [name: metStudyName, node: "Test Studies\\${metStudyName}".toString()])

        def inpData = ['id'  : metStudyId,
                   'path': "\\Test Studies\\${metStudyName}\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Test Studies\\${metStudyName}\\", true);
        assertRecordsWasDeleted('deapp.de_subject_metabolomics_data', metStudyId);
    }

    void testItDeletesRBMData() {
        def rbmStudyName = 'Test RBM Study'
        def rbmStudyId = 'TESTRBM'

        RBMDataProcessor.process(
                new File("fixtures/Test Studies/${rbmStudyName}/RBMDataToUpload"),
                [name: rbmStudyName, node: "Test Studies\\${rbmStudyName}".toString()])

        def inpData = ['id'  : rbmStudyId,
                       'path': "\\Test Studies\\${rbmStudyName}\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Test Studies\\${rbmStudyName}\\", true);
        assertRecordsWasDeleted('deapp.de_subject_rbm_data', rbmStudyId);
    }

    void testItDeletesRNASeqData() {
        def rnaStudyName = 'Test RNASeq Study'
        def rnaStudyId = 'GSE_A_37424'

        RNASeqProcessor.process(
                new File("fixtures/Test Studies/${rnaStudyName}/RNASeqDataToUpload"),
                [name: rnaStudyName, node: "Test Studies\\${rnaStudyName}".toString()])

        def inpData = ['id'  : rnaStudyId,
                       'path': "\\Test Studies\\${rnaStudyName}\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Test Studies\\${rnaStudyName}\\", true);
        assertRecordsWasDeleted('deapp.de_subject_rna_data', rnaStudyId);
    }
}
