package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.Study
import com.thomsonreuters.lsps.transmart.fixtures.StudyInfo
import com.thomsonreuters.lsps.db.core.DatabaseType
import org.hamcrest.CoreMatchers
import org.hamcrest.core.IsNull
import spock.lang.Specification

import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasNode
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasSample
import static org.junit.Assert.*

class DeleteOperationTestCase extends Specification implements ConfigAwareTestCase {
    private SNPDataProcessor _processorLoadSNP
    private ClinicalDataProcessor _processorLoadClinical
    private DeleteDataProcessor _processorDelete
    private ProteinDataProcessor _processorLoadProtein
    private MIRNADataProcessor _processorLoadMIRNA
    private MetabolomicsDataProcessor _processorLoadMetabolomics
    private RBMDataProcessor _processorLoadRBM
    private RNASeqDataProcessor _processorLoadRNASeq

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

    void setup() {
        ConfigAwareTestCase.super.setUp()
        if (database?.databaseType == DatabaseType.Postgres) {
            runScript('I2B2_DELETE_PARTITION.sql')
        }
        runScript('I2B2_DELETE_ALL_DATA.sql')
        runScript('I2B2_DELETE_ALL_NODES.sql')
        runScript('I2B2_DELETE_1_NODE.sql')
        runScript('I2B2_REMOVE_EMPTY_PARENT_NODES.sql')
        processorDelete.process(id: null, path: studyPath)
        processorDelete.process(id: studyId, path: null)
    }

    String studyName = 'Test Study'
    String studyId = 'GSE0'
    String studyNameSNP = 'Test Study'
    String studyNameClinical = 'Test Study'
    String studyPath = "\\Test Studies\\${studyName}\\"

    def expressionData = Fixtures.getExpressionData(studyName, studyId)
    def delOp1ExpressionData = expressionData.copyWithSuffix('DOP1')
    def delOp2ExpressionData = expressionData.copyWithSuffix('DOP2')
    def delOp3ExpressionData = expressionData.copyWithSuffix('DOP3')
    def delOp4ExpressionData = expressionData.copyWithSuffix('DOP4')

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
        def sample = sql.firstRow('select VARIANT_SUBJECT_SUMMARY_ID from deapp.de_variant_subject_summary where assay_id in ( ' +
                'select assay_id ' +
                ' from deapp.de_subject_sample_mapping ' +
                ' where trial_name = ?)',
                trialId)
        assertThat(sample, IsNull.nullValue())
    }

    void assertThatTopNodeDelete(String pathTopNode, isDelete) {
        Integer i2b2CountExpect = (isDelete ? 0 : 1);
        def i2b2Count = sql.firstRow('select count(*) from i2b2metadata.i2b2 where c_fullname = ?', pathTopNode)
        assertEquals(i2b2CountExpect, i2b2Count[0] as Integer)

        def i2b2SecureCount = sql.firstRow('select count(*) from i2b2metadata.i2b2_secure where c_fullname = ?', pathTopNode)
        assertEquals("Top Node ${pathTopNode} removed", i2b2CountExpect, i2b2SecureCount[0] as Integer)

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
        given:
        expressionData.load(config)
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        assertThat(sql, hasNode(studyPath))
        processorDelete.process('id': studyId, 'path': null);

        expect:
        def testData = ['id': studyId, 'path': studyPath];
        assertThatDataDeleted(testData, true);

        cleanup:
        Study.deleteById(config, expressionData.studyId)
    }

    void testItDeleteDataSensitiveCase() {
        given:
        processorLoadClinical.process(
                new File("fixtures/Test Studies/${studyNameClinical}_${studyId}/ClinicalDataToUpload").toPath(),
                [name: studyNameClinical, node: "\\Delete Operation Test\\${studyNameClinical}\\".toString()])

        def inpData = ['id'  : studyId.toLowerCase(),
                       'path': null];
        processorDelete.process(inpData);

        expect:
        assertThatTopNodeDelete("\\Delete Operation Test\\", true);
    }
    /**
     * Remove data by full path study and don't understand trialId.
     */
    void testItDeleteDataByName() {
        given:
        expressionData.load(config)
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        assertThat(sql, hasNode(studyPath))
        processorDelete.process(id: null, path: studyPath)

        expect:
        def testData = ['id': studyId, 'path': studyPath]
        assertThatDataDeleted(testData, true);
    }

    /*
     * Check slash in the end of path name to remove
     */

    void testDeleteDataByNameWOSlash() {
        given:
        expressionData.load(config)
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        def inpData = ['id'  : null,
                       'path': "\\Test Studies\\${studyName}"];
        processorDelete.process(inpData);

        expect:
        def testData = [
                'id'  : studyId,
                'path': "\\Test Studies\\${studyName}\\"];

        assertThatDataDeleted(testData, true);
    }

    /**
     * Remove data by trial Id and full path study.
     */
    void testItDeleteDataByIdAndName() {
        given:
        expressionData.load(config)
        assertThat(sql, hasSample(studyId, 'TST1000000719'))
        def inpData = ['id'  : studyId,
                       'path': "\\Test Studies\\${studyName}\\"];
        processorDelete.process(inpData);

        expect:
        def testData = [
                'id'  : studyId,
                'path': "\\Test Studies\\${studyName}\\"];

        assertThatDataDeleted(testData, true);
    }

    void testItDeleteTopNode() {
        given:
        delOp1ExpressionData.load(config, "Delete Operation Test\\")
        assertThat(sql, hasSample(delOp1ExpressionData.studyId, 'TST1000000719'))
        def inpData = ['id'  : delOp1ExpressionData.studyId,
                       'path': "\\Delete Operation Test\\${delOp1ExpressionData.studyName}\\"];
        processorDelete.process(inpData);

        expect:
        assertThatTopNodeDelete("\\Delete Operation Test\\", true)
    }

    void testItNotDeleteTopNode() {
        given:
        delOp1ExpressionData.load(config, "Delete Operation Test\\")
        delOp2ExpressionData.load(config, "Delete Operation Test\\")
        assertThat(sql, hasSample(delOp1ExpressionData.studyId, 'TST1000000719'))
        assertThat(sql, hasSample(delOp2ExpressionData.studyId, 'TST1000000719'))

        when:
        def inpData = ['id'  : delOp2ExpressionData.studyId,
                       'path': "\\Delete Operation Test\\${delOp2ExpressionData.studyName}\\"];
        processorDelete.process(inpData);

        assertThatTopNodeDelete("\\Delete Operation Test\\", false)

        inpData = ['id': delOp1ExpressionData.studyId, 'path': "\\Delete Operation Test\\${delOp1ExpressionData.studyName}\\"];
        processorDelete.process(inpData);

        then:
        assertThatTopNodeDelete("\\Delete Operation Test\\", true)
    }

    void testItDeleteSNPData() {
        setup:
        processorLoadSNP.process(
                new File("fixtures/Test Studies/${studyNameSNP}_${studyId}/SNPDataToUpload").toPath(),
                [name: studyName, node: "\\Test Studies\\${studyNameSNP}".toString()])
        def inpData = ['id'  : studyId,
                       'path': "\\Test Studies\\${studyNameSNP}\\SNP\\"];
        def status = processorDelete.process(inpData);

        expect:
        assertTrue(status)
        assertThatTopNodeDelete("\\Test Studies\\${studyNameSNP}\\", true);
    }

    void testItDeleteSubNode() {
        given:
        expressionData.load(config)
        processorLoadSNP.process(
                new File("fixtures/Test Studies/${studyNameSNP}_${studyId}/SNPDataToUpload").toPath(),
                [name: studyName, node: "Test Studies\\${studyName}".toString()])
        when:
        def inpData = ['id'  : studyId,
                       'path': "\\Test Studies\\${studyName}\\SNP\\"];
        processorDelete.process(inpData);

        then:
        assertThatTopNodeDelete("\\Test Studies\\", false);
    }

    void testItDeleteClinicalData() {
        setup:
        String conceptPath = "\\Delete Operation Test\\${studyNameClinical}\\"
        String conceptPathForPatient = conceptPath + "Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\"

        processorLoadClinical.process(
                new File("fixtures/Test Studies/${studyNameClinical}_${studyId}/ClinicalDataToUpload").toPath(),
                [name: studyNameClinical, node: "\\Delete Operation Test\\${studyNameClinical}\\".toString()])

        def inpData = ['id'  : null,
                       'path': "\\Delete Operation Test\\${studyNameClinical}\\"];
        processorDelete.process(inpData);

        expect:
        assertThatTopNodeDelete("\\Delete Operation Test\\", true);
        assertThat(db, CoreMatchers.not(hasRecord("observation_fact", sourcesystem_cd: studyId)))
    }

    void testItDeleteSubNodeClinicalData() {
        setup:
        processorLoadClinical.process(
                new File("fixtures/Test Studies/${studyNameClinical}_${studyId}/ClinicalDataToUpload").toPath(),
                [name: studyNameClinical, node: "Test Studies\\${studyNameClinical}".toString()])

        def inpData = ['id'  : null,
                       'path': "\\Test Studies\\${studyNameClinical}\\Biomarker Data\\Mutations\\TST001 (Entrez ID: 1956)\\AA mutation\\T790M\\"];
        processorDelete.process(inpData);

        expect:
        assertThatTopNodeDelete("\\Test Studies\\", false)
    }

    void testItDeleteVCFData() {
        given:
        assertTrue(Fixtures.getVcfData().copyWithSuffix('DOP').load(config))

        when:
        def inpData = ['id'  : 'GSE0',
                       'path': "\\Test Studies\\${studyName} DOP\\"];
        processorDelete.process(inpData);

        then:
        assertThatDataDeleted(inpData, true)
        assertThatDataDeletedFromDeVariantSubSum(inpData)
    }

    void testItDeleteTopEmptyNode() {
        given:
        delOp3ExpressionData.load(config, "Delete Operation Test\\Test Study\\")
        delOp4ExpressionData.load(config, "Delete Operation Test\\Test Study\\")
        assertThat(sql, hasSample(delOp3ExpressionData.studyId, 'TST1000000719'))
        assertThat(sql, hasSample(delOp4ExpressionData.studyId, 'TST1000000719'))

        when:
        def inpData = [id  : delOp4ExpressionData.studyId,
                       path: "\\Delete Operation Test\\Test Study\\${delOp4ExpressionData.studyName}\\"];
        processorDelete.process(inpData);

        assertThatSubTopNodeDelete("\\Delete Operation Test\\Test Study\\", false)

        inpData = [id  : delOp3ExpressionData.studyId,
                   path: "\\Delete Operation Test\\Test Study\\${delOp3ExpressionData.studyName}\\"];
        processorDelete.process(inpData);

        then:
        assertThatSubTopNodeDelete("\\Delete Operation Test\\Test Study\\", true)
        assertThatTopNodeDelete("\\Delete Operation Test\\", true)
    }

    void testItDeletesProteinData() {
        given:
        def studyInfo = new StudyInfo('GSE37425', 'Test Protein Study')

        proteinDataProcessor.process(
                new File("fixtures/Test Studies/${studyInfo.name}_${studyInfo.id}/ProteinDataToUpload").toPath(),
                [name: studyInfo.name, node: "Test Studies\\${studyInfo.name}".toString()])

        def inpData = ['id'  : studyInfo.id,
                       'path': "\\Test Studies\\${studyInfo.name}\\"];
        def status = processorDelete.process(inpData);

        expect:
        assertTrue(status)
        assertThatTopNodeDelete("\\Test Studies\\${studyInfo.name}\\", true);
        assertRecordsWasDeleted('deapp.de_subject_protein_data', studyInfo.id);

        cleanup:
        Study.deleteById(config, studyInfo.id)
    }

    void testItDeletesMIRNAData() {
        setup:
        def mirnaStudyName = 'Test MirnaQpcr Study'
        def mirnaStudyId = 'TEST005'
        def mirnaType = 'MIRNA_QPCR'

        mirnaDataProcessor.process(
                new File("fixtures/Test Studies/${mirnaStudyName}/MIRNA_QPCRDataToUpload").toPath(),
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
                new File("fixtures/Test Studies/${mirnaStudyName}/MIRNA_SEQDataToUpload").toPath(),
                [name: mirnaStudyName, node: "Test Studies\\${mirnaStudyName}".toString(), base_datatype: mirnaType])
        inpData = ['id'  : mirnaStudyId,
                   'path': "\\Test Studies\\${mirnaStudyName}\\"];
        processorDelete.process(inpData);

        expect:
        assertThatTopNodeDelete("\\Test Studies\\${mirnaStudyName}\\", true);
        assertRecordsWasDeleted('deapp.de_subject_mirna_data', mirnaStudyId);
    }

    void testItDeletesMetabolomicsData() {
        setup:
        def metStudyName = 'Test Metabolomics Study'
        def metStudyId = 'GSE37427'

        metabolomicsDataProcessor.process(
                new File("fixtures/Test Studies/${metStudyName}/MetabolomicsDataToUpload").toPath(),
                [name: metStudyName, node: "Test Studies\\${metStudyName}".toString()])

        def inpData = ['id'  : metStudyId,
                       'path': "\\Test Studies\\${metStudyName}\\"];
        processorDelete.process(inpData);

        expect:
        assertThatTopNodeDelete("\\Test Studies\\${metStudyName}\\", true);
        assertRecordsWasDeleted('deapp.de_subject_metabolomics_data', metStudyId);
    }

    void testItDeletesRBMData() {
        setup:
        def rbmStudyName = 'Test RBM Study'
        def rbmStudyId = 'TESTRBM'
        Study.deleteByPath(config, "Test Studies\\${rbmStudyName}".toString())

        RBMDataProcessor.process(
                new File("fixtures/Test Studies/${rbmStudyName}/RBMDataToUpload").toPath(),
                [name: rbmStudyName, node: "Test Studies\\${rbmStudyName}".toString()])

        def inpData = ['id'  : rbmStudyId,
                       'path': "\\Test Studies\\${rbmStudyName}\\"];
        processorDelete.process(inpData);

        expect:
        assertThatTopNodeDelete("\\Test Studies\\${rbmStudyName}\\", true);
        assertRecordsWasDeleted('deapp.de_subject_rbm_data', rbmStudyId);
    }

    void testItDeletesRNASeqData() {
        setup:
        def rnaStudyName = 'Test RNASeq Study'
        def rnaStudyId = 'GSE_A_37424'

        RNASeqProcessor.process(
                new File("fixtures/Test Studies/${rnaStudyName}/RNASeqDataToUpload").toPath(),
                [name: rnaStudyName, node: "Test Studies\\${rnaStudyName}".toString()])

        def inpData = ['id'  : rnaStudyId,
                       'path': "\\Test Studies\\${rnaStudyName}\\"];
        processorDelete.process(inpData);

        expect:
        assertThatTopNodeDelete("\\Test Studies\\${rnaStudyName}\\", true);
        assertRecordsWasDeleted('deapp.de_subject_rna_data', rnaStudyId);
    }

    void testItDeletePartDataByIdWithoutPath() {
        setup:
        expressionData.load(config)
        sql.execute("""DELETE
                         FROM i2b2demodata.concept_dimension
                        WHERE sourcesystem_cd = ?""", [studyId])
        processorDelete.process('id': studyId, 'path': null)

        expect:
        def sample = sql.firstRow('select * from deapp.de_subject_sample_mapping where trial_name = ? and sample_cd = ?',
                studyId, 'TST1000000719')
        assertNull(sample)
    }

}
