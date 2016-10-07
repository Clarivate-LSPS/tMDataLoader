package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import com.thomsonreuters.lsps.transmart.fixtures.Study
import spock.lang.Specification
import static com.thomsonreuters.lsps.transmart.etl.matchers.SqlMatchers.hasRecord

import static org.junit.Assert.assertThat

class SubOperationProcessorTest extends Specification implements ConfigAwareTestCase {
    ClinicalData clinicalData = Fixtures.clinicalData.copyWithSuffix('MOVESC')
    private ClinicalDataProcessor _processor
    private PreOperationProccesor _preOperationProcessor
    private PostOperationProcessor _postOperationProcessor
    private MoveStudyProcessor _moveStudyProcessor
    private OperationProcessor _operationProcessor

    void setup() {
        ConfigAwareTestCase.super.setUp()
    }

    OperationProcessor getOperationProcessor(){
        _operationProcessor ?: (_operationProcessor = new OperationProcessor(config))
    }

    ClinicalDataProcessor getProcessor() {
        _processor ?: (_processor = new ClinicalDataProcessor(config))
    }

    PreOperationProccesor getPreOperationProcessor() {
        _preOperationProcessor ?: (_preOperationProcessor = new PreOperationProccesor(config))
    }

    PostOperationProcessor getPostOperationProcessor(){
        _postOperationProcessor ?: (_postOperationProcessor = new PostOperationProcessor(config))
    }

    MoveStudyProcessor getMoveStudyProcessor() {
        _moveStudyProcessor ?: (_moveStudyProcessor = new MoveStudyProcessor(config))
    }

    def 'it should check save studyId to new studyId'() {
        setup:
        def oldPath = '\\Test Studies\\' + clinicalData.studyName + '\\'
        def newPath = '\\Test Studies\\' + clinicalData.studyName + ' new way\\'

        Study.deleteById(config, clinicalData.studyId)
        cleanOldData(clinicalData.studyId)
        cleanOldData(clinicalData.studyId + '2')

        config.securitySymbol = 'Y'
        clinicalData.load(config)

        config.moveStudy = true
        config.moveStudyOldPath = oldPath
        config.moveStudyNewPath = newPath
        config.keepSecurityAs = clinicalData.studyId + '2'

        operationProcessor.process()

        expect:
        checkSetSecurityStatus(clinicalData.studyId, 0)
        checkSetSecurityStatus((clinicalData.studyId + '2'), 1)
    }

    def 'it should check use security configuration from saved configuration'(){
        setup:
        def securityConfig = clinicalData.studyId + '_NEW'
        def secondClinicalData = clinicalData.copyWithSuffix('MEGA')

        def firstPath = '\\Test Studies\\' + clinicalData.studyName + '\\'
        def forgetPath = '\\Test Studies\\' + clinicalData.studyName + ' new way\\'
        def secondPath = '\\Test Studies\\' + secondClinicalData.studyName + '\\'

        Study.deleteById(config, clinicalData.studyId)
        Study.deleteById(config, secondClinicalData.studyId)

        cleanOldData(clinicalData.studyId)
        cleanOldData(secondClinicalData.studyId)
        cleanOldData(securityConfig)

        config.securitySymbol = 'Y'
        clinicalData.load(config)

        config.moveStudy = true
        config.moveStudyOldPath = firstPath
        config.moveStudyNewPath = forgetPath
        config.keepSecurityAs = securityConfig

        operationProcessor.process()

        secondClinicalData.load(config)

        config.moveStudyOldPath = secondPath
        config.moveStudyNewPath = firstPath
        config.useSecurityFrom = securityConfig
        config.keepSecurityAs = null

        operationProcessor.process()

        expect:
        checkSetSecurityStatus(clinicalData.studyId, 0)
        checkSetSecurityStatus(secondClinicalData.studyId, 0)
        checkSetSecurityStatus(securityConfig, 1)

    }

    private void cleanOldData(remStudyId) {
        remStudyId = remStudyId.toUpperCase()
        def tables = [
                ['table': 'biomart.bio_experiment', 'value': remStudyId, 'column': 'accession'],
                ['table': 'biomart.bio_data_uid', 'value': "EXP:$remStudyId", 'column': 'unique_id'],
                ['table': 'searchapp.search_secure_object', 'value': "EXP:$remStudyId", 'column': 'bio_data_unique_id']
        ]
        for (t in tables) {
            sql.execute("DELETE FROM ${t.table} WHERE ${t.column} = ?", [t.value as String])
        }

    }

    private void checkSetSecurityStatus(String checkStudyId, value) {
        checkStudyId = checkStudyId.toUpperCase()
        def tables = [
                ['table': 'biomart.bio_experiment', 'value': checkStudyId, 'column': 'accession'],
                ['table': 'biomart.bio_data_uid', 'value': "EXP:$checkStudyId", 'column': 'unique_id'],
                ['table': 'searchapp.search_secure_object', 'value': "EXP:$checkStudyId", 'column': 'bio_data_unique_id']
        ]
        for (t in tables) {
            def res = sql.firstRow("SELECT COUNT(*) AS cnt FROM ${t.table} WHERE ${t.column} = ?", [t.value as String])
            assert res.cnt == value, "${checkStudyId}: Number of records in ${t.table} (${res.cnt}) doesn't match expected ($value)"
        }
    }

    def 'it should check move security study to public study'(){
        setup:
        def secondClinicalData = clinicalData.copyWithSuffix('MOVEPUBLIC')

        def firstPath = '\\Test Studies\\' + clinicalData.studyName + '\\'
        def secondPath = '\\Test Studies\\' + secondClinicalData.studyName + '\\'

        Study.deleteById(config, clinicalData.studyId)
        Study.deleteById(config, secondClinicalData.studyId)

        Study.deleteByPath(config, firstPath)
        Study.deleteByPath(config, secondPath)

        cleanOldData(clinicalData.studyId)
        cleanOldData(secondClinicalData.studyId)

        config.securitySymbol = 'Y'
        clinicalData.load(config)

        config.securitySymbol = 'N'
        secondClinicalData.load(config)

        config.moveStudy = true
        config.moveStudyOldPath = firstPath
        config.moveStudyNewPath = secondPath
        config.replaceStudy = true

        operationProcessor.process()

        expect:
        checkSetSecurityStatus(clinicalData.studyId, 0)
        checkSetSecurityStatus(secondClinicalData.studyId, 0)
    }

    def 'it should check move public study to security study'(){
        setup:
        def secondClinicalData = clinicalData.copyWithSuffix('MOVESECURE')

        def firstPath = '\\Test Studies\\' + clinicalData.studyName + '\\'
        def secondPath = '\\Test Studies\\' + secondClinicalData.studyName + '\\'

        Study.deleteById(config, clinicalData.studyId)
        Study.deleteById(config, secondClinicalData.studyId)

        Study.deleteByPath(config, firstPath)
        Study.deleteByPath(config, secondPath)

        cleanOldData(clinicalData.studyId)
        cleanOldData(secondClinicalData.studyId)

        config.securitySymbol = 'N'
        clinicalData.load(config)

        config.securitySymbol = 'Y'
        secondClinicalData.load(config)

        config.moveStudy = true
        config.moveStudyOldPath = firstPath
        config.moveStudyNewPath = secondPath
        config.replaceStudy = true

        operationProcessor.process()

        expect:
        checkSetSecurityStatus(clinicalData.studyId, 0)
        checkSetSecurityStatus(secondClinicalData.studyId, 1)
    }

    def 'it should check move security study to security study'(){
        setup:
        def secondClinicalData = clinicalData.copyWithSuffix('MOVESECURE')

        def firstPath = '\\Test Studies\\' + clinicalData.studyName + '\\'
        def secondPath = '\\Test Studies\\' + secondClinicalData.studyName + '\\'

        Study.deleteById(config, clinicalData.studyId)
        Study.deleteById(config, secondClinicalData.studyId)

        Study.deleteByPath(config, firstPath)
        Study.deleteByPath(config, secondPath)

        cleanOldData(clinicalData.studyId)
        cleanOldData(secondClinicalData.studyId)

        config.securitySymbol = 'Y'
        clinicalData.load(config)

        config.securitySymbol = 'Y'
        secondClinicalData.load(config)

        config.moveStudy = true
        config.moveStudyOldPath = firstPath
        config.moveStudyNewPath = secondPath
        config.replaceStudy = true

        operationProcessor.process()

        expect:
        checkSetSecurityStatus(clinicalData.studyId, 1)
        checkSetSecurityStatus(secondClinicalData.studyId, 0)
    }

    def 'it should check load study with use-security-from option'(){
        setup:
        Study.deleteById(config, clinicalData.studyId)
        cleanOldData(clinicalData.studyId)
        cleanOldData('GSE-TEST')

        sql.executeInsert("INSERT INTO biomart.bio_data_uid (bio_data_id,unique_id,bio_data_type) VALUES (-1, 'EXP:GSE-TEST', 'BIO_EXPERIMENT')")
        sql.executeInsert("INSERT INTO biomart.bio_experiment (bio_experiment_id, title, accession, etl_id) VALUES (-1, 'Test Experiment', 'GSE-TEST', 'METADATA:GSE-TEST')")
        sql.executeInsert("INSERT INTO searchapp.search_secure_object (search_secure_object_id, bio_data_id, display_name, data_type, bio_data_unique_id) VALUES (-1, -1, 'Test Studies - GSE-TEST', 'BIO_CLINICAL_TRIAL', 'EXP:GSE-TEST')")


        config.securitySymbol = 'Y'
        config.useSecurityFrom = 'GSE-TEST'
        clinicalData.load(config)

        expect:
        checkSetSecurityStatus(clinicalData.studyId, 1)
        checkSetSecurityStatus('GSE-TEST', 0)

        assertThat(db, hasRecord('biomart.bio_experiment', [bio_experiment_id: '-1'], null))
        assertThat(db, hasRecord('searchapp.search_secure_object', [bio_data_unique_id:"EXP:${clinicalData.studyId}"],[display_name:"Test Studies - EXP:${clinicalData.studyId}"]))
    }
}
