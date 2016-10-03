package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import com.thomsonreuters.lsps.transmart.fixtures.Study
import spock.lang.Specification

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

        Study.deleteByPath(config, newPath)
        Study.deleteByPath(config, oldPath)
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

        Study.deleteByPath(config, forgetPath)
        Study.deleteByPath(config, firstPath)
        Study.deleteByPath(config, firstPath)

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
}
