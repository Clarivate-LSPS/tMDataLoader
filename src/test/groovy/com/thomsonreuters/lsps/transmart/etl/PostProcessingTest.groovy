package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.Fixtures
import com.thomsonreuters.lsps.transmart.fixtures.ClinicalData
import com.thomsonreuters.lsps.transmart.fixtures.Study
import spock.lang.Specification

class PostProcessingTest extends Specification implements ConfigAwareTestCase {
    ClinicalData clinicalData = Fixtures.clinicalData
    private ClinicalDataProcessor _processor

    void setup() {
        ConfigAwareTestCase.super.setUp()
        runScript('COPY_SECURITY_FROM_OTHER_STUDY.sql')
    }

    ClinicalDataProcessor getProcessor() {
        _processor ?: (_processor = new ClinicalDataProcessor(config))
    }

    def 'it should check copy security configuration'() {
        setup:
        def clinicalDataSecond = clinicalData.copyWithSuffix('SECOND')

        cleanOldData(clinicalData.studyId)
        cleanOldData(clinicalDataSecond.studyId)

        sql.executeUpdate("DELETE FROM searchapp.search_auth_sec_object_access WHERE auth_sec_obj_access_id = -1")
        sql.executeUpdate("DELETE FROM searchapp.search_auth_sec_object_access WHERE secure_object_id = " +
                "                            (SELECT search_secure_object_id FROM searchapp.search_secure_object WHERE bio_data_unique_id = 'EXP:'||?)", clinicalDataSecond.studyId)
        sql.executeUpdate("DELETE FROM searchapp.search_auth_sec_object_access WHERE secure_object_id = " +
                "                            (SELECT search_secure_object_id FROM searchapp.search_secure_object WHERE bio_data_unique_id = 'EXP:'||?)", clinicalData.studyId)

        Study.deleteById(config, clinicalData.studyId)
        Study.deleteById(config, clinicalDataSecond.studyId)

        config.securitySymbol = 'Y'
        def result = clinicalData.load(config)

        def res = sql.firstRow("Select search_secure_object_id as ssoi FROM searchapp.search_secure_object where bio_data_unique_id = 'EXP:'||?", clinicalData.studyId)

        // auth_principal_id = 1 is 'admin' user
        // secure_access_level_id = 8 - default value for 'VIEW' level
        sql.executeInsert("""INSERT INTO searchapp.search_auth_sec_object_access
                        (auth_sec_obj_access_id, auth_principal_id, secure_object_id, secure_access_level_id)
                        VALUES
                        (-1, 1, ?, 8)
                        """, res.ssoi)


        config.securitySymbol = 'Y'
        config.copySecurityFrom = true
        config.csStudyId = clinicalData.studyId
        clinicalDataSecond.load(config, '\\Demographics\\Test Study SECOND\\')

        expect:
        checkSetSecurityStatus(clinicalData.studyId, 1)
        checkSetSecurityStatus(clinicalDataSecond.studyId, 1)

        def cnt = sql.firstRow("""SELECT count(*) as cnt FROM searchapp.search_auth_sec_object_access WHERE secure_object_id =
                            (SELECT search_secure_object_id FROM searchapp.search_secure_object WHERE bio_data_unique_id = 'EXP:'||?)""", clinicalDataSecond.studyId)
        assert (cnt.cnt == 1), "Number of records in searchapp.search_auth_sec_object_access doesn't match expected value"


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

}
