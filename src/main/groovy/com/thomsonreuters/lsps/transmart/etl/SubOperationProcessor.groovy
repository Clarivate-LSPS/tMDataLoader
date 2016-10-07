package com.thomsonreuters.lsps.transmart.etl

import groovy.sql.Sql

abstract class SubOperationProcessor {
    def config
    Sql sql

    public static final String PUBLIC_TOKEN = "EXP:PUBLIC"

    SubOperationProcessor(Object conf) {
        config = conf
    }

    Boolean process() {
        def res = false
        TransmartDatabaseFactory.newDatabase(config).withSql { sql ->
            this.sql = sql
            res = processing()
        }
        res
    }

    abstract Boolean processing()

    Boolean existSecurityConfiguration(String studyId) {
        def exist = sql.firstRow('select count(accession) as cnt FROM biomart.bio_experiment WHERE accession = ?', studyId)
        return exist.cnt > 0
    }

    String getBioExperimentIdByAccession(String studyId) {
        def id = sql.firstRow('select bio_experiment_id bei FROM biomart.bio_experiment WHERE accession = ?', studyId)
        return id.bei
    }

    Boolean existStudyId(String studyId) {
        def exist = sql.firstRow('select count(sourcesystem_cd) as cnt from i2b2metadata.i2b2 where sourcesystem_cd = ?', studyId)
        return exist.cnt > 0
    }

    String getStudyIdByPath(String path) {
        path = ('\\' + path + '\\').replace('\\\\', '\\')
        def result = sql.firstRow('SELECT DISTINCT sourcesystem_cd as scd FROM i2b2metadata.i2b2 WHERE c_fullname LIKE ? ESCAPE \'`\'', path + '%')
        result?.scd
    }

    String getSecurityTokenByPath(String path) {
        path = ('\\' + path + '\\').replace('\\\\', '\\')
        def result = sql.firstRow('SELECT secure_obj_token as sot FROM i2b2metadata.i2b2_secure WHERE c_fullname = ?', path)
        result?.sot
    }

    def updateStudyId(String studyId, String newStudyId) {
        sql.executeUpdate('UPDATE biomart.bio_experiment SET accession = :newStudyId WHERE accession = :studyId',
                [newStudyId: newStudyId, studyId: studyId])
        sql.executeUpdate('UPDATE biomart.bio_data_uid SET unique_id = :newStudyId WHERE unique_id = :studyId',
                [newStudyId: ('EXP:' + newStudyId), studyId: ('EXP:' + studyId)])
        sql.executeUpdate('UPDATE searchapp.search_secure_object SET bio_data_unique_id = :newStudyId, display_name = replace(display_name, :studyId, :newStudyId) WHERE bio_data_unique_id = \'EXP:\' || :studyId',
                [newStudyId: 'EXP:' + newStudyId, studyId: studyId])
    }

    def updateSecurityToken(token, path) {
        path = ('\\' + path + '\\').replace('\\\\', '\\')
        def trial = getStudyIdByPath(path)
        sql.executeUpdate("UPDATE i2b2demodata.observation_fact SET tval_char = :token WHERE concept_cd = 'SECURITY' AND sourcesystem_cd = :trial",
                [token: token, trial: trial])
        sql.executeUpdate("UPDATE i2b2metadata.i2b2_secure SET secure_obj_token = :token WHERE c_fullname = :path",
                [token: token, path: path])
        sql.executeUpdate("UPDATE i2b2demodata.patient_trial SET secure_obj_token = :token WHERE trial = :trial",
                [token: token, trial: trial])
    }

    Boolean checkPathForTop(String path) {
        path = ('\\' + path + '\\').replace('\\\\', '\\')
        def count = sql.firstRow("SELECT count(*) as cnt FROM i2b2metadata.i2b2 WHERE c_fullname = ? AND c_visualattributes = 'FAS'", path)
        return count.cnt == 1
    }

    void deleteOldStudyConfiguration(String studyId) {
        sql.executeUpdate('DELETE FROM biomart.bio_experiment WHERE accession = ?', studyId)
        sql.executeUpdate('DELETE FROM biomart.bio_data_uid WHERE unique_id = ?', ('EXP:' + studyId))
        sql.executeUpdate('DELETE FROM searchapp.search_secure_object WHERE bio_data_unique_id = ?', ('EXP:' + studyId))
    }
}
