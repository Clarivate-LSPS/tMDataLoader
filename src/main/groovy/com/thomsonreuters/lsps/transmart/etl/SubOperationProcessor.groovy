package com.thomsonreuters.lsps.transmart.etl

import groovy.sql.Sql

abstract class SubOperationProcessor {
    def config
    Sql sql

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
        def result = sql.firstRow('SELECT DISTINCT sourcesystem_cd as scd FROM i2b2metadata.i2b2 WHERE c_fullname LIKE ?ESCAPE \'`\'', path + '%')
        result.scd
    }

    def updateStudyId(String studyId, String newStudyId) {
        sql.executeUpdate('UPDATE biomart.bio_experiment SET accession = :newStudyId WHERE accession = :studyId',
                [newStudyId: newStudyId, studyId: studyId])
        sql.executeUpdate('UPDATE biomart.bio_data_uid SET unique_id = :newStudyId WHERE unique_id = :studyId',
                [newStudyId: ('EXP:' + newStudyId), studyId: ('EXP:' + studyId)])
        sql.executeUpdate('UPDATE searchapp.search_secure_object SET bio_data_unique_id = :newStudyId WHERE bio_data_unique_id = :studyId',
                [newStudyId: 'EXP:' + newStudyId, studyId: 'EXP:' + studyId])
    }

    Boolean checkPathForTop(String path) {
        def count = sql.firstRow("SELECT count(*) as cnt FROM i2b2metadata.i2b2 WHERE c_fullname = ? AND c_visualattributes = 'FAS'", path)
        return count.cnt == 1
    }

    void deleteOldStudyConfiguration(String studyId) {
        sql.executeUpdate('DELETE FROM biomart.bio_experiment WHERE accession = ?', studyId)
        sql.executeUpdate('DELETE FROM biomart.bio_data_uid WHERE unique_id = ?', ('EXP:' + studyId))
        sql.executeUpdate('DELETE FROM searchapp.search_secure_object WHERE bio_data_unique_id = ?', ('EXP:' + studyId))
    }
}
