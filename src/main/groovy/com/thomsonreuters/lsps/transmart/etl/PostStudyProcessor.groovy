package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.db.core.Database
import groovy.sql.Sql

import java.nio.file.Path

class PostStudyProcessor{
    Map config
    Database database
    Sql sql
    Path dir
    def studyInfo
    def jobId

    private static final String COPY_SECURITY_PROCEDURE_NAME = 'COPY_SECURITY_FROM_OTHER_STUDY'

    PostStudyProcessor(conf, sql, Path dir, studyInfo, jobId) {
        this.config = conf
        this.sql = sql
        this.dir = dir
        this.studyInfo = studyInfo
        this.jobId = jobId
    }

    boolean process() {
        if ((config?.replaceStudy) && (studyInfo.oldId) && ((String)studyInfo.id).toUpperCase() != ((String)studyInfo.oldId).toUpperCase()) {
            replaceStudy()
        }
        if (config.copySecurityFrom){
            copySecurityFrom()
        }

        return true;
    }

    protected void copySecurityFrom(){
        def studyId = studyInfo['id']
        def studyIdFrom = config.csStudyId
        sql.call("{call " + config.controlSchema + "." + COPY_SECURITY_PROCEDURE_NAME + "(?,?,?)}", [studyId, studyIdFrom, jobId])
    }

    protected void replaceStudy() {

        String studyId = studyInfo.id.toUpperCase()
        String oldStudyId = studyInfo.oldId.toUpperCase()
        String newToken = "EXP:$studyId"
        String oldToken = "EXP:$oldStudyId"

        sql.execute("DELETE FROM biomart.bio_experiment WHERE accession = :studyId", [studyId: studyId])
        sql.execute("DELETE FROM biomart.bio_data_uid WHERE unique_id = :newToken", [newToken: newToken])
        sql.execute("DELETE FROM searchapp.search_secure_object WHERE bio_data_unique_id = :newToken",
                [newToken: newToken])

        sql.executeUpdate("UPDATE biomart.bio_experiment SET accession = :studyId WHERE accession = :oldStudyId",
                [studyId   : studyId,
                 oldStudyId: oldStudyId])
        sql.executeUpdate("UPDATE biomart.bio_data_uid SET unique_id = :newToken WHERE unique_id = :oldToken",
                [newToken: newToken,
                 oldToken: oldToken])
        sql.executeUpdate("""
                UPDATE searchapp.search_secure_object SET bio_data_unique_id = :newToken
                WHERE bio_data_unique_id = :oldToken
            """, [newToken: newToken, oldToken: oldToken])
    }
}
