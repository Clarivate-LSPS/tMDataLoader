package com.thomsonreuters.lsps.transmart.fixtures

import com.thomsonreuters.lsps.transmart.etl.DeleteCrossProcessor
import com.thomsonreuters.lsps.transmart.etl.DeleteDataProcessor
import groovy.sql.Sql

/**
 * Date: 27.04.2015
 * Time: 12:49
 */
class Study {
    private List<AbstractData> dataList = []

    static void deleteById(config, String studyId) {
        new DeleteDataProcessor(config).process(id: studyId)
    }

    static void deleteByPath(config, String path) {
        new DeleteDataProcessor(config).process(path: path)
    }

    static void deleteCross(config, String path) {
        new DeleteCrossProcessor(config).process([
                path            : path,
                isDeleteConcepts: true
        ])
    }

    static void deleteCrossByConceptCD(config, String conceptCD) {
        new DeleteCrossProcessor(config).process([
                conceptCD       : conceptCD,
                isDeleteConcepts: true
        ])
    }

    Study withData(AbstractData data) {
        this.dataList.add(data)
        return this
    }

    static void deleteStudyMetaDataById(String studyId, Sql sql) {
        if (!(studyId && sql)) {
            throw new IllegalArgumentException();
        }

        def experimentId =
                sql.firstRow("select bio_experiment_id from biomart.bio_experiment where accession= ?", studyId)?.bio_experiment_id

        if (!experimentId)
            return

        sql.execute("delete from biomart.bio_clinical_trial where trial_number = $studyId")
        sql.execute("delete from biomart.bio_data_uid where bio_data_id = ?", experimentId)
        sql.execute("delete from biomart.bio_experiment where bio_experiment_id = ?", experimentId)

        def folderId = sql.firstRow("select folder_id from fmapp.fm_folder_association where object_uid=?", "EXP:$studyId".toString())?.folder_id
        if (!folderId)
            return

        sql.execute("delete from fmapp.fm_folder_association where folder_id = ?", folderId)
        sql.execute("delete from fmapp.fm_data_uid where fm_data_id = ?", folderId)
        sql.execute("delete from fmapp.fm_folder where folder_id = ?", folderId)

        sql.execute("delete from amapp.am_tag_association where subject_uid= ?", "FOL:$folderId".toString())
    }
}
