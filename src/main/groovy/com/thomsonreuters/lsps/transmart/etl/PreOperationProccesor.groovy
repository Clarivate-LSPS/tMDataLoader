package com.thomsonreuters.lsps.transmart.etl

class PreOperationProccesor extends SubOperationProcessor {

    public PreOperationProccesor(Object configObject) {
        super(configObject)
    }

    @Override
    Boolean processing() {
        config.logger.log("Run pre-operation processor")

        if (config.deleteStudyById || config.deleteStudyByPath) {
            if (config.deleteStudyByPath){
                config.deleteStudyByIdValue = getStudyIdByPath(config.moveStudyOldPath)
            }
            if (existSecurityConfiguration(config.deleteStudyByIdValue)){
                def browseLink = sql.firstRow("select count(fm.folder_id) as cnt from fmapp.fm_folder_association fm, biomart.bio_data_uid bdu where fm.object_uid = bdu.unique_id and bdu.bio_data_id = ?",
                    getBioExperimentIdByAccession(config.deleteStudyByIdValue))
                if (browseLink.cnt > 0 && !config.deleteSecurity){
                    throw new DataProcessingException("Tab \"Browse\" contain link to this study. You have to use tMDataLoader option --delete-security for delete this study.")
                }
            }
        }

        if (config.moveStudy) {
            if (config.keepSecurityAs) {
                String studyId = getStudyIdByPath(config.moveStudyOldPath)

                if (!existStudyId(config.keepSecurityAs)) {
                    if (checkPathForTop(config.moveStudyOldPath)) {
                        updateStudyId(studyId, config.keepSecurityAs)
                    } else {
                        throw new DataProcessingException("You have to choose top node path")
                    }
                } else {
                    throw new DataProcessingException("Study with ${config.keepSecurityAs} exists. Please, use other studyId.")
                }
            }
        }

        true
    }

}
