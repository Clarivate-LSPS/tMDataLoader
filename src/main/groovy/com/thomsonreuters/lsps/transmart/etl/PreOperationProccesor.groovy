package com.thomsonreuters.lsps.transmart.etl

import java.util.zip.DataFormatException

class PreOperationProccesor extends SubOperationProcessor {

    public PreOperationProccesor(Object configObject) {
        super(configObject)
    }

    @Override
    Boolean processing() {
        config.logger.log("Run pre-operation processor")

        if (config.deleteStudyById || config.deleteStudyByPath) {
            if (config.deleteStudyByPath) {
                config.deleteStudyByIdValue = getStudyIdByPath(config.moveStudyOldPath)
            }
            if (existSecurityConfiguration(config.deleteStudyByIdValue)) {
                def browseLink = sql.firstRow("select count(fm.folder_id) as cnt from fmapp.fm_folder_association fm, biomart.bio_data_uid bdu where fm.object_uid = bdu.unique_id and bdu.bio_data_id = ?",
                        getBioExperimentIdByAccession(config.deleteStudyByIdValue))
                if (browseLink.cnt > 0 && !config.deleteSecurity) {
                    throw new DataProcessingException("Tab \"Browse\" contain link to this study. You have to use tMDataLoader option --delete-security for delete this study.")
                }
            }
        }

        if (config.moveStudy) {
            //check new path exists, if don't use --replace-study
            if (!config.replaceStudy) {
                config.moveStudyNewPath = config.moveStudyNewPath.trim()
                config.moveStudyNewPath = ('\\' + config.moveStudyNewPath + '\\').replace('\\\\', '\\')
                def chNewPath = sql.firstRow("SELECT count(*) AS cnt FROM i2b2metadata.i2b2 WHERE c_fullname = ?", config.moveStudyNewPath)
                if (chNewPath.cnt > 0) {
                    throw new DataFormatException("Study target path is already exists")
                }
            } else {
                //check security configuration if use --replace-study
                def oldSecureToken = getSecurityTokenByPath(config.moveStudyOldPath)
                def newSecureToken = getSecurityTokenByPath(config.moveStudyNewPath)

                def delConfig = config.clone()
                delConfig.moveStudy = false
                delConfig.moveStudyOldPath = ''
                delConfig.moveStudyNewPath = ''
                delConfig.deleteStudyByPathValue = config.moveStudyNewPath
                delConfig.deleteStudyByIdValue = getStudyIdByPath(config.moveStudyNewPath)

                if (oldSecureToken != PUBLIC_TOKEN && newSecureToken == PUBLIC_TOKEN) {
                    DeleteDataProcessor deleteDataProcessor = new DeleteDataProcessor(delConfig)
                    deleteDataProcessor.process(id: getStudyIdByPath(config.moveStudyNewPath), path: config.moveStudyNewPath)

                    deleteOldStudyConfiguration(getStudyIdByPath(config.moveStudyOldPath))
                    if (!config.keepSecurityAs || !config.keepSecurity) {
                        updateSecurityToken(PUBLIC_TOKEN, config.moveStudyOldPath)
                    }
                } else if (oldSecureToken == PUBLIC_TOKEN && newSecureToken !=PUBLIC_TOKEN) {
                    DeleteDataProcessor deleteDataProcessor = new DeleteDataProcessor(delConfig)
                    deleteDataProcessor.process(id: getStudyIdByPath(config.moveStudyNewPath), path: config.moveStudyNewPath)

                    updateSecurityToken(newSecureToken, config.moveStudyOldPath)
                } else if (oldSecureToken != PUBLIC_TOKEN && newSecureToken != PUBLIC_TOKEN){
                    DeleteDataProcessor deleteDataProcessor = new DeleteDataProcessor(delConfig)
                    deleteDataProcessor.process(id: getStudyIdByPath(config.moveStudyNewPath), path: config.moveStudyNewPath, ds: 'Y')

                    updateSecurityToken(newSecureToken, config.moveStudyOldPath)
                }

            }

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

        config.logger.log("Finish pre-operation processor")
        true
    }

}
