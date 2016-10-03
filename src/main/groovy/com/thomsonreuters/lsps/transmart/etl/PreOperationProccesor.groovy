package com.thomsonreuters.lsps.transmart.etl

class PreOperationProccesor extends SubOperationProcessor {

    public PreOperationProccesor(Object configObject) {
        super(configObject)
    }

    @Override
    Boolean processing() {
        config.logger.log("Run pre-operation processor")

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
