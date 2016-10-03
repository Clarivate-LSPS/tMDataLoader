package com.thomsonreuters.lsps.transmart.etl

class PostOperationProcessor extends SubOperationProcessor{

    public PostOperationProcessor(Object conf) {
        super(conf)
    }

    @Override
    Boolean processing() {
        config.logger.log("Run post-operation processor")

        if (config.moveStudy) {
            if (config.useSecurityFrom) {
                String studyId = getStudyIdByPath(config.moveStudyNewPath)

                if (existSecurityConfiguration(config.useSecurityFrom)) {
                    deleteOldStudyConfiguration(studyId)
                    updateStudyId(studyId, config.useSecurityFrom)
                } else {
                    throw new DataProcessingException("Not found configuration for ${config.useSecurityFrom}")
                }
            }
        }

        true
    }
}
