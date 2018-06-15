package com.thomsonreuters.lsps.transmart.etl

class OperationProcessor {

    def config
    def dataOperationProcessor

    OperationProcessor(conf) {
        config = conf
    }

    boolean process() {
        def res = false;
        //Delete data study
        if ((config?.deleteStudyById) || (config?.deleteStudyByPath)) {
            dataOperationProcessor = new DeleteDataProcessor(config)
        } else if (config?.isDeleteTree) {
            dataOperationProcessor = new DeleteCrossProcessor(config)
        } else if (config?.moveStudy) {
            dataOperationProcessor = new MoveStudyProcessor(config)
        }

        try {
            def data = dataOperationProcessor.processData()
            res = dataOperationProcessor.process(data)
        }
        catch (Exception e) {
            config.logger.log(LogType.ERROR, "Exception: ${e}")
        }
        return res;
    }
}
