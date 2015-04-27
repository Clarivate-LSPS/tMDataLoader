package com.thomsonreuters.lsps.transmart.fixtures

import com.thomsonreuters.lsps.transmart.etl.DeleteDataProcessor

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

    Study withData(AbstractData data) {
        this.dataList.add(data)
        return this
    }
}
