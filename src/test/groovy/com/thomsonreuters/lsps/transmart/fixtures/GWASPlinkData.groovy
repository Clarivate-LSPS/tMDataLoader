package com.thomsonreuters.lsps.transmart.fixtures

import com.thomsonreuters.lsps.transmart.etl.AbstractDataProcessor
import com.thomsonreuters.lsps.transmart.etl.GWASPlinkDataProcessor
import sun.reflect.generics.reflectiveObjects.NotImplementedException

/**
 * Date: 26-Jul-16
 * Time: 17:36
 */
class GWASPlinkData extends AbstractData<GWASPlinkData> {
    final String dataType = 'GWASPlinkData'

    @Override
    protected AbstractDataProcessor newDataProcessor(Object config) {
        return new GWASPlinkDataProcessor(config)
    }

    @Override
    protected void adaptFiles(StudyInfo oldStudyInfo) {
        throw new NotImplementedException()
    }
}
