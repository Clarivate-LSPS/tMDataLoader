package com.thomsonreuters.lsps.transmart.fixtures

import com.thomsonreuters.lsps.transmart.TdfUtils
import com.thomsonreuters.lsps.transmart.etl.ExpressionDataProcessor

import static com.thomsonreuters.lsps.transmart.fixtures.FileAdaptUtils.adaptFile
import static com.thomsonreuters.lsps.transmart.fixtures.FileAdaptUtils.getFileMapping

/**
 * Date: 23.04.2015
 * Time: 12:43
 */
class ExpressionData extends AbstractData<ExpressionData> {
    final String dataType = "ExpressionData"

    @Override
    protected ExpressionDataProcessor newDataProcessor(config) {
        return new ExpressionDataProcessor(config)
    }

    @Override
    protected void adaptFiles(StudyInfo oldStudyInfo) {
        adaptFile(dir, /<<STUDY_NAME>>_<<STUDY_ID>>_Gene_Expression_Data_(\w)\.txt/, oldStudyInfo, studyInfo)
        adaptFile(dir, /<<STUDY_NAME>>_<<STUDY_ID>>_Subject_Sample_Mapping_File\.txt/, oldStudyInfo, studyInfo) {
            TdfUtils.transformColumnValue(0, it.oldFile as File, it.newFile as File) { _ -> studyInfo.id }
            it.oldFile.delete()
        }
    }
}
