package com.thomsonreuters.lsps.transmart.fixtures
import com.thomsonreuters.lsps.transmart.TdfUtils
import com.thomsonreuters.lsps.transmart.etl.DataProcessor
import com.thomsonreuters.lsps.transmart.etl.ProteinDataProcessor

import static com.thomsonreuters.lsps.transmart.fixtures.FileAdaptUtils.adaptFile
/**
 * Date: 26.06.2015
 * Time: 13:57
 */
public class ProteinData extends AbstractData<ProteinData> {
    final String dataType = "ProteinData";

    @Override
    protected DataProcessor newDataProcessor(config) {
        return new ProteinDataProcessor(config);
    }

    @Override
    protected void adaptFiles(StudyInfo oldStudyInfo) {
        adaptFile(dir, /<<STUDY_NAME>>_<<STUDY_ID>>_PROTEIN_Data_(\w)\.txt/, oldStudyInfo, studyInfo)
        adaptFile(dir, /<<STUDY_NAME>>_<<STUDY_ID>>_Subject_Sample_Mapping_File\.txt/, oldStudyInfo, studyInfo) {
            TdfUtils.transformColumnValue(0, it.oldFile as File, it.newFile as File) { _ -> studyInfo.id }
            it.oldFile.delete()
        }
    }
}
