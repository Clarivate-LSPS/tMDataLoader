package com.thomsonreuters.lsps.transmart.fixtures
import com.thomsonreuters.lsps.transmart.TdfUtils
import com.thomsonreuters.lsps.transmart.etl.ClinicalDataProcessor
import com.thomsonreuters.lsps.transmart.etl.DataProcessor
import com.thomsonreuters.lsps.transmart.etl.mappings.ClinicalDataMapping
/**
 * Date: 27.04.2015
 * Time: 13:59
 */
class ClinicalData extends AbstractData<ClinicalData> {
    final String dataType = 'ClinicalData'

    @Override
    protected DataProcessor newDataProcessor(config) {
        return new ClinicalDataProcessor(config)
    }

    @Override
    protected void adaptFiles(StudyInfo studyInfo) {
        List<File> files = dir.listFiles()

        File mappingFile = files.find { it.name ==~ /.*_Mapping_File\.txt$/ }
        def mapping = ClinicalDataMapping.loadFromFile(mappingFile)
        mapping.eachFileMapping { ClinicalDataMapping.FileMapping fileMapping ->
            TdfUtils.transformColumnValue(0, new File(dir, fileMapping.fileName)) { _ -> studyInfo.id }
        }

        File newMappingFile = new File(mappingFile.parentFile,
                "${studyInfo.name}_${studyInfo.id}_Subject_Sample_Mapping_File.txt")
        mappingFile.renameTo(newMappingFile)
    }
}
