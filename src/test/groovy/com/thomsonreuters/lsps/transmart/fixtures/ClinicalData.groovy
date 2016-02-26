package com.thomsonreuters.lsps.transmart.fixtures

import com.thomsonreuters.lsps.transmart.TdfUtils
import com.thomsonreuters.lsps.transmart.etl.ClinicalDataProcessor
import com.thomsonreuters.lsps.transmart.etl.DataProcessor
import com.thomsonreuters.lsps.transmart.etl.mappings.ClinicalDataMapping
import com.thomsonreuters.lsps.io.file.TempStorage

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

    static build(String studyId, String studyName, @DelegatesTo(ClinicalDataBuilder) Closure closure) {
        def studyInfo = new StudyInfo(studyId, studyName)
        def dir = TempStorage.instance.createSingletonTempDirectory(
                "${studyName}_${studyId}_ClinicalData_${UUID.randomUUID()}") { dir ->
            ClinicalDataBuilder builder = new ClinicalDataBuilder(dir, studyInfo)
            closure.delegate = builder
            closure.call()
        }
        return new ClinicalData(dir: dir, studyInfo: studyInfo)
    }

    @Override
    protected void adaptFiles(StudyInfo oldStudyInfo) {
        List<File> files = dir.listFiles()

        File mappingFile = files.find { it.name ==~ /.*_Mapping_File\.txt$/ }
        def mapping = ClinicalDataMapping.loadFromFile(mappingFile.toPath())
        mapping.eachFileMapping { ClinicalDataMapping.FileMapping fileMapping ->
            TdfUtils.transformColumnValue(0, new File(dir, fileMapping.fileName)) { _ -> studyId }
        }

        File newMappingFile = new File(mappingFile.parentFile,
                "${studyName}_${studyId}_Subject_Sample_Mapping_File.txt")
        mappingFile.renameTo(newMappingFile)
    }
}

