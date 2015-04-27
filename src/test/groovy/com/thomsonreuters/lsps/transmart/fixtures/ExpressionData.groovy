package com.thomsonreuters.lsps.transmart.fixtures

import com.thomsonreuters.lsps.transmart.TdfUtils
import com.thomsonreuters.lsps.transmart.etl.ExpressionDataProcessor
import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter

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
        List<File> files = dir.listFiles()

        def expressionDataFilePattern = /.*_Gene_Expression_Data_(\w)\.txt$/
        def dataType = null
        File expressionDataFile = files.find {
            def m = it.name =~ expressionDataFilePattern
            if (!m) {
                return false
            }
            dataType = m.group(1)
            return true
        }
        File newExpressionDataFile = new File(expressionDataFile.parentFile,
                "${studyInfo.name}_${studyInfo.id}_Gene_Expression_Data_${dataType}.txt")
        expressionDataFile.renameTo(newExpressionDataFile)

        File mappingFile = files.find { it.name ==~ /.*_Subject_Sample_Mapping_File\.txt$/ }
        File newMappingFile = new File(mappingFile.parentFile,
                "${studyInfo.name}_${studyInfo.id}_Subject_Sample_Mapping_File.txt")
        TdfUtils.transformColumnValue(0, mappingFile, newMappingFile) { _ -> studyInfo.id }
        mappingFile.delete()
    }
}
