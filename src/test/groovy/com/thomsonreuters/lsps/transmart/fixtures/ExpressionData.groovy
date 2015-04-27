package com.thomsonreuters.lsps.transmart.fixtures
import com.thomsonreuters.lsps.transmart.etl.ExpressionDataProcessor
import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import com.thomsonreuters.lsps.transmart.util.TempStorage
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter
/**
 * Date: 23.04.2015
 * Time: 12:43
 */
class ExpressionData {
    StudyInfo studyInfo

    String getStudyId() {
        return studyInfo.id
    }

    String getStudyName() {
        return studyInfo.name
    }

    File dir

    void load(config, parentNode = "Test Studies\\") {
        new ExpressionDataProcessor(config).process(dir, [name: studyName, node: "$parentNode$studyName" as String])
    }

    ExpressionData withStudy(String newStudyName, String newStudyId) {
        return fromTemplate(this, newStudyName, newStudyId)
    }

    static ExpressionData fromTemplate(ExpressionData template, String newStudyName, String newStudyId) {
        def newDir = TempStorage.instance.createSingletonTempDirectoryFrom(template.dir,
                "${newStudyName}_${newStudyId}_ExpressionData") { dir ->
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
                    "${newStudyName}_${newStudyId}_Gene_Expression_Data_${dataType}.txt")
            expressionDataFile.renameTo(newExpressionDataFile)

            File mappingFile = files.find { it.name ==~ /.*_Subject_Sample_Mapping_File\.txt$/ }
            File newMappingFile = new File(mappingFile.parentFile,
                    "${newStudyName}_${newStudyId}_Subject_Sample_Mapping_File.txt")
            CsvLikeFile csvMappingFile = new CsvLikeFile(mappingFile)
            newMappingFile.withWriter { expr ->
                def printer = new CSVPrinter(expr, CSVFormat.TDF.withHeader(csvMappingFile.header))
                csvMappingFile.eachEntry { String[] values ->
                    printer.print(newStudyId)
                    for (int i = 1; i < values.length; i++) {
                        printer.print(values[i])
                    }
                    printer.println()
                }
            }
            mappingFile.delete()
        }
        return new ExpressionData(dir: newDir, studyInfo: new StudyInfo(newStudyId, newStudyName))
    }
}
