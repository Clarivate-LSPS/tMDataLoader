package com.thomsonreuters.lsps.transmart.fixtures

/**
 * Date: 03.11.2015
 * Time: 13:52
 */
class ClinicalDataBuilder {
    StudyInfo studyInfo
    File dir

    ClinicalDataBuilder(File dir, StudyInfo studyInfo) {
        this.dir = dir
        this.studyInfo = studyInfo
    }

    void dataFile(String fileName, List<String> columns, @DelegatesTo(DataFileBuilder) Closure block) {
        new File(dir, fileName).withPrintWriter { writer ->
            DataFileBuilder.build(studyInfo.id, columns, block).writeTo(writer)
        }
    }

    void mappingFile(@DelegatesTo(MappingFileBuilder) Closure block) {
        new File(dir, "${studyInfo.name}_${studyInfo.id}_Subject_Sample_Mapping_File.txt").withPrintWriter { writer ->
            MappingFileBuilder.build(block).writeTo(writer)
        }
    }
}
