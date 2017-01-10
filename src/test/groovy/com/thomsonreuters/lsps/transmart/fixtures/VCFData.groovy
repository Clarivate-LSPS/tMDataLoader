package com.thomsonreuters.lsps.transmart.fixtures;

import com.thomsonreuters.lsps.transmart.etl.AbstractDataProcessor
import com.thomsonreuters.lsps.transmart.etl.VCFDataProcessor;

/**
 * Date: 26.06.2015
 * Time: 13:57
 */
public class VCFData extends AbstractData<VCFData> {
    final String dataType = "VCDData"

    @Override
    protected AbstractDataProcessor newDataProcessor(config) {
        return new VCFDataProcessor(config)
    }

    @Override
    protected void adaptFiles(StudyInfo oldStudyInfo) {
        def subjectSampleMapping = new File(dir, 'Subject_Sample_Mapping_File.txt')
        def oldSubjectSampleMapping = new File(dir, 'Subject_Sample_Mapping_File.txt.old')
        subjectSampleMapping.renameTo(oldSubjectSampleMapping)
        def newSubjectSampleMapping = new File(dir, 'Subject_Sample_Mapping_File.txt.new')
        newSubjectSampleMapping.withWriter { out ->
            oldSubjectSampleMapping.eachLine {
                if (it =~ /#\s*STUDY_ID\s*:/) {
                    out.println("# STUDY_ID: ${studyId}")
                } else {
                    out.println(it)
                }
            }
        }
        newSubjectSampleMapping.renameTo(subjectSampleMapping)
    }
}
