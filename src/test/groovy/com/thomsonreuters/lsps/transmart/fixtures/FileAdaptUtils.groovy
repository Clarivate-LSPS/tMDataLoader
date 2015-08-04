package com.thomsonreuters.lsps.transmart.fixtures

import java.util.regex.Matcher
import java.util.regex.Pattern
/**
 * Date: 04.08.2015
 * Time: 13:29
 */
class FileAdaptUtils {
    static def adaptFile(File dir, String fileNamePattern, StudyInfo oldStudyInfo, StudyInfo studyInfo, Closure adaptWith) {
        def mapping = getFileMapping(dir, fileNamePattern, oldStudyInfo, studyInfo)
        adaptWith(mapping)
    }

    static def adaptFile(File dir, String fileNamePattern, StudyInfo oldStudyInfo, StudyInfo studyInfo) {
        adaptFile(dir, fileNamePattern, oldStudyInfo, studyInfo) { mapping ->
            mapping.oldFile.renameTo(mapping.newFile)
        }
    }

    /**
     * Get file mapping from old study to new study
     *
     * @param dir data directory
     * @param fileNamePattern file name pattern with possible <<STUDY_ID>> and <<STUDY_NAME>> entries (can occur only once)
     * @param oldStudyInfo old study info
     * @param studyInfo new study info
     * @return
     */
    static def getFileMapping(File dir, String fileNamePattern, StudyInfo oldStudyInfo, StudyInfo studyInfo) {
        boolean hasId = fileNamePattern.contains('<<STUDY_ID>>')
        boolean hasName = fileNamePattern.contains('<<STUDY_NAME>>')
        if (!(hasId || hasName)) {
            return;
        }
        if (hasId) {
            fileNamePattern = fileNamePattern.replaceFirst('<<STUDY_ID>>',
                    "(?P<studyId>${Pattern.quote(oldStudyInfo.id)})")
        }
        if (hasName) {
            fileNamePattern = fileNamePattern.replaceFirst('<<STUDY_NAME>>',
                    "(?P<studyName>${Pattern.quote(oldStudyInfo.name)})")
        }
        Pattern pattern = Pattern.compile(fileNamePattern)
        Matcher matcher = null
        File oldFile = dir.listFiles().find {
            matcher = pattern.matcher(it.name)
            return matcher.matches()
        }
        String newFileName = oldFile.name
        if (hasId) {
            newFileName = newFileName.substring(0, matcher.start('studyId')) + studyInfo.id +
                    newFileName.substring(matcher.end('studyId'))
        }
        if (hasName) {
            newFileName = newFileName.substring(0, matcher.start('studyName')) + studyInfo.name +
                    newFileName.substring(matcher.end('studyName'))
        }
        return [oldFile: oldFile, newFile: new File(oldFile.parentFile, newFileName)]
    }
}
