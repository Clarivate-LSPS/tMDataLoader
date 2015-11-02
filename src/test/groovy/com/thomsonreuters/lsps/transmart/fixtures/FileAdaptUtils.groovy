package com.thomsonreuters.lsps.transmart.fixtures

import java.util.regex.Matcher
import java.util.regex.Pattern

/**
 * Date: 04.08.2015
 * Time: 13:29
 */
class FileAdaptUtils {
    static
    def adaptFile(File dir, String fileNamePattern, StudyInfo oldStudyInfo, StudyInfo studyInfo, Closure adaptWith) {
        def mapping = getFileMapping(dir, fileNamePattern, oldStudyInfo, studyInfo)
        adaptWith(mapping)
    }

    static def adaptFile(File dir, String fileNamePattern, StudyInfo oldStudyInfo, StudyInfo studyInfo) {
        adaptFile(dir, fileNamePattern, oldStudyInfo, studyInfo) { mapping ->
            mapping.oldFile.renameTo(mapping.newFile)
        }
    }

    @SuppressWarnings("GroovyAssignabilityCheck")
    private static Integer getNamedGroupIndex(Pattern pattern, String name) {
        pattern.namedGroups().get(name)
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
                    "(?<studyId>\\\\Q${oldStudyInfo.id}\\\\E)")
        }
        if (hasName) {
            fileNamePattern = fileNamePattern.replaceFirst('<<STUDY_NAME>>',
                    "(?<studyName>\\\\Q${oldStudyInfo.name}\\\\E)")
        }
        Pattern pattern = Pattern.compile(fileNamePattern)
        Matcher matcher = null
        File oldFile = dir.listFiles().find {
            matcher = pattern.matcher(it.name)
            return matcher.matches()
        }
        String newFileName = oldFile.name
        TreeMap<String, String> replacements = new TreeMap<>(new Comparator<String>() {
            @Override
            int compare(String group1, String group2) {
                return -(matcher.start(getNamedGroupIndex(pattern, group1)) -
                        matcher.start(getNamedGroupIndex(pattern, group2)))
            }
        })
        if (hasId) {
            replacements.put('studyId', studyInfo.id)
        }
        if (hasName) {
            replacements.put('studyName', studyInfo.name)
        }
        for (def replacement : replacements.entrySet()) {
            def group = getNamedGroupIndex(pattern, replacement.key)
            newFileName = newFileName.substring(0, matcher.start(group)) + studyInfo.name +
                    newFileName.substring(matcher.end(group))
        }
        return [oldFile: oldFile, newFile: new File(oldFile.parentFile, newFileName)]
    }
}
