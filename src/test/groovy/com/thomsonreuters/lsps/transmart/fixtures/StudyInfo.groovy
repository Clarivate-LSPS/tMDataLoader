package com.thomsonreuters.lsps.transmart.fixtures

/**
 * Date: 27.04.2015
 * Time: 13:03
 */
class StudyInfo {
    String id
    String name

    StudyInfo(String id, String name) {
        this.id = id
        this.name = name
    }

    StudyInfo withSuffix(String suffix) {
        return new StudyInfo("${id}${suffix.toUpperCase()}", "${name} ${suffix}")
    }

    StudyInfo withSuffixCS(String studyIdSuffix, String pathSuffix) {
        return new StudyInfo("${id}${studyIdSuffix}", "${name} ${pathSuffix}")
    }

    StudyInfo withName(String studyName) {
        return new StudyInfo(id, studyName)
    }

    StudyInfo withSuffixForId(String suffix){
        return new StudyInfo("${id}${suffix.toUpperCase()}", "${name}")
    }
}
