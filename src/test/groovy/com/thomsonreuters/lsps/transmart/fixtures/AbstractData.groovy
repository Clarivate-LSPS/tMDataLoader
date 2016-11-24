package com.thomsonreuters.lsps.transmart.fixtures

import com.thomsonreuters.lsps.transmart.etl.AbstractDataProcessor
import com.thomsonreuters.lsps.io.file.TempStorage

/**
 * Date: 27.04.2015
 * Time: 12:57
 */
abstract class AbstractData<T extends AbstractData> {
    StudyInfo studyInfo
    File dir

    String getStudyId() {
        return studyInfo.id
    }

    String getStudyName() {
        return studyInfo.name
    }

    abstract String getDataType()
    protected abstract AbstractDataProcessor newDataProcessor(config)
    protected abstract void adaptFiles(StudyInfo oldStudyInfo)

    boolean reload(config, String parentNode = "Test Studies\\") {
        Study.deleteById(config, studyId)
        load(config, parentNode)
    }

    boolean load(config, String parentNode = "Test Studies\\") {
        if (!parentNode.endsWith('\\')) {
            parentNode += '\\'
        }
        loadByPath(config, "$parentNode$studyName")
    }

    boolean loadByPath(config, String path = "Test Studies\\Test study\\"){
        newDataProcessor(config).process(dir.toPath(), [name: studyName, node: path])
    }

    T copyWithSuffix(String suffix) {
        return copyAttachedToStudy(studyInfo.withSuffix(suffix))
    }

    /**
     * Creates a copy of data, attached to specified study
     *
     * @param studyInfo study information
     * @return copy of expression data attached to study
     */
    T copyAttachedToStudy(StudyInfo studyInfo, String copyName = null) {
        def newDir = TempStorage.instance.createSingletonTempDirectoryFrom(dir,
                copyName ?: "${studyInfo.name}_${studyInfo.id}_${dataType}") { dir ->
            if (studyInfo.id != this.studyInfo.id || studyInfo.name != this.studyInfo.name) {
                this.class.newInstance(dir: dir, studyInfo: studyInfo).adaptFiles(this.studyInfo)
            }
        }
        return (T) this.class.newInstance(dir: newDir, studyInfo: studyInfo)
    }
}