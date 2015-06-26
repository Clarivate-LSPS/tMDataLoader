package com.thomsonreuters.lsps.transmart.fixtures

import com.thomsonreuters.lsps.transmart.etl.DataProcessor
import com.thomsonreuters.lsps.transmart.util.TempStorage

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
    protected abstract DataProcessor newDataProcessor(config)
    protected abstract void adaptFiles(StudyInfo oldStudyInfo)

    boolean load(config, parentNode = "Test Studies\\") {
        if (!parentNode.endsWith('\\')) {
            parentNode += '\\'
        }
        newDataProcessor(config).process(dir, [name: studyName, node: "$parentNode$studyName" as String])
    }

    public T copyWithSuffix(String suffix) {
        return copyAttachedToStudy(studyInfo.withSuffix(suffix))
    }

    /**
     * Creates a copy of data, attached to specified study
     *
     * @param studyInfo study information
     * @return copy of expression data attached to study
     */
    T copyAttachedToStudy(StudyInfo studyInfo) {
        def newDir = TempStorage.instance.createSingletonTempDirectoryFrom(dir,
                "${studyInfo.name}_${studyInfo.id}_${dataType}") { dir ->
            this.class.newInstance(dir: dir, studyInfo: studyInfo).adaptFiles(this.studyInfo)
        }
        return (T) this.class.newInstance(dir: newDir, studyInfo: studyInfo)
    }
}