package com.thomsonreuters.lsps.transmart.etl

import java.nio.file.DirectoryStream
import java.nio.file.Files
import java.nio.file.Path

class StudyProcessor {

    def config

    StudyProcessor(config) {
        this.config = config
    }

    boolean processStudy(Path studyPath, String parentDir) {
        if (!studyPath || Files.notExists(studyPath))
            throw new IllegalArgumentException('Study path incorrect')

        boolean isStudyUploadSuccessful = true
        String studyName = studyPath.fileName.toString()
        DataProcessorFactory.processorsType.each { processorType ->
            def studyInfo = ['name': studyName, 'node': "${parentDir}\\${studyName}".toString()]
            if (!processDataDirectory(studyPath, processorType, studyInfo)) {
                isStudyUploadSuccessful = false
            }
        }

        if (isStudyUploadSuccessful) {
            markDir(studyPath, '_DONE_')
        } else {
            if (!config.isNoRenameOnFail)
                markDir(studyPath, '_FAIL_')
        }

        isStudyUploadSuccessful
    }

    protected boolean processDataDirectory(Path studyDir, String dataType, studyInfo) {
        def res = true

        Files.newDirectoryStream(studyDir, new DirectoryStream.Filter<Path> () {
            @Override
            boolean accept(Path entry) throws IOException {
                return Files.isDirectory(entry) &&
                        entry.getFileName().toString() ==~ /^(?:${dataType}|${dataType.toLowerCase().capitalize()})Data(?:ToUpload)?\b.*/
            }

        }).withCloseable { dataDirs ->
            for (Path dataDir : dataDirs) {
                config.logger.log "Processing ${dataType} data"
                def dataProcessor = DataProcessorFactory.newDataProcessor(dataType, config)
                try {
                    studyInfo['base_datatype'] = dataType
                    res = res && dataProcessor.process(dataDir, studyInfo)
                }
                catch (Exception e) {
                    res = false
                    config.logger.log(LogType.ERROR, e)
                }

                if (res) {
                    markDir(dataDir, '_DONE_')
                } else {
                    if (!config.isNoRenameOnFail)
                        markDir(dataDir, '_FAIL_')

                    if (config.stopOnFail)
                        break
                }
            }
        }

        res
    }

    protected Path markDir(Path dir, String mark) {
        return Files.move(dir, dir.resolveSibling("${mark}${dir.fileName}"))
    }


}
