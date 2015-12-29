package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.transmart.util.PathUtils

import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path

class ZipStudyProcessor extends StudyProcessor {

    ZipStudyProcessor(config) {
        super(config)
    }

    @Override
    boolean processStudy(Path zipStudyPath, String parentDir) {
        if (!zipStudyPath || Files.notExists(zipStudyPath))
            throw new IllegalArgumentException('Zip study path incorrect')

        boolean isStudyUploadSuccessful = true

        FileSystems.newFileSystem(zipStudyPath, null).withCloseable { FileSystem zipFileSystem ->
            Path studyPath = zipFileSystem.getPath(zipStudyPath.fileName.toString().replace('.zip', ''))
            isStudyUploadSuccessful = super.processStudy(studyPath, parentDir)
        }

        if (isStudyUploadSuccessful) {
            Files.move(zipStudyPath, zipStudyPath.resolveSibling("_DONE_${zipStudyPath.fileName}"))
        } else {
            if (!config.isNoRenameOnFail)
                Files.move(zipStudyPath, zipStudyPath.resolveSibling("_FAIL_${zipStudyPath.fileName}"))

        }

        isStudyUploadSuccessful
    }

    @Override
    protected Path markDir(Path dir, String mark) {
        Path markingPath = dir.resolveSibling("${mark}${dir.fileName}")
        Files.createDirectory(markingPath)

        PathUtils.movePath(dir, markingPath)
        PathUtils.deletePath(dir)

        markingPath
    }
}
