/*************************************************************************
 * tranSMART Data Loader - ETL tool for tranSMART
 *
 * Copyright 2012-2013 Thomson Reuters
 *
 * This product includes software developed at Thomson Reuters
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License 
 * as published by the Free Software  
 * Foundation, either version 3 of the License, or (at your option) any later version, along with the following terms:
 * 1.	You may convey a work based on this program in accordance with section 5, provided that you retain the above notices.
 * 2.	You may convey verbatim copies of this program code as you receive it, in any medium, provided that you retain the above notices.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS    * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *
 ******************************************************************/

package com.thomsonreuters.lsps.transmart.etl

import groovy.io.FileType

import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path

class DirectoryProcessor {
    def config

    DirectoryProcessor(conf) {
        config = conf
    }

    boolean process(Path dir, String root = "") {
        config.logger.log("Processing directory: ${dir}")

        try {
            List<Path> studies = []
            List<Path> nestedDirs = []
            List<Path> metaDataDirs = []
            dir.eachDir { Path directory ->
                String name = directory.fileName.toString()
                if (name ==~ /^(\.|_DONE_|_FAIL_|_DISABLED_).*/) {
                    return
                }

                // FIXME: Do we really need it on this level?
                if (name ==~ /(?i)_MetaData/) {
                    metaDataDirs << directory
                } else if (checkIfStudyPath(directory)) {
                    studies << directory
                } else {
                    nestedDirs << directory
                }
            }
            dir.eachFileMatch(FileType.FILES, ~/.+\.zip$/) { Path it ->
                if (checkIfStudyPath(it)) {
                    studies.add(it)
                }
            }

            if (!studies.empty) {
                if (!processStudies(studies, root) && config.stopOnFail)
                    throw new Exception("Processing failed")
            }
            nestedDirs.each {
                process(it, "$root\\${it.fileName}")
            }

            // looping through MetaData nodes (well, only one)
            metaDataDirs.each {
                config.logger.log("=== PROCESSING METADATA FOLDER ===")
                if (!processMetaData(it) && config.stopOnFail) {
                    throw new Exception("Processing failed")
                }
                config.logger.log("=== FINISHED PROCESSING METADATA FOLDER ===")
            }

            return true

        } catch (Exception ignored) {
            return false
        }
    }

    boolean process(dir, String root = "") {
        process(dir.asType(File).toPath(), root)
    }

    private boolean checkIfStudyPath(Path file) {
        boolean result = false

        if (isZipFile(file)) {
            return FileSystems.newFileSystem(file, null).withCloseable { zipFileSystem ->
                Path path = ((FileSystem) zipFileSystem).getPath(file.fileName.toString().replace('.zip', ''))
                return checkIfStudyPath(path)
            }
        }

        if (Files.isDirectory(file)) {
            file.eachDirMatch(~/[^\._].+/) { Path childFile ->
                if (childFile.fileName.toString() ==~
                        /^(?i)(${DataProcessorFactory.processorTypes.join('|')})Data(ToUpload)?\b.*/) {
                    result = true
                }
            }
        }

        return result
    }

    private boolean isZipFile(Path file) {
        return file.fileName.toString().endsWith('.zip');
    }

    private boolean processMetaData(Path dir) {
        def res = false
        config.logger.log("Processing metadata dir ${dir.fileName}");

        def metadataProcessor = new MetaDataProcessor(config)

        try {
            res = metadataProcessor.process(dir, [:])
        } catch (Exception e) {
            config.logger.log(LogType.ERROR, "Exception: ${e}")
        }

        if (res) {
            Files.move(dir, dir.resolveSibling("_DONE_${dir.fileName}"))
        } else {
            if (!config.isNoRenameOnFail)
                Files.move(dir, dir.resolveSibling("_FAIL_${dir.fileName}"))
        }

        res
    }

    private boolean processStudies(List<Path> studies, String parentNode) {
        config.logger.log("=== PROCESSING STUDIES IN '${parentNode}' ===")
        def isAllSuccessful = true
        // looping through studies
        for (def studyPath : studies) {
            // dir name is the study

            config.logger.log "== Found study: ${studyPath.fileName} =="
            StudyProcessor studyProcessor
            if (isZipFile(studyPath)) {
                studyProcessor = new ZipStudyProcessor(config)
            } else {
                studyProcessor = new StudyProcessor(config)
            }
            boolean studyProcessSuccessful = studyProcessor.processStudy(studyPath, parentNode)
            isAllSuccessful = isAllSuccessful && studyProcessSuccessful
            if (!isAllSuccessful && config.stopOnFail) {
                break
            }
        }

        config.logger.log("=== FINISHED PROCESSING ${parentNode} ===")
        return isAllSuccessful
    }
}
