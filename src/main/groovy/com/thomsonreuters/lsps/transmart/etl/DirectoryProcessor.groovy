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

import java.nio.file.DirectoryStream
import java.nio.file.FileSystem
import java.nio.file.FileSystems
import java.nio.file.Files
import java.nio.file.Path

class DirectoryProcessor {
    def config

    DirectoryProcessor(conf) {
        config = conf
    }

    boolean process(dir, String root = "") {
        def d = dir as File
        config.logger.log("Processing directory: ${dir}")

        try {

            Files.newDirectoryStream(d.toPath(), new DirectoryStream.Filter<Path> () {
                @Override
                boolean accept(Path entry) throws IOException {
                    return entry.getFileName().toString() ==~ /[^\._].+/
                }

            }).withCloseable { directories ->
                for (Path directory : directories) {
                    def node = root + "\\${directory.fileName}"
                    List studies = getStudies(directory)

                    if (studies.empty) {
                        process(directory.toFile(), node)
                        continue
                    }

                    if (!processStudies(studies, node) && config.stopOnFail)
                        throw new Exception("Processing failed")

                }
            }

            // looping through MetaData nodes (well, only one)
            d.eachDirMatch(~/(?i)_MetaData/) {
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

    private List getStudies(Path dir) {
        List result = []
        dir.eachFileMatch(FileType.ANY,~/[^\._].+/) {
            if (checkIfStudyFolder(it)) {
                result.add(it)
            }
        }

        result
    }

    private boolean checkIfStudyFolder(Path file) {
        boolean result = false

        if (isZipFile(file)) {
            FileSystems.newFileSystem(file, null).withCloseable { FileSystem zipFileSystem ->
                Path path = zipFileSystem.getPath(file.fileName.toString().replace('.zip', ''))
                path.eachDirMatch(~/[^\._].+/) { Path childFile ->
                    if (childFile.fileName.toString() ==~
                            /^(?i)(${DataProcessorFactory.processorsType.join('|')})Data(ToUpload)?\b.*/) {
                        result = true
                    }
                }
            }
        }

        if ( Files.isDirectory(file) ) {
            file.eachDirMatch(~/[^\._].+/) { Path childFile ->
                if (childFile.fileName.toString() ==~
                        /^(?i)(${DataProcessorFactory.processorsType.join('|')})Data(ToUpload)?\b.*/) {
                    result = true
                }
            }
        }

        return result
    }

    private boolean isZipFile(Path file) {
        return file.fileName.toString().endsWith('.zip');
    }

    private boolean processMetaData(File dir) {
        def res = false
        config.logger.log("Processing metadata dir ${dir.name}");

        def metadataProcessor = new MetaDataProcessor(config)

        try {
            res = metadataProcessor.process(dir, [:])
        } catch (Exception e) {
            config.logger.log(LogType.ERROR, "Exception: ${e}")
        }

        if (res) {
            dir.renameTo(new File(dir, "_DONE_${dir.name}"))
        } else {
            if (!config.isNoRenameOnFail)
                dir.renameTo(new File(dir, "_FAIL_${dir.name}"))
        }

        res
    }

    private boolean processStudies(List studies, String parentNode) {
        config.logger.log("=== PROCESSING STUDIES IN ${parentNode} ===")
        def isAllSuccessful = true
        studies.each { Path studyPath ->
            // looping through studies
            // dir name is the study

            String studyName = studyPath.fileName.toString()
            config.logger.log "== Found study: ${studyName} =="
            StudyProcessor studyProcessor =
                    isZipFile(studyPath) ? new ZipStudyProcessor(config) : new StudyProcessor(config)

            boolean studyProcessSuccessful = studyProcessor.
                    processStudy(studyPath, studyPath.parent.fileName.toString())
            isAllSuccessful = isAllSuccessful && studyProcessSuccessful

        }

        config.logger.log("=== FINISHED PROCESSING ${parentNode} ===")
        return isAllSuccessful
    }


}
