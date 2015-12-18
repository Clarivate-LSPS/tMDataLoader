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

import com.thomsonreuters.lsps.transmart.util.PathUtils
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

            // looping through top nodes
            d.eachDirMatch(~/[^\._].+/) {
                if (!checkIfStudyFolder(it)) { // break the recursion at study level
                    def node = root + "\\${it.name}"

                    if (checkIfHasStudies(it)) {
                        if (!processStudies(it, node) && config.stopOnFail) {
                            throw new Exception("Processing failed")
                        }

                        // just in case there are any nested folders there
                        process(it, node)
                    } else {
                        // nested directory
                        process(it, node)
                    }
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
        }
        catch (Exception ignored) {
            return false
        }
    }

    private boolean checkIfHasStudies(dir) {
        def res = false
        // looping through the files, looking for study folders
        dir.eachFileMatch(FileType.ANY,~/[^\._].+/) {
            if (checkIfStudyFolder(it)) {
                res = true // can't break out of closure easily other than using exception
            }
        }

        return res
    }

    private boolean checkIfStudyFolder(File file) {
        boolean result = false

        if (isZipFile(file)) {
            FileSystems.newFileSystem(file.toPath(), null).withCloseable { FileSystem zipFileSystem ->
                Path path = zipFileSystem.getPath(file.name.replace('.zip', ''))
                path.eachDirMatch(~/[^\._].+/) { Path childFile ->
                    if (childFile.fileName.toString() ==~
                            /^(?i)(${DataProcessorFactory.processorsType.join('|')})Data(ToUpload)?\b.*/) {
                        result = true
                    }
                }
            }

            return result
        }

        if ( file.isDirectory() ) {
            file.eachDirMatch(~/[^\._].+/) {
                if (it.name ==~ /^(?i)(${DataProcessorFactory.processorsType.join('|')})Data(ToUpload)?\b.*/) {
                    result = true
                }
            }

            return result
        }


        return result
    }

    private boolean isZipFile(File file) {
        return file.name.endsWith('.zip');
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


    private boolean processStudies(File studiesParentDir, String parentNode) {
        config.logger.log("=== PROCESSING STUDIES IN ${parentNode} ===")
        def isAllSuccessful = true
        studiesParentDir.eachFileMatch(FileType.ANY,~/(?!\.|_DONE_|_FAIL_|_DISABLED_).+/) {
            // looping through studies
            // dir name is the study

            def studyName = it.name
            config.logger.log "== Found study: ${studyName} =="

            try {
            boolean studyProcessSuccessful = isZipFile(it) ? processZipStudy(it.toPath()) : processFolderStudy(it.toPath());
            isAllSuccessful = isAllSuccessful && studyProcessSuccessful
            } catch (Exception e) {
                e.printStackTrace()
            }
        }

        config.logger.log("=== FINISHED PROCESSING ${parentNode} ===")
        return isAllSuccessful
    }

    private boolean processFolderStudy(Path path) {
        boolean isStudyUploadSuccessful = true
        String studyName = path.fileName.toString()

        DataProcessorFactory.processorsType.each { processorType ->
            def studyInfo = ['name': studyName, 'node': "${path.getParent().fileName}\\${studyName}".toString()]
            if (!processDataDirectory(path, processorType, studyInfo)) {
                isStudyUploadSuccessful = false
            }
        }

        if (isStudyUploadSuccessful) {
            Files.move(path, path.resolveSibling("_DONE_${path.fileName}"))
        } else {
            if (!config.isNoRenameOnFail)
                Files.move(path, path.resolveSibling("_FAIL_${path.fileName}"))
        }

        isStudyUploadSuccessful
    }

    private boolean processZipStudy(Path path) {
        boolean isStudyUploadSuccessful = true

        FileSystems.newFileSystem(path, null).withCloseable { FileSystem zipFileSystem ->
            Path ziPath = zipFileSystem.getPath(path.fileName.toString().replace('.zip', ''))
            String studyName = ziPath.getFileName().toString()

            DataProcessorFactory.processorsType.each { processorType ->
                def studyInfo = ['name': studyName,
                                 'node': "${path.parent.fileName}\\${studyName}".toString(),
                                 'isZip': true]
                if (!processDataDirectory(ziPath, processorType, studyInfo)) {
                    isStudyUploadSuccessful = false
                }
            }

            //rename zip entry
            if (isStudyUploadSuccessful) {
                markZipPath(ziPath, '_DONE_')
            } else {
                if (!config.isNoRenameOnFail)
                    markZipPath(ziPath, '_FAIL_')

            }
        }

        //rename zip file
        if (isStudyUploadSuccessful) {
            Files.move(path, path.resolveSibling("_DONE_${path.fileName}"))
        } else {
            if (!config.isNoRenameOnFail)
                Files.move(path, path.resolveSibling("_FAIL_${path.fileName}"))

        }

        isStudyUploadSuccessful
    }

    private boolean processDataDirectory(Path parentDir, String dataType, studyInfo) {
        def res = true

        Files.newDirectoryStream(parentDir, new DirectoryStream.Filter<Path> () {
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
                    studyInfo.isZip ? markZipPath(dataDir, '_DONE_') :
                            Files.move(dataDir, dataDir.resolveSibling("_DONE_${dataDir.fileName}"))
                } else {
                    if (!config.isNoRenameOnFail)
                        studyInfo.isZip ? markZipPath(dataDir, '_FAIL_') :
                                Files.move(dataDir, dataDir.resolveSibling("_FAIL_${dataDir.fileName}"))

                    if (config.stopOnFail)
                        break
                }
            }
        }

        res
    }

    private Path markZipPath(Path dir,String mark) {
        Path markingPath = dir.resolveSibling("${mark}${dir.fileName}")
        Files.createDirectory(markingPath)

        PathUtils.movePath(dir, markingPath)
        PathUtils.deletePath(dir)

        markingPath
    }

}
