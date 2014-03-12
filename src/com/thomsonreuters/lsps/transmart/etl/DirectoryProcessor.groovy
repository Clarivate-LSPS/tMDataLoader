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

import static groovy.io.FileType.*

class DirectoryProcessor {
    def config

    DirectoryProcessor(conf) {
        config = conf
    }

    boolean process(dir, String root = "") {           // TODO: BROKEN NOW!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        def d

        if (dir instanceof File)
            d = dir
        else
            d = new File(dir)

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
        catch (Exception e) {
            return false
        }
    }

    private boolean checkIfHasStudies(dir) {
        def res = false
        // looping through the subfolders, looking for study folders
        dir.eachDirMatch(~/[^\._].+/) {
            if (checkIfStudyFolder(it)) {
                res = true // can't break out of closure easily other than using exception
            }
        }

        return res
    }

    private boolean checkIfStudyFolder(dir) {
        def res = false
        // looping through the subfolders, looking for predefined names
        dir.eachDirMatch(~/[^\._].+/) {
            if (it.name ==~ /(ClinicalData|ExpressionData|S(?:NP|np)Data|RBMData|MetaData)(ToUpload)*/) {
                res = true
            }
        }

        return res
    }

    private boolean processMetaData(dir) {
        def isAllSuccessful = true

        dir.eachFileMatch(~/(?i)(?!\.|_DONE_|_FAIL_|_DISABLED_).+\.txt/) {

            config.logger.log("Processing metadata file ${it.name}")

            def metadataProcessor = new MetaDataProcessor(config)
            def res = false

            try {
                res = metadataProcessor.process(it, {})
            }
            catch (Exception e) {
                config.logger.log(LogType.ERROR, "Exception: ${e}")
            }

            if (res) {
                it.renameTo(new File(dir, "_DONE_${it.name}"))
            } else {
                if (!config.isNoRenameOnFail)
                    it.renameTo(new File(dir, "_FAIL_${it.name}"))
            }

            isAllSuccessful = isAllSuccessful && res
        }

        return isAllSuccessful
    }

    private boolean processStudies(d, String parentNode) {

        config.logger.log("=== PROCESSING STUDIES IN ${parentNode} ===")
        def isAllSuccessful = true

        d.eachDirMatch(~/(?!\.|_DONE_|_FAIL_|_DISABLED_).+/) {
            // looping through studies
            // dir name is the study

            def studyName = it.name
            config.logger.log "== Found study: ${studyName} =="

            def studyInfo = ['name': studyName, 'node': "${parentNode}\\${studyName}".toString()]

            def isStudyUploadSuccessful = true

            // looking for clinical data first
            def dataDir = new File(it, 'ClinicalData')
            if (!(dataDir.exists() && dataDir.isDirectory())) {
                dataDir = new File(it, 'ClinicalDataToUpload')
                if (!(dataDir.exists() && dataDir.isDirectory())) {
                    dataDir = null
                }
            }

            if (dataDir) {
                config.logger.log "Processing clinical data"
                def res = false

                def clinicalProcessor = new ClinicalDataProcessor(config)
                try {
                    res = clinicalProcessor.process(dataDir, studyInfo)
                }
                catch (Exception e) {
                    config.logger.log(LogType.ERROR, "Exception: ${e}")
                }

                if (res) {
                    dataDir.renameTo(new File(it, "_DONE_${dataDir.name}"))
                } else {
                    if (!config.isNoRenameOnFail)
                        dataDir.renameTo(new File(it, "_FAIL_${dataDir.name}"))

                    if (config.stopOnFail) return false
                }

                isStudyUploadSuccessful = isStudyUploadSuccessful && res
            }

            // then expression data
            dataDir = new File(it, 'ExpressionData')
            if (!(dataDir.exists() && dataDir.isDirectory())) {
                dataDir = new File(it, 'ExpressionDataToUpload')
                if (!(dataDir.exists() && dataDir.isDirectory())) {
                    dataDir = null
                }
            }

            if (dataDir) {
                config.logger.log "Processing expression data"
                def res = false

                def expProcessor = new ExpressionDataProcessor(config)
                try {
                    res = expProcessor.process(dataDir, studyInfo)
                }
                catch (Exception e) {
                    config.logger.log(LogType.ERROR, "Exception: ${e}")
                }

                if (res) {
                    dataDir.renameTo(new File(it, "_DONE_${dataDir.name}"))
                } else {
                    if (!config.isNoRenameOnFail)
                        dataDir.renameTo(new File(it, "_FAIL_${dataDir.name}"))

                    if (config.stopOnFail) return false
                }

                isStudyUploadSuccessful = isStudyUploadSuccessful && res
            }

            // SNP data
            dataDir = ['SNPData', 'SNPDataToUpload', 'SnpDataToUpload'].
                    collect { String dirName -> new File(it as File, dirName) }.
                    find { File f -> f.exists() && f.isDirectory() }

            if (dataDir) {
                config.logger.log "Processing SNP data"
                def res = false

                def expProcessor = new SNPDataProcessor(config)
                try {
                    res = expProcessor.process(dataDir, studyInfo)
                }
                catch (Exception e) {
                    config.logger.log(LogType.ERROR, "Exception: ${e}")
                }

                if (res) {
                    dataDir.renameTo(new File(it, "_DONE_${dataDir.name}"))
                } else {
                    if (!config.isNoRenameOnFail)
                        dataDir.renameTo(new File(it, "_FAIL_${dataDir.name}"))

                    if (config.stopOnFail) return false
                }

                isStudyUploadSuccessful = isStudyUploadSuccessful && res
            }

            // then RBM data
            dataDir = new File(it, 'RBMData')
            if (!(dataDir.exists() && dataDir.isDirectory())) {
                dataDir = new File(it, 'RBMDataToUpload')
                if (!(dataDir.exists() && dataDir.isDirectory())) {
                    dataDir = null
                }
            }

            if (dataDir) {
                config.logger.log "Processing RBM data"
                def res = false

                def rbmProcessor = new RBMDataProcessor(config)
                try {
                    res = rbmProcessor.process(dataDir, studyInfo)
                }
                catch (Exception e) {
                    config.logger.log(LogType.ERROR, "Exception: ${e}")
                }

                if (res) {
                    dataDir.renameTo(new File(it, "_DONE_${dataDir.name}"))
                } else {
                    if (!config.isNoRenameOnFail)
                        dataDir.renameTo(new File(it, "_FAIL_${dataDir.name}"))

                    if (config.stopOnFail) return false
                }

                isStudyUploadSuccessful = isStudyUploadSuccessful && res
            }

            // then metadata
            dataDir = new File(it, 'MetaData')
            if (!(dataDir.exists() && dataDir.isDirectory())) {
                dataDir = new File(it, 'MetaDataToUpload')
                if (!(dataDir.exists() && dataDir.isDirectory())) {
                    dataDir = null
                }
            }

            if (dataDir) {
                config.logger.log "Processing study metadata"
                def res = processMetaData(dataDir)

                if (res) {
                    dataDir.renameTo(new File(it, "_DONE_${dataDir.name}"))
                } else {
                    if (!config.isNoRenameOnFail)
                        dataDir.renameTo(new File(it, "_FAIL_${dataDir.name}"))

                    if (config.stopOnFail) return false
                }

                isStudyUploadSuccessful = isStudyUploadSuccessful && res
            }

            if (isStudyUploadSuccessful) {
                it.renameTo(new File(d, "_DONE_${it.name}"))
            } else {
                if (!config.isNoRenameOnFail)
                    it.renameTo(new File(d, "_FAIL_${it.name}"))
            }

            isAllSuccessful = isAllSuccessful && isStudyUploadSuccessful
        }

        config.logger.log("=== FINISHED PROCESSING ${parentNode} ===")
        return isAllSuccessful
    }
}
