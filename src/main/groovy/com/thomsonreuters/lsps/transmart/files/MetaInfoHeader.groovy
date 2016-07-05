package com.thomsonreuters.lsps.transmart.files

import java.nio.file.Path

class MetaInfoHeader {
    static Map<String, String> getMetaInfo(CsvLikeFile csvLikeFile) {
        getMetaInfo(csvLikeFile.headComments)
    }

    static Map<String, String> getMetaInfo(String[] headComments) {
        headComments.findAll { !it.isEmpty() && it.contains(':') }.collectEntries {
            it.split(':', 2)*.trim()
        }
    }

    static Map<String, String> getMetaInfo(Path file, String lineComment) {
        file.withReader { reader ->
            getMetaInfo(HeadCommentsReader.readHeadComments(reader, lineComment))
        }
    }
}
