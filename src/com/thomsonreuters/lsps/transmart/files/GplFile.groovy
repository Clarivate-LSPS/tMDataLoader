package com.thomsonreuters.lsps.transmart.files

/**
 * Created by bondarev on 3/28/14.
 */
class GplFile extends CsvLikeFile {
    GplFile(File file) {
        super(file, '#')
    }

    Map<String, String> getMetaInfo() {
        headComments.findAll { !it.isEmpty() && it.contains(':') }.collectEntries {
            it.split(':', 2)*.trim()
        }
    }
}
