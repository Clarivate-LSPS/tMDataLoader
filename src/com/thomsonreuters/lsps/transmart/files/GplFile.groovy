package com.thomsonreuters.lsps.transmart.files

/**
 * Created by bondarev on 3/28/14.
 */
class GplFile extends CsvLikeFile {
    def logger

    GplFile(File file) {
        super(file, '#')
    }


}
