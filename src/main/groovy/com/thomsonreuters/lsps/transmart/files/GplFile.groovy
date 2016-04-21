package com.thomsonreuters.lsps.transmart.files

import java.nio.file.Path

/**
 * Created by bondarev on 3/28/14.
 */
class GplFile extends CsvLikeFile {
    GplFile(Path file) {
        super(file, '#')
    }

    Map<String, String> getMetaInfo() {
        MetaInfoHeader.getMetaInfo(this)
    }
}
