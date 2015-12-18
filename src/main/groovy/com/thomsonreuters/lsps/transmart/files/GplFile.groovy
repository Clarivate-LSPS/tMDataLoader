package com.thomsonreuters.lsps.transmart.files

import java.nio.file.Path

/**
 * Created by bondarev on 3/28/14.
 */
class GplFile extends CsvLikeFile implements MetaInfoHeader {
    GplFile(Path file) {
        super(file, '#')
    }
}
