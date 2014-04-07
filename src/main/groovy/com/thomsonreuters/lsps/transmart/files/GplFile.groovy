package com.thomsonreuters.lsps.transmart.files

/**
 * Created by bondarev on 3/28/14.
 */
@Mixin(MetaInfoHeader)
class GplFile extends CsvLikeFile {
    GplFile(File file) {
        super(file, '#')
    }
}
