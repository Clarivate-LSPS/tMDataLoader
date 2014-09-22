package com.thomsonreuters.lsps.transmart.files

/**
 * Created by bondarev on 4/7/14.
 */
trait MetaInfoHeader {
    Map<String, String> getMetaInfo() {
        headComments.findAll { !it.isEmpty() && it.contains(':') }.collectEntries {
            it.split(':', 2)*.trim()
        }
    }
}
