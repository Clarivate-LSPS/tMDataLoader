package com.thomsonreuters.lsps.utils

import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Date: 11-May-16
 * Time: 13:57
 */
class ResourceUtils {
    public static <T> T withCloseableResources(
            @ClosureParams(value = SimpleType, options = ['java.util.List']) Closure<T> closure) {
        def resources = []
        try {
            closure.call(resources)
        } finally {
            for (def closeable : resources) {
                try {
                    closeable.close()
                } catch (Exception ignored) {
                }
            }
        }
    }
}
