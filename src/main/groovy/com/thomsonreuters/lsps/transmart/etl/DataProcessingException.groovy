package com.thomsonreuters.lsps.transmart.etl

import groovy.transform.CompileStatic

/**
 * Date: 17-Dec-15
 * Time: 18:37
 */
@CompileStatic
class DataProcessingException extends RuntimeException {
    DataProcessingException(String message) {
        super(message)
    }
}
