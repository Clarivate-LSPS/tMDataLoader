package com.thomsonreuters.lsps.transmart.etl

import java.nio.file.Path

/**
 * Date: 21-Apr-16
 * Time: 15:48
 */
interface DataProcessor {
    boolean process(Path dir, studyInfo)
}