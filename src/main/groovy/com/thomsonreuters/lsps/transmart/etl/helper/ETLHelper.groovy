package com.thomsonreuters.lsps.transmart.etl.helper

import java.sql.Timestamp
import java.text.SimpleDateFormat

class ETLHelper {
    private static def DATE_FORMAT = [
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd HH:mm",
            "yyyy-MM-dd",
    ]

    static String toTimestampString(String dateStr){
        Date localDateTime = null
        for (def format: DATE_FORMAT){
            try{
                localDateTime = new SimpleDateFormat(format).parse(dateStr)
                break;
            } catch (Exception e){
                continue;
            }
        }
        return new Timestamp(localDateTime.getTime()).toString()
    }
}
