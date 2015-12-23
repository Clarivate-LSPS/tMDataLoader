package com.thomsonreuters.lsps.transmart.etl.statistic

/**
 * Date: 23-Dec-15
 * Time: 12:36
 */
enum Quartile {
    LOWER(25),
    MEDIAN(50),
    UPPER(75);

    int percent

    Quartile(int percent) {
        this.percent = percent
    }
}
