package com.thomsonreuters.lsps.transmart.etl.statistic

/**
 * Date: 21.10.2014
 * Time: 19:48
 */
class RangeCondition<T extends Comparable<T>> extends ValidationRuleCondition {
    ValueRange<T> range
    Closure<T> parseValue

    RangeCondition(ValueRange<? extends Comparable> range) {
        this.range = range
        T sample = range.from ?: range.to
        if (sample instanceof Double) {
            parseValue = (Closure<T>) Double.&parseDouble
        } else {
            parseValue = Closure.IDENTITY
        }
    }

    @Override
    boolean check(String value) {
        return range.contains(parseValue(value))
    }
}
