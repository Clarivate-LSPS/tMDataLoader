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
            parseValue = { String val ->
                try {
                    (Closure<T>) Double.parseDouble(val)
                } catch (NumberFormatException ignored) {
                    null
                }
            }
        } else {
            parseValue = Closure.IDENTITY
        }
    }

    @Override
    boolean check(String value) {
        T parsedValue = parseValue(value)
        return !parsedValue.is(null) && range.contains(parsedValue)
    }
}
