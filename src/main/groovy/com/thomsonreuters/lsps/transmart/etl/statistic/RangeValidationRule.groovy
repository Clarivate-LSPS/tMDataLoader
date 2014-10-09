package com.thomsonreuters.lsps.transmart.etl.statistic

/**
 * Date: 09.10.2014
 * Time: 18:34
 */
class RangeValidationRule<T extends Comparable<T>> extends ValidationRule {
    ValueRange<T> range

    RangeValidationRule(String description, ValueRange<T> range) {
        super(ValidationRuleType.RangeCheck, description)
        this.range = range
    }
}
