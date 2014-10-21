package com.thomsonreuters.lsps.transmart.etl.statistic

/**
 * Date: 21.10.2014
 * Time: 20:25
 */
class EqualityCondition extends ValidationRuleCondition {
    final String expectedValue
    final boolean isEqual

    EqualityCondition(String expectedValue, boolean isEqual) {
        this.expectedValue = expectedValue
        this.isEqual = isEqual
    }

    @Override
    boolean check(String value) {
        return expectedValue.equals(value) == isEqual
    }
}
