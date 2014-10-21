package com.thomsonreuters.lsps.transmart.etl.statistic

/**
 * Date: 21.10.2014
 * Time: 19:46
 */
abstract class ValidationRuleCondition {
    abstract boolean check(String value)
}
