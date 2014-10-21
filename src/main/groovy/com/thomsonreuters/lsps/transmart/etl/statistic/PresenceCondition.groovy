package com.thomsonreuters.lsps.transmart.etl.statistic

/**
 * Date: 21.10.2014
 * Time: 19:48
 */
class PresenceCondition extends ValidationRuleCondition {
    boolean presence

    PresenceCondition(boolean presence) {
        this.presence = presence
    }

    @Override
    boolean check(String value) {
        return value.empty != presence
    }
}
