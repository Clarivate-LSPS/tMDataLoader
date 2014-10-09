package com.thomsonreuters.lsps.transmart.etl.statistic

import com.thomsonreuters.lsps.transmart.etl.LogType
import com.thomsonreuters.lsps.transmart.etl.Logger

/**
 * Date: 07.10.2014
 * Time: 17:32
 */
class ValidationRule {
    private static final Logger logger = Logger.getLogger(ValidationRule.class)

    final ValidationRuleType type

    ValidationRule(ValidationRuleType type) {
        this.type = type
    }

    public static List<ValidationRule> parseList(String validationRules) {
        def rules = []
        if (!validationRules) {
            return rules
        }
        def tokenizer = new StringTokenizer(validationRules, ';')
        while (tokenizer.hasMoreTokens()) {
            rules.add(parse(tokenizer.nextToken()))
        }
        return rules
    }

    public static ValidationRule parse(String validationRule) {
        if (validationRule.toLowerCase().equals('required')) {
            return new ValidationRule(ValidationRuleType.Required)
        } else {
            logger.log(LogType.WARNING, "Can't parse validation rule: '${validationRule}', ignored")
        }
    }
}
