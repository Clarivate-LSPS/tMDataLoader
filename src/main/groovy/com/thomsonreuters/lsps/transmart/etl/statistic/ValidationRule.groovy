package com.thomsonreuters.lsps.transmart.etl.statistic

import com.thomsonreuters.lsps.transmart.etl.LogType
import com.thomsonreuters.lsps.transmart.etl.Logger

import java.util.regex.Matcher
import java.util.regex.Pattern

/**
 * Date: 07.10.2014
 * Time: 17:32
 */
class ValidationRule {
    private static final Logger logger = Logger.getLogger(ValidationRule.class)

    final ValidationRuleType type
    final String description

    ValidationRule(ValidationRuleType type, String description) {
        this.type = type
        this.description = description
    }

    public static List<ValidationRule> parseList(String validationRules) {
        def rules = []
        if (!validationRules) {
            return rules
        }
        def tokenizer = new StringTokenizer(validationRules, ';')
        while (tokenizer.hasMoreTokens()) {
            ValidationRule rule = parse(tokenizer.nextToken())
            if (rule) {
                rules.add(rule)
            }
        }
        return rules
    }

    private static final Pattern whenCondition = Pattern.compile(/^\s*(.*)\s*,\s+when\s+(.*)\s*$/)
    private static final Pattern greaterThanOrEqualTo = Pattern.compile(/^(?:greater than or equal to\s+|>=)\s*(.+)$/,
            Pattern.CASE_INSENSITIVE)
    private static final Pattern greaterThan = Pattern.compile(/^(?:greater than\s+|>)\s*(.+)$/, Pattern.CASE_INSENSITIVE)
    private static final Pattern lesserThanOrEqualTo = Pattern.compile(/^(?:lesser than or equal to\s+|<=)\s*(.+)$/,
            Pattern.CASE_INSENSITIVE)
    private static final Pattern lesserThan = Pattern.compile(/^(?:lesser than\s+|<)\s*(.+)$/, Pattern.CASE_INSENSITIVE)
    private static final Pattern between = Pattern.compile(/^between\s+(.+?)\s+to\s+(.+)$/, Pattern.CASE_INSENSITIVE)
    private static final Pattern range = Pattern.compile(/^(.+?)-(.+)$/)

    private static final Map<Pattern, Closure<ValueRange<String>>> valueRangeFactory = [
            (greaterThanOrEqualTo): { Matcher m -> new ValueRange<>(from: m.group(1), includeFrom: true) },
            (greaterThan)         : { Matcher m -> new ValueRange<>(from: m.group(1), includeFrom: false) },
            (lesserThanOrEqualTo) : { Matcher m -> new ValueRange<>(to: m.group(1), includeTo: true) },
            (lesserThan)          : { Matcher m -> new ValueRange<>(to: m.group(1), includeTo: false) },
            (between)             : { Matcher m -> new ValueRange<>(from: m.group(1), to: m.group(2)) },
            (range)               : { Matcher m -> new ValueRange<>(from: m.group(1), to: m.group(2)) },
    ]

    public static ValidationRule parse(String sentence) {
        String validationRuleWithCondition = sentence.trim()
        Matcher matcher = whenCondition.matcher(validationRuleWithCondition)
        String validationRule, condition
        (validationRule, condition) = matcher.find() ? [matcher.group(1), matcher.group(2)] : [validationRuleWithCondition, null]
        if (condition != null) {
            logger.log(LogType.WARNING, "!!!Conditions is not supported yet!!! Condition ignored: '${condition}'")
        }
        if (validationRule.toLowerCase().equals('required')) {
            return new ValidationRule(ValidationRuleType.Required, validationRule)
        }
        ValueRange<String> valueRange = null
        for (def entry : valueRangeFactory.entrySet()) {
            Pattern pattern = entry.key
            Closure<ValueRange> creator = entry.value
            matcher = pattern.matcher(validationRule)
            if (matcher.find()) {
                try {
                    valueRange = creator(matcher)
                    break
                } catch(Exception ignored) {
                }
            }
        }
        if (!valueRange) {
            logger.log(LogType.WARNING, "Can't parse validation rule: '${validationRule}', ignored")
            return null
        }

        //TODO: cast range to specific type
        ValueRange<Double> targetRange = new ValueRange<Double>()
        try {
            if (valueRange.from) {
                targetRange.from = Double.parseDouble(valueRange.from)
                targetRange.includeFrom = valueRange.includeFrom
            }
            if (valueRange.to) {
                targetRange.to = Double.parseDouble(valueRange.to)
                targetRange.includeTo = valueRange.includeTo
            }
            return new RangeValidationRule(validationRule, targetRange)
        } catch (NumberFormatException ex) {
            logger.log(LogType.WARNING, "Can't parse validation rule: '${validationRule}, invalid format '${ex.message}', ignored")
            return null
        }
    }
}
