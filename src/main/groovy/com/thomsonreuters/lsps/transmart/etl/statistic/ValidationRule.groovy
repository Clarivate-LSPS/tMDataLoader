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
    final String conditionField
    final ValidationRuleCondition condition

    ValidationRule(ValidationRuleType type, String description, String conditionField, ValidationRuleCondition condition) {
        this.type = type
        this.description = description
        this.conditionField = conditionField
        this.condition = condition
    }

    ValidationRule(ValidationRuleType type, String description) {
        this(type, description, null, null)
    }

    public static List<ValidationRule> parseList(String validationRules) {
        def rules = []
        if (!validationRules) {
            return rules
        }
        def tokenizer = new StringTokenizer(validationRules, ';')
        while (tokenizer.hasMoreTokens()) {
            ValidationRule rule = parse(tokenizer.nextToken().trim())
            if (rule) {
                rules.add(rule)
            }
        }
        return rules
    }

    private static final Pattern whenCondition = Pattern.compile(/^\s*(.*)\s*,\s+when\s+"([^"]+)"\s+is\s+(.*)$/)
    private static final Pattern greaterThanOrEqualTo = Pattern.compile(/^(?:greater than or equal to\s+|>=)\s*(.+)$/,
            Pattern.CASE_INSENSITIVE)
    private static
    final Pattern greaterThan = Pattern.compile(/^(?:greater than\s+|>)\s*(.+)$/, Pattern.CASE_INSENSITIVE)
    private static final Pattern lesserThanOrEqualTo = Pattern.compile(/^(?:lesser than or equal to\s+|<=)\s*(.+)$/,
            Pattern.CASE_INSENSITIVE)
    private static final Pattern lesserThan = Pattern.compile(/^(?:lesser than\s+|<)\s*(.+)$/, Pattern.CASE_INSENSITIVE)
    private static final Pattern between = Pattern.compile(/^between\s+(.+?)\s+to\s+(.+)$/, Pattern.CASE_INSENSITIVE)
    private static final Pattern range = Pattern.compile(/^(.+?)-(.+)$/)
    private static final Pattern equality = Pattern.compile(/^(not\s+)?equals?\s+to\s+"([^"]*)"$/, Pattern.CASE_INSENSITIVE)
    private static final Pattern presence = Pattern.compile(/^present$/, Pattern.CASE_INSENSITIVE)
    private static final Pattern absence = Pattern.compile(/^blank/, Pattern.CASE_INSENSITIVE)

    private static final Map<Pattern, Closure<ValueRange<String>>> valueRangeFactory = [
            (greaterThanOrEqualTo): { Matcher m -> new ValueRange<>(from: m.group(1), includeFrom: true) },
            (greaterThan)         : { Matcher m -> new ValueRange<>(from: m.group(1), includeFrom: false) },
            (lesserThanOrEqualTo) : { Matcher m -> new ValueRange<>(to: m.group(1), includeTo: true) },
            (lesserThan)          : { Matcher m -> new ValueRange<>(to: m.group(1), includeTo: false) },
            (between)             : { Matcher m -> new ValueRange<>(from: m.group(1), to: m.group(2)) },
            (range)               : { Matcher m -> new ValueRange<>(from: m.group(1), to: m.group(2)) },
    ]

    private static ValidationRuleCondition parseRuleCondition(String condition) {
        ValueRange<String> range = parseValueRange(condition)
        if (!range.is(null)) {
            try {
                return new RangeCondition(convertValueRange(range, Double.&parseDouble))
            } catch (NumberFormatException ex) {
                logger.log(LogType.WARNING, "Failed to parse value range: ${condition}, error: ${ex.message}")
                return null
            }
        }
        if (presence.matcher(condition).matches()) {
            return new PresenceCondition(true)
        }
        if (absence.matcher(condition).matches()) {
            return new PresenceCondition(false)
        }
        Matcher matcher = equality.matcher(condition)
        if (matcher.find()) {
            return new EqualityCondition(matcher.group(2), matcher.group(1).is(null))
        }
        return null
    }

    private static ValueRange<String> parseValueRange(String range) {
        ValueRange<String> valueRange = null
        for (def entry : valueRangeFactory.entrySet()) {
            Pattern pattern = entry.key
            Closure<ValueRange> creator = entry.value
            Matcher matcher = pattern.matcher(range)
            if (matcher.find()) {
                try {
                    valueRange = creator(matcher)
                    break
                } catch (Exception ignored) {
                }
            }
        }
        return valueRange
    }

    private
    static <T extends Comparable<T>> ValueRange<T> convertValueRange(ValueRange<String> valueRange, Closure<T> convertWith) {
        ValueRange<T> targetRange = new ValueRange<T>()
        if (valueRange.from) {
            targetRange.from = convertWith(valueRange.from)
            targetRange.includeFrom = valueRange.includeFrom
        }
        if (valueRange.to) {
            targetRange.to = convertWith(valueRange.to)
            targetRange.includeTo = valueRange.includeTo
        }
        return targetRange
    }

    public static ValidationRule parse(String sentence) {
        String validationRuleWithCondition = sentence.trim()
        Matcher matcher = whenCondition.matcher(validationRuleWithCondition)
        ValidationRuleCondition ruleCondition = null
        String validationRule, conditionField, condition
        (validationRule, conditionField, condition) = matcher.find() ?
                [matcher.group(1), matcher.group(2), matcher.group(3)] :
                [validationRuleWithCondition, null, null]
        if (condition != null) {
            ruleCondition = parseRuleCondition(condition)
            if (ruleCondition.is(null)) {
                logger.log(LogType.WARNING, "Can't parse validation rule condition: ${sentence}, ignored")
                return null
            }
        }
        if (validationRule.toLowerCase().equals('required')) {
            return new ValidationRule(ValidationRuleType.Required, sentence, conditionField, ruleCondition)
        }
        ValueRange<String> valueRange = parseValueRange(validationRule)
        if (!valueRange) {
            logger.log(LogType.WARNING, "Can't parse validation rule: '${sentence}', ignored")
            return null
        }

        //TODO: cast range to specific type
        try {
            return new RangeValidationRule(sentence, convertValueRange(valueRange, Double.&parseDouble), conditionField, ruleCondition)
        } catch (NumberFormatException ex) {
            logger.log(LogType.WARNING, "Can't parse validation rule: '${sentence}, invalid format '${ex.message}', ignored")
            return null
        }
    }
}
