package com.thomsonreuters.lsps.transmart.etl.statistic

import groovy.transform.CompileStatic

/**
 * Date: 06.10.2014
 * Time: 14:58
 */
@CompileStatic
class VariableStatistic {
    final String name
    final VariableType type
    final List<ValidationRule> validationRules

    Factor factor
    long notEmptyValuesCount
    long totalValuesCount
    private double prevMean, prevSDBase
    private List<Double> doubleValues
    private boolean valuesSorted
    private List<RangeValidationRule> rangeValidationRules
    private ValidationRule requiredRule
    private ValidationRule typeRule
    double mean, median, min = Double.POSITIVE_INFINITY, max = Double.NEGATIVE_INFINITY, sdBase
    private Map<ValidationRule, List<String>> violatedRules = [:]
    boolean unique

    VariableStatistic(String name, VariableType type, List<ValidationRule> validationRules) {
        this.name = name
        this.type = type
        this.validationRules = Collections.unmodifiableList(validationRules)
        requiredRule = validationRules.find { it.type == ValidationRuleType.Required }
        if (type in [VariableType.Numerical, VariableType.Date]) {
            typeRule = new ValidationRule(ValidationRuleType.Type, String.format("Type is %s", type.name()))
        }
        unique = validationRules.any { it.type == ValidationRuleType.Unique }
        if (type == VariableType.ID) {
            if (!requiredRule) {
                requiredRule = new ValidationRule(ValidationRuleType.Required, "ID is required")
            }
            this.unique = true
        }

        if (type == VariableType.Categorical) {
            factor = new Factor()
        } else if (type == VariableType.Numerical) {
            doubleValues = []
            this.rangeValidationRules = []
            for (def rule : validationRules) {
                if (rule.type == ValidationRuleType.RangeCheck) {
                    rangeValidationRules.add(rule as RangeValidationRule)
                }
            }
        }
    }

    private List<Double> getSortedDoubleValues() {
        if (!valuesSorted) {
            Collections.sort(doubleValues)
            valuesSorted = true
        }
        doubleValues
    }

    private static double getMedianImpl(List<Double> sortedValues) {
        if (sortedValues.size() == 0)
            return Double.NaN
        int middle = (int) (sortedValues.size() / 2)
        if ((sortedValues.size() & 1) == 1) {
            return sortedValues.get(middle)
        } else {
            return (sortedValues.get(middle - 1) + sortedValues.get(middle)) / 2
        }
    }

    double getMin() {
        Double.isInfinite(min) ? Double.NaN : min
    }

    double getMax() {
        Double.isInfinite(max) ? Double.NaN : max
    }

    // Tukey's hinges method
    double getLowerQuartile() {
        def values = sortedDoubleValues
        getMedianImpl(values.subList(0, (int) (values.size() + 1) / 2))
    }

    // Tukey's hinges method
    double getUpperQuartile() {
        def values = sortedDoubleValues
        getMedianImpl(values.subList((int) values.size() / 2, values.size()))
    }

    double getIqr() {
        getUpperQuartile() - getLowerQuartile()
    }

    double getMedian() {
        getMedianImpl(sortedDoubleValues)
    }

    double getStandardDerivation() {
        return Math.sqrt(sdBase / (notEmptyValuesCount - 1))
    }

    long getEmptyValuesCount() {
        return totalValuesCount - notEmptyValuesCount
    }

    boolean getHasMissingData() {
        return emptyValuesCount > 0
    }

    boolean getHasRangeChecks() {
        return rangeValidationRules != null && !rangeValidationRules.empty
    }

    boolean getRequired() {
        return requiredRule != null
    }

    List<String> getMissingValueIds() {
        if (required) violatedRules[requiredRule] ?: []
    }

    Map<String, List<String>> getViolatedRangeChecks() {
        Map<String, List<String>> violatedRangeChecks = [:]
        for (def entry : violatedRules.entrySet()) {
            if (entry.key.type == ValidationRuleType.RangeCheck || entry.key.type == ValidationRuleType.Type) {
                violatedRangeChecks.put(entry.key.description, entry.value)
            }
        }
        violatedRangeChecks
    }

    private boolean isRuleApplicable(ValidationRule rule, Map<String, String> variableValues) {
        if (rule.conditionField.is(null)) {
            return true
        }
        String value = variableValues.get(rule.conditionField)
        if (value.is(null)) {
            value = ''
        }
        return rule.condition.check(value)
    }

    void collectValue(String id, String value, Map<String, String> variableValues) {
        totalValuesCount++
        if (!value.isEmpty()) {
            notEmptyValuesCount++
            switch (type) {
                case VariableType.Categorical:
                    collectCategoricalValue(value)
                    break
                case VariableType.Numerical:
                    try {
                        double doubleValue = Double.parseDouble(value)
                        checkValueInRange(id, doubleValue, variableValues)
                        collectNumericalValue(doubleValue)
                    } catch (NumberFormatException ex) {
                        addRuleViolation(typeRule, id)
                    }
                    break
            }
        } else if (required && isRuleApplicable(requiredRule, variableValues)) {
            addRuleViolation(requiredRule, id)
        }
    }

    private collectCategoricalValue(String value) {
        factor.addValue(value)
    }

    private void checkValueInRange(String id, double value, Map<String, String> variableValues) {
        rangeValidationRules.each {
            if (isRuleApplicable(it, variableValues) && !it.range.contains(value)) {
                addRuleViolation(it, id)
            }
        }
    }

    private void collectNumericalValue(double doubleValue) {
        doubleValues.add(doubleValue)
        valuesSorted = false
        mean = prevMean + (doubleValue - prevMean) / notEmptyValuesCount
        sdBase = prevSDBase + (doubleValue - prevMean) * (doubleValue - mean)
        if (doubleValue < min) {
            min = doubleValue
        }
        if (doubleValue > max) {
            max = doubleValue
        }
        prevMean = mean
        prevSDBase = sdBase
    }

    private void addRuleViolation(ValidationRule rule, String id) {
        List<String> ids = violatedRules.get(rule)
        if (ids.is(null)) {
            violatedRules.put(rule, ids = [])
        }
        ids.add(id)
    }

    private static String idList(ids) {
        ids.collect { "'${it}'" }.join(', ')
    }

    String getQCMissingData() {
        if (required) {
            hasMissingData ? "${emptyValuesCount} missing (${idList(missingValueIds)})" : 'OK'
        } else {
            ''
        }
    }

    String getQCRangeCheck() {
        if (notEmptyValuesCount == 0) {
            return 'All values are empty'
        }
        if (hasRangeChecks) {
            def violatedRangeChecks = violatedRangeChecks
            if (violatedRangeChecks) {
                "Range checks failed: ${violatedRangeChecks.collect { desc, ids -> "${desc} (${idList(ids)})" }.join('; ')}"
            } else {
                'OK'
            }
        } else {
            ''
        }
    }

    @Override
    String toString() {
        return "${name}(${type.name()})"
    }
}
