package com.thomsonreuters.lsps.transmart.etl.statistic

/**
 * Date: 06.10.2014
 * Time: 14:58
 */
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
    private List<RangeValidationRule> rangeValidationRules;
    private ValidationRule requiredRule;
    double mean, median, min = Double.MAX_VALUE, max = Double.MIN_VALUE, sdBase
    private Map<ValidationRule, List<String>> violatedRules = [:]
    boolean unique

    VariableStatistic(String name, VariableType type, List<ValidationRule> validationRules) {
        this.name = name
        this.type = type
        this.validationRules = Collections.unmodifiableList(validationRules)
        requiredRule = validationRules.find { it.type == ValidationRuleType.Required }
        this.unique = validationRules.any { it.type == ValidationRuleType.Unique }
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
            this.rangeValidationRules = validationRules.findAll { it.type == ValidationRuleType.RangeCheck }
        }
    }

    double getMedian() {
        if (!valuesSorted) {
            Collections.sort(doubleValues)
            valuesSorted = true
        }
        int middle = doubleValues.size() / 2
        if ((doubleValues.size() & 1) == 1) {
            return doubleValues.get(middle)
        } else {
            return (doubleValues.get(middle - 1) + doubleValues.get(middle)) / 2
        }
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

    boolean getRequired() {
        return requiredRule != null
    }

    List<String> getMissingValueIds() {
        if (required) violatedRules[requiredRule] ?: []
    }

    Map<String, List<String>> getViolatedRangeChecks() {
        violatedRules.findAll { it.key.type == ValidationRuleType.RangeCheck }.
                collectEntries { rule, ids -> [rule.description, ids] }
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
                    double doubleValue = Double.parseDouble(value)
                    checkValueInRange(id, doubleValue)
                    collectNumericalValue(doubleValue)
                    break
            }
        } else if (required) {
            addRuleViolation(requiredRule, id)
        }
    }

    private collectCategoricalValue(String value) {
        factor.addValue(value)
    }

    private void checkValueInRange(String id, double value) {
        rangeValidationRules.each {
            if (!it.range.contains(value)) {
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

    @Override
    String toString() {
        return "${name}(${type.name()})"
    }
}
