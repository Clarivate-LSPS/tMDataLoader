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
    double mean, median, min = Double.MAX_VALUE, max = Double.MIN_VALUE, sdBase
    private List<String> missingDataIds = []
    boolean required
    boolean unique

    VariableStatistic(String name, VariableType type, List<ValidationRule> validationRules) {
        this.name = name
        this.type = type
        this.validationRules = Collections.unmodifiableList(validationRules)
        this.unique = type == VariableType.ID || validationRules.any { it.type == ValidationRuleType.Unique }
        this.required = type == VariableType.ID || validationRules.any { it.type == ValidationRuleType.Required }
        if (type == VariableType.Categorical) {
            factor = new Factor()
        } else if (type == VariableType.Numerical) {
            doubleValues = []
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

    String getQCMissingData() {
        required ? (hasMissingData ? "${emptyValuesCount} missing (${missingDataIds.collect { "'${it}'" }.join(', ')})" : 'OK') : ''
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
                    collectNumericalValue(value)
                    break
            }
        } else if (required) {
            missingDataIds.add(id)
        }
    }

    private collectCategoricalValue(String value) {
        factor.addValue(value)
    }

    private void collectNumericalValue(String value) {
        double doubleValue = Double.parseDouble(value)
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

    @Override
    String toString() {
        return "${name}(${type.name()})"
    }
}
