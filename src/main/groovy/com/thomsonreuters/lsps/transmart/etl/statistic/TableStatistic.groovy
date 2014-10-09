package com.thomsonreuters.lsps.transmart.etl.statistic

/**
 * Date: 06.10.2014
 * Time: 14:58
 */
class TableStatistic {
    VariableStatistic idVariable
    Map<String, VariableStatistic> variables = [:]

    private Map currentVariableValues

    TableStatistic withRecordStatisticForVariable(String name, VariableType variableType) {
        withRecordStatisticForVariable(name, variableType, [])
    }

    TableStatistic withRecordStatisticForVariable(String name, VariableType variableType, List<ValidationRule> validationRules) {
        VariableStatistic variable = new VariableStatistic(name, variableType, validationRules)
        if (variableType == VariableType.ID) {
            if (idVariable != null) {
                throw new UnsupportedOperationException("Can't set ID variable to '${name}'. Another ID variable is already defined ('${idVariable.name}')")
            }
            idVariable = variable
        }
        variables.put(name, variable)
        return this
    }

    def startCollectForRecord() {
        if (idVariable.is(null)) {
            throw new UnsupportedOperationException('No ID variable is defined')
        }
        if (!currentVariableValues.is(null)) {
            throw new UnsupportedOperationException('Another record collecting is already in progress');
        }
        currentVariableValues = [:]
    }

    def collectVariableValue(String name, String value) {
        currentVariableValues.put(name, value)
    }

    def endCollectForRecord() {
        if (currentVariableValues.is(null)) {
            throw new UnsupportedOperationException('No active collectForRecord operation in progress')
        }
        collectForRecord(currentVariableValues)
        currentVariableValues = null
    }

    def collectForRecord(Map<String, String> variableValues) {
        String id = variableValues.get(idVariable.name)
        variableValues.each { String name, String value ->
            VariableStatistic var = variables.get(name)
            if (var.is(null)) {
                throw new IllegalArgumentException("Unknown variable: ${name}")
            }
            var.collectValue(id, value, variableValues)
        }
    }
}
