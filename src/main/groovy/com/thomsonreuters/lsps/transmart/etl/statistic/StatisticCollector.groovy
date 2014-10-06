package com.thomsonreuters.lsps.transmart.etl.statistic
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
/**
 * Date: 06.10.2014
 * Time: 14:52
 */
class StatisticCollector {
    Map<String, TableStatistic> tables = [:]
    TableStatistic currentTable

    def collectForTable(String tableName, @ClosureParams(value = SimpleType.class, options = 'com.thomsonreuters.lsps.transmart.etl.statistic.TableStatistic') Closure closure) {
        TableStatistic savedTable = currentTable
        try {
            TableStatistic tableStatistic = new TableStatistic()
            currentTable = tableStatistic
            tables.put(tableName, tableStatistic)
            closure.call(tableStatistic)
            return tableStatistic
        } finally {
            currentTable = savedTable
        }
    }

    def startCollectForRecord() {
        currentTable.startCollectForRecord()
    }

    def collectVariableValue(String name, String value) {
        currentTable.collectVariableValue(name, value)
    }

    def endCollectForRecord() {
        currentTable.endCollectForRecord()
    }

    def collectForRecord(Map variableValues) {
        currentTable.collectForRecord(variableValues)
    }
}
