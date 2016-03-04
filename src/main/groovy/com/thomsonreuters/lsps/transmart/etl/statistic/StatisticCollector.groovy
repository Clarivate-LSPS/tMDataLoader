package com.thomsonreuters.lsps.transmart.etl.statistic

import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVPrinter

/**
 * Date: 06.10.2014
 * Time: 14:52
 */
class StatisticCollector {
    Map<String, TableStatistic> tables = [:]
    TableStatistic currentTable

    TableStatistic collectForTable(String tableName,
                        @ClosureParams(value = SimpleType.class, options = 'com.thomsonreuters.lsps.transmart.etl.statistic.TableStatistic') Closure closure) {
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

    void printReport(Appendable out) {
        CSVFormat format = CSVFormat.TDF.withRecordSeparator(System.getProperty("line.separator")).
                withHeader('File', 'Variable', 'Variable Type', 'N', 'null', 'Mean', 'Median', 'IQR', 'Min', 'Max', 'SD', 'Count', 'Required', 'Validation rule', 'QC missing data', 'QC data range')
        CSVPrinter printer = new CSVPrinter(out, format)
        tables.each { tableName, table ->
            table.variables.each { _, var ->
                printer.print(tableName)
                printer.print(var.name)
                printer.print(var.type.name())
                printer.print(var.notEmptyValuesCount)
                printer.print(var.emptyValuesCount)
                if (var.type == VariableType.Numerical && var.notEmptyValuesCount > 0) {
                    printer.print(var.mean.round(6))
                    printer.print(var.median.round(6))
                    printer.print(var.iqr.round(6))
                    printer.print(var.min.round(6))
                    printer.print(var.max.round(6))
                    printer.print(var.standardDerivation.round(6))
                } else {
                    printer.print('')
                    printer.print('')
                    printer.print('')
                    printer.print('')
                    printer.print('')
                    printer.print('')
                }
                if (var.type == VariableType.Categorical) {
                    printer.print(var.factor.counts.collect { val, count -> "${val}: ${count}" }?.join(', '))
                } else {
                    printer.print('')
                }
                printer.print(var.required ? 'Yes' : '')
                printer.print(var.validationRules.findAll { it.type != ValidationRuleType.Required }*.description.join('; '))
                printer.print(var.QCMissingData)
                printer.print(var.QCRangeCheck)
                printer.println()
            }
        }
        printer.flush()
    }
}
