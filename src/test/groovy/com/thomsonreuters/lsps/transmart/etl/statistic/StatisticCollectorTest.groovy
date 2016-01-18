package com.thomsonreuters.lsps.transmart.etl.statistic

import spock.lang.Specification

/**
 * Date: 06.10.2014
 * Time: 17:35
 */
class StatisticCollectorTest extends Specification {
    def 'it should collect statistic'() {
        when:
        def statistic = new StatisticCollector()
        statistic.collectForTable('TEST') { table ->
            table.withRecordStatisticForVariable('id', VariableType.ID)
                    .withRecordStatisticForVariable('txt', VariableType.Text)
                    .withRecordStatisticForVariable('cat', VariableType.Categorical)
                    .withRecordStatisticForVariable('num', VariableType.Numerical)

            table.collectForRecord(id: '1', txt: 'Some value', cat: 'cat1', num: '10')
            table.collectForRecord(id: '2', txt: 'Other value', cat: 'cat2', num: '50')
            table.collectForRecord(id: '3', txt: 'Yet other value', cat: 'cat1', num: '60')
        }
        then:
        statistic.tables.TEST != null
        Map<String, VariableStatistic> vars = statistic.tables.TEST.variables
        [vars.id.name, vars.id.type, vars.id.emptyValuesCount, vars.id.notEmptyValuesCount, vars.id.required, vars.id.missingValueIds] ==
                ['id', VariableType.ID, 0, 3, true, []]
        [vars.txt.name, vars.txt.type, vars.txt.emptyValuesCount, vars.txt.notEmptyValuesCount, vars.txt.required, vars.txt.missingValueIds] ==
                ['txt', VariableType.Text, 0, 3, false, null]
        [vars.cat.name, vars.cat.type, vars.cat.emptyValuesCount, vars.cat.notEmptyValuesCount, vars.cat.required, vars.cat.missingValueIds, vars.cat.factor] ==
                ['cat', VariableType.Categorical, 0, 3, false, null, new Factor(cat1: 2, cat2: 1)]
        [vars.num.name, vars.num.type, vars.num.emptyValuesCount, vars.num.notEmptyValuesCount, vars.num.required, vars.num.missingValueIds,
         vars.num.mean, vars.num.median, vars.num.min, vars.num.max, vars.num.standardDerivation.round(3)] ==
                ['num', VariableType.Numerical, 0, 3, false, null, 40.0, 50.0, 10.0, 60.0, 26.458]
    }

    def 'it should calculate IQR'() {
        given:
        def table = new TableStatistic()
        table.withRecordStatisticForVariables(id: VariableType.ID, num: VariableType.Numerical)

        when:
        def num = table.variables.num

        then:
        num.iqr == Double.NaN

        when:
        table.collectForRecord(id: '1', num: '100')

        then:
        num.iqr == 0

        when:
        table.collectForRecord(id: '1', num: '500')

        then:
        num.iqr == 400

        when:
        table.collectForRecord(id: '1', num: '300')

        then:
        num.lowerQuartile == 200
        num.upperQuartile == 400
        num.iqr == 200

        when:
        table.collectForRecord(id: '1', num: '200')
        table.collectForRecord(id: '1', num: '400')

        then:
        num.iqr == 200

        when:
        table.collectForRecord(id: '1', num: '600')

        then:
        num.lowerQuartile == 200
        num.upperQuartile == 500
        num.iqr == 300
    }

    def 'it should validate variable type'() {
        given:
        def table = new TableStatistic()
        table.withRecordStatisticForVariables(id: VariableType.ID, num: VariableType.Numerical)
        def num = table.variables.num

        when:
        table.collectForRecord(id: '1', num: 'a')
        table.collectForRecord(id: '2', num: 'b')

        then:
        num.min == Float.NaN
        num.violatedRangeChecks == ['Type is Numerical': ['1', '2']]
    }

    def 'it should report if all variable values are empty'() {
        given:
        def table = new TableStatistic()
        table.withRecordStatisticForVariables(id: VariableType.ID, num: VariableType.Numerical).
                withRecordStatisticForVariable('numWithChecks', VariableType.Numerical, ValidationRule.parseList('>10'))
        def num = table.variables.num
        def numWithChecks = table.variables.numWithChecks

        when:
        table.collectForRecord(id: '1', num: '', numWithChecks: '')
        table.collectForRecord(id: '2', num: '', numWithChecks: '')

        then:
        num.QCRangeCheck == 'All values are empty'
        numWithChecks.QCRangeCheck == 'All values are empty'
    }
}
