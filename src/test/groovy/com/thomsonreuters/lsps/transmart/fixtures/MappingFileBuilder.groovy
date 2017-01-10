package com.thomsonreuters.lsps.transmart.fixtures

import com.thomsonreuters.lsps.transmart.etl.statistic.VariableType

/**
 * Date: 03.11.2015
 * Time: 13:38
 */
class MappingFileBuilder {
    private String dataFileName
    private List<List<String>> mappings = []
    private boolean hasVariablesInfo
    private List<List<String>> metaInfo = []
    private boolean hasBaseline

    def forDataFile(String dataFileName, Closure block) {
        this.dataFileName = dataFileName
        try {
            mapSpecial('STUDY_ID', 1)
            mapSpecial('SUBJ_ID', 2)
            block.delegate = this
            block.call()
        } finally {
            this.dataFileName = null
        }
    }

    def addMappingRow(List<String> row) {
        mappings.add(row)
    }

    private
    def mapInternal(String categoryCd, int column, String label, String labelSource, VariableType variableType, String validationRules) {
        mapInternal(categoryCd, column, label, labelSource, null, variableType, validationRules)
    }

    private
    def mapInternal(String categoryCd, int column, String label, String labelSource, String baseline, VariableType variableType, String validationRules) {
        def row = [dataFileName, categoryCd, column.toString(), label, labelSource]
        if (baseline != null) {
            hasBaseline = true
            row.add(baseline)
        }
        if (variableType) {
            hasVariablesInfo = true
            row.add(variableType.name())
            row.add(validationRules)
        }
        mappings.add(row)
    }

    def map(String categoryCd, int column, String label, String baseline, VariableType variableType = null, String validationRules = null) {
        mapInternal(categoryCd, column, label, '', baseline, variableType, validationRules)
    }

    def map(String categoryCd, int column, String label, VariableType variableType = null, String validationRules = null) {
        mapInternal(categoryCd, column, label, '', null, variableType, validationRules)
    }

    def mapLabelSource(String categoryCd, int column, String labelSource, VariableType variableType = null, String validationRules = null) {
        mapInternal(categoryCd, column, '\\', labelSource, variableType, validationRules)
    }

    def addMetaInfo(List<String> row) {
        metaInfo.add(row)
    }

    def mapSpecial(String name, int column) {
        mapInternal('', column, name, '', null, null)
    }

    static build(Closure block) {
        MappingFileBuilder builder = new MappingFileBuilder()
        block.delegate = builder
        block.call()
        return builder
    }

    void writeTo(PrintWriter writer) {
        if (metaInfo) {
            for (def metaInfoRow : metaInfo) {
                writer.println('#' + metaInfoRow.join('\t'))
            }
        }
        def columns = ['filename', 'category_cd', 'col_nbr', 'data_label', 'data_label_source']
        if (hasBaseline) {
            columns.add('baseline')
        }
        if (hasVariablesInfo) {
            columns.add('variable_type')
            columns.add('validation_rules')
        }
        writer.println(columns.join('\t'))
        for (def mapping : mappings) {
            writer.println(mapping.join('\t'))
        }
    }
}
