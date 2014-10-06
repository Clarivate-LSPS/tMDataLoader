package com.thomsonreuters.lsps.transmart.etl.statistic

/**
 * Date: 06.10.2014
 * Time: 14:58
 */
class VariableStatistic {
    String name
    VariableType type
    long notEmptyValuesCount
    long emptyValuesCount
    boolean required
    boolean unique

    void setType(VariableType type) {
        this.type = type
        this.unique = this.unique || type == VariableType.ID
        this.required = this.required || type == VariableType.ID
    }

    boolean getHasMissingData() {
        return emptyValuesCount > 0
    }

    String getQCMissingData() {
        required ? (hasMissingData ? "${emptyValuesCount} missing (<id list>)" : 'OK') : ''
    }

    void collectValue(String id, String value) {
        if (!value.isEmpty()) {
            notEmptyValuesCount++
        } else {
            emptyValuesCount++
        }
    }
}
