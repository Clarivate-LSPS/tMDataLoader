package com.thomsonreuters.lsps.transmart.etl.statistic
/**
 * Date: 06.10.2014
 * Time: 16:39
 */
enum VariableType {
    ID,
    Text,
    Categorical,
    Numerical,
    Timepoint,
    Date,
    Timestamp,
    Tag;

    public static VariableType tryParse(String variableType, VariableType defaultType) {
        if (!variableType) {
            return defaultType
        }
        try {
            return VariableType.valueOf(variableType.capitalize())
        } catch (Exception ignored) {
            return defaultType
        }
    }
}