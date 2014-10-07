package com.thomsonreuters.lsps.transmart.etl.mappings

import com.thomsonreuters.lsps.transmart.etl.LogType
import com.thomsonreuters.lsps.transmart.etl.Logger
import com.thomsonreuters.lsps.transmart.etl.statistic.ValidationRule
import com.thomsonreuters.lsps.transmart.etl.statistic.VariableType
import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

/**
 * Date: 07.10.2014
 * Time: 16:08
 */
class ClinicalDataMapping {
    private static final Logger logger = Logger.getLogger(ClinicalDataMapping.class)

    private Map<String, FileMapping> mappings;

    public static final class Entry {
        String CATEGORY_CD
        int COLUMN
        String DATA_LABEL
        int DATA_LABEL_SOURCE
        String DATA_LABEL_SOURCE_TYPE
        VariableType variableType
        List<ValidationRule> validationRules
    }

    public static final class FileMapping {
        String fileName
        int STUDY_ID
        int SITE_ID
        int SUBJ_ID
        int VISIT_NAME
        List<Entry> _DATA = []
    }

    ClinicalDataMapping(Map<String, FileMapping> mappings) {
        this.mappings = Collections.unmodifiableMap(mappings)
    }

    void eachFileMapping(@ClosureParams(value = SimpleType, options = ['com.thomsonreuters.lsps.transmart.etl.mappings.ClinicalDataMapping.FileMapping']) Closure closure) {
        mappings.values().each(closure)
    }

    public static ClinicalDataMapping loadFromFile(File mappingFile) {
        return new ClinicalDataMapping(processMappingFile(mappingFile))
    }

    private static Object processMappingFile(File f) {
        Map mappings = [:]

        logger.log("Mapping file: ${f.name}")

        CsvLikeFile mappingFile = new CsvLikeFile(f)
        Map<String, Integer> columnMapping = (1..<mappingFile.header.length).collectEntries { [mappingFile.header[it], it] }
        int variableTypeIdx = columnMapping.variable_type ?: -1
        int validationRulesIdx = columnMapping.validation_rules ?: -1
        mappingFile.eachEntry { cols, lineNum ->
            String fileName = cols[0]
            if (!mappings.containsKey(fileName)) {
                mappings[fileName] = new FileMapping(fileName: fileName)
            }

            FileMapping curMapping = mappings[fileName]

            def dataLabel = cols[3]
            if (dataLabel != 'OMIT' && dataLabel != 'DATA_LABEL') {
                def variableType = variableTypeIdx >= 0 ?
                        VariableType.valueOf(cols[variableTypeIdx].capitalize()) :
                        VariableType.Text
                def validationRules = validationRulesIdx ? ValidationRule.parseList(cols[validationRulesIdx]) : []
                if (dataLabel == '\\') {
                    // the actual data label should be taken from a specified column [4]
                    def dataLabelSource = 0
                    def dataLabelSourceType = ''

                    def m = cols[4] =~ /^(\d+)(A|B){0,1}$/
                    if (m.size() > 0) {
                        dataLabelSource = m[0][1].toInteger()
                        dataLabelSourceType = (m[0][2] in ['A', 'B']) ? m[0][2] : 'A'
                    }

                    if (cols[1] && cols[2].toInteger() > 0 && dataLabelSource > 0) {
                        curMapping._DATA.add(new Entry(
                                CATEGORY_CD           : cols[1],
                                COLUMN                : cols[2].toInteger(),
                                DATA_LABEL_SOURCE     : dataLabelSource,
                                DATA_LABEL_SOURCE_TYPE: dataLabelSourceType,
                                variableType: variableType,
                                validationRules: validationRules
                        ))
                    }
                } else {
                    if (curMapping.hasProperty(dataLabel)) {
                        curMapping[dataLabel] = cols[2].toInteger()
                    } else {
                        if (cols[1] && cols[2].toInteger() > 0) {
                            curMapping._DATA.add(new Entry(
                                    DATA_LABEL: dataLabel,
                                    CATEGORY_CD: cols[1],
                                    COLUMN: cols[2].toInteger()
                            ))
                        } else {
                            logger.log(LogType.ERROR, "Category or column number is missing for line ${lineNum}")
                            throw new Exception("Error parsing mapping file")
                        }
                    }
                }
            }
        }

        if (mappings.size() <= 0) {
            logger.log(LogType.ERROR, "Empty mappings file!")
            throw new Exception("Empty mapping file")
        }

        return mappings
    }
}
