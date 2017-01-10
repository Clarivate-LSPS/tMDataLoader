package com.thomsonreuters.lsps.transmart.etl.mappings

import com.thomsonreuters.lsps.transmart.etl.DataProcessingException
import com.thomsonreuters.lsps.transmart.etl.LogType
import com.thomsonreuters.lsps.transmart.etl.Logger
import com.thomsonreuters.lsps.transmart.etl.statistic.ValidationRule
import com.thomsonreuters.lsps.transmart.etl.statistic.VariableType
import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import groovy.transform.stc.ClosureParams
import groovy.transform.stc.SimpleType

import java.nio.file.Files
import java.nio.file.Path

/**
 * Date: 07.10.2014
 * Time: 16:08
 */
class ClinicalDataMapping {
    private static final Logger logger = Logger.getLogger(ClinicalDataMapping.class)

    private Map<String, FileMapping> mappings

    public static final class Entry {
        String CATEGORY_CD
        int COLUMN
        String DATA_LABEL
        int DATA_LABEL_SOURCE
        String DATA_LABEL_SOURCE_TYPE
        VariableType variableType
        List<ValidationRule> validationRules
        String baseline
        int baselineColumn
    }

    public static final class FileMapping {
        String fileName
        int STUDY_ID
        int SITE_ID
        int SUBJ_ID
        int VISIT_NAME
        int SAMPLE_ID
        List<Entry> _DATA = []
    }

    ClinicalDataMapping(Map<String, FileMapping> mappings) {
        this.mappings = Collections.unmodifiableMap(mappings)
    }

    void eachFileMapping(
            @ClosureParams(value = SimpleType, options = ['com.thomsonreuters.lsps.transmart.etl.mappings.ClinicalDataMapping.FileMapping']) Closure closure) {
        mappings.values().each(closure)
    }

    public static ClinicalDataMapping loadFromFile(Path f, Map colsMetaSize) {
        loadFromCsvLikeFile(new CsvLikeFile(f, "#"), colsMetaSize)
    }

    public static ClinicalDataMapping loadFromCsvLikeFile(CsvLikeFile mappingFile, Map colsMetaSize) {
        new ClinicalDataMapping(processMappingFile(mappingFile, colsMetaSize))
    }

    private static class FileParsingInfo {
        FileMapping fileMapping
        Set<Integer> mappedColumns = new HashSet<>()
        int actualColumnsCount = -1
    }

    private static Map<String, FileMapping> processMappingFile(CsvLikeFile mappingFile, Map<String, Integer> colsMetaSize) {
        Map<String, FileParsingInfo> mappings = [:]

        logger.log("Mapping file: ${mappingFile.file.fileName}")

        List<String> mappingErrors = []
        List<String> mappingWarnings = []
        Map<String, Integer> columnMapping = (1..<mappingFile.header.length).collectEntries {
            [mappingFile.header[it], it]
        }
        int variableTypeIdx = columnMapping.variable_type ?: -1
        int validationRulesIdx = columnMapping.validation_rules ?: -1
        boolean hasBaselineColumn = columnMapping.containsKey('baseline')
        mappingFile.eachEntry { cols, lineNum ->
            String fileName = cols[0]
            FileParsingInfo parsingInfo = mappings[fileName]
            if (!parsingInfo) {
                mappings[fileName] = parsingInfo = new FileParsingInfo(fileMapping: new FileMapping(fileName: fileName))
                Path dataFile = mappingFile.file.resolveSibling(fileName)
                if (Files.exists(dataFile)) {
                    parsingInfo.actualColumnsCount = new CsvLikeFile(dataFile, '# ').header.size()
                } else {
                    mappingErrors.add("File '$fileName' doesn't exists")
                }
            }

            FileMapping curMapping = parsingInfo.fileMapping
            def dataLabel = cols[3]
            if (dataLabel != 'OMIT' && dataLabel != 'DATA_LABEL') {
                def variableType = variableTypeIdx >= 0 ?
                        VariableType.tryParse(cols[variableTypeIdx].capitalize(), VariableType.Text) :
                        VariableType.Text
                def validationRules = validationRulesIdx >= 0 ? ValidationRule.parseList(cols[validationRulesIdx]) : []
                Integer columnIndex
                try {
                    columnIndex = (cols[2] as String).toInteger()
                } catch (NumberFormatException ex) {
                    mappingErrors.add("Invalid or empty column index '${cols[2]}' for row: ${cols}")
                    return
                }
                if (!parsingInfo.mappedColumns.add(columnIndex)) {
                    mappingWarnings.add("Column index '${columnIndex}' is already mapped in other row for row: ${cols}")
                }
                if (columnIndex <= 0 || (parsingInfo.actualColumnsCount >= 0 && columnIndex > parsingInfo.actualColumnsCount)) {
                    mappingErrors.add("Column index '${columnIndex}' is out of bounds of data file columns (1-$parsingInfo.actualColumnsCount) for row: ${cols}")
                    return
                }
                if (curMapping.hasProperty(dataLabel)) {
                    curMapping[dataLabel] = columnIndex
                } else {
                    def entry = new Entry(
                            CATEGORY_CD: cols[1],
                            COLUMN: columnIndex,
                            variableType: variableType,
                            validationRules: validationRules
                    )
                    if (hasBaselineColumn) {
                        entry.baseline = cols[columnMapping['baseline']]
                    }
                    if (entry.CATEGORY_CD.length() > colsMetaSize.CATEGORY_CD) {
                        mappingErrors.add("CATEGORY_CD is too long (${entry.CATEGORY_CD.length()} > ${colsMetaSize.CATEGORY_CD}) for row [$lineNum]: ${cols}")
                        return
                    }
                    if (dataLabel == '\\') {
                        if (!entry.CATEGORY_CD) {
                            mappingErrors.add("CATEGORY_CD wasn't specified for variable with DATA_LABEL_SOURCE for row [$lineNum]: ${cols}")
                            return
                        }

                        def m = cols[4] =~ /^(\d+)(A|B){0,1}$/
                        if (!m) {
                            mappingErrors.add("Invalid data_label_source '${cols[4]}' for row [$lineNum]: ${cols}")
                            return
                        }
                        def dataLabelSource = m[0][1].toInteger()
                        def dataLabelSourceType = (m[0][2] in ['A', 'B']) ? m[0][2] : 'A'

                        if (dataLabelSource <= 0 || (parsingInfo.actualColumnsCount >= 0 && dataLabelSource > parsingInfo.actualColumnsCount)) {
                            mappingErrors.add("Data label source column index '${dataLabelSource}' is out of bounds of data file columns (1-$parsingInfo.actualColumnsCount) for row [$lineNum]: ${cols}")
                            return
                        }

                        entry.DATA_LABEL_SOURCE = dataLabelSource
                        entry.DATA_LABEL_SOURCE_TYPE = dataLabelSourceType
                    } else {
                        entry.DATA_LABEL = dataLabel
                    }
                    curMapping._DATA.add(entry)
                }
            }
        }

        if (hasBaselineColumn) {
            for (def entry : mappings.entrySet()) {
                entry.value.fileMapping._DATA.each { e ->
                    e.baselineColumn = entry.value.fileMapping._DATA.find {
                        it.DATA_LABEL == e.baseline
                    }?.COLUMN ?: -1
                }
            }
        }

        if (mappingErrors.size() > 0) {
            logger.logAndThrow(new DataProcessingException("Mapping file '${mappingFile.file.fileName}' has errors:\n${mappingErrors.join('\n')}"))
        }

        for (def warning : mappingWarnings) {
            logger.log(LogType.WARNING, warning)
        }

        if (mappings.size() <= 0) {
            logger.log(LogType.ERROR, "Empty mappings file!")
            throw new DataProcessingException("Empty mapping file")
        }

        Map<String, FileMapping> result = [:]
        for (def entry : mappings.entrySet()) {
            result.put(entry.key, entry.value.fileMapping)
        }
        return result
    }
}
