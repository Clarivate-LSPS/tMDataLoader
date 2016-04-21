/*************************************************************************
 * tranSMART Data Loader - ETL tool for tranSMART
 *
 * Copyright 2012-2013 Thomson Reuters
 *
 * This product includes software developed at Thomson Reuters
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License 
 * as published by the Free Software  
 * Foundation, either version 3 of the License, or (at your option) any later version, along with the following terms:
 * 1.	You may convey a work based on this program in accordance with section 5, provided that you retain the above notices.
 * 2.	You may convey verbatim copies of this program code as you receive it, in any medium, provided that you retain the above notices.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS    * FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *
 ******************************************************************/

package com.thomsonreuters.lsps.transmart.etl

import com.thomsonreuters.lsps.db.core.DatabaseType
import com.thomsonreuters.lsps.db.loader.DataLoader
import com.thomsonreuters.lsps.transmart.etl.mappings.ClinicalDataMapping
import com.thomsonreuters.lsps.transmart.etl.mappings.TagReplacer
import com.thomsonreuters.lsps.transmart.etl.statistic.StatisticCollector
import com.thomsonreuters.lsps.transmart.etl.statistic.TableStatistic
import com.thomsonreuters.lsps.transmart.etl.statistic.VariableType
import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import com.thomsonreuters.lsps.transmart.files.MetaInfoHeader
import groovy.sql.Sql
import groovy.transform.CompileStatic

import java.nio.file.Files
import java.nio.file.Path
import java.sql.SQLException

class ClinicalDataProcessor extends AbstractDataProcessor {
    StatisticCollector statistic = new StatisticCollector()
    def usedStudyId = ''

    public ClinicalDataProcessor(Object conf) {
        super(conf);
    }

    @CompileStatic
    private long processEachRow(Path f, ClinicalDataMapping.FileMapping fMappings, Closure<List> processRow) {
        def processedCount = 0L
        def _DATA = fMappings._DATA
        //Custom tags
        def tagReplacer = TagReplacer.fromFileMapping(fMappings)
        CsvLikeFile csvFile = new CsvLikeFile(f, '# ', config.allowNonUniqueColumnNames.asBoolean())
        statistic.collectForTable(f.fileName.toString()) { table ->
            addStatisticVariables(table, csvFile, fMappings)
            csvFile.eachEntry { it, lineNumber ->
                String[] data = (String[]) it
                String[] cols = new String[data.length + 1]
                cols[0] = '' // to support 0-index properly (we use it for empty data values)
                System.arraycopy(data, 0, cols, 1, data.length)

                processedCount++

                if (cols[fMappings.STUDY_ID]) {
                    // the line shouldn't be empty

                    if (!cols[fMappings.SUBJ_ID]) {
                        throw new Exception("SUBJ_ID are not defined at line ${lineNumber}")
                    }

                    if (!usedStudyId) {
                        usedStudyId = cols[fMappings.STUDY_ID]
                    }

                    if (usedStudyId != cols[fMappings.STUDY_ID]) {
                        throw new DataProcessingException("STUDY_ID differs from previous in ${lineNumber} line in ${csvFile.file.fileName} file.")
                    }

                    Map<String, String> output = [
                            study_id       : cols[fMappings.STUDY_ID],
                            site_id        : cols[fMappings.SITE_ID],
                            subj_id        : cols[fMappings.SUBJ_ID],
                            visit_name     : cols[fMappings.VISIT_NAME],
                            sample_cd      : cols[fMappings.SAMPLE_ID],
                            data_label     : '', // DATA_LABEL
                            data_value     : '', // DATA_VALUE
                            category_cd    : '', // CATEGORY_CD
                            ctrl_vocab_code: '', // CTRL_VOCAB_CODE - unused
                            valuetype_cd   : (String) null,
                    ]

                    if (_DATA) {
                        table.startCollectForRecord()
                        table.collectVariableValue('SUBJ_ID', output.subj_id)
                        for (def v : _DATA) {
                            int valueColumn = v.COLUMN
                            String value = cols[valueColumn]
                            if (valueColumn > 0) {
                                table.collectVariableValue(csvFile.header[valueColumn - 1], value)
                            }
                            if (v['CATEGORY_CD'] != '') {
                                def out = output.clone()
                                out['data_value'] = fixColumn(value)
                                if (v.variableType == VariableType.Timepoint) {
                                    out['valuetype_cd'] = v.variableType.name().toUpperCase()
                                }
                                def cat_cd = v.CATEGORY_CD
                                cat_cd = tagReplacer.replaceTags(cat_cd, cols)
                                if (!cat_cd) {
                                    continue
                                }

                                if (v.DATA_LABEL_SOURCE > 0) {
                                    // ok, the actual data label is in the referenced column
                                    out['data_label'] = fixColumn(cols[v.DATA_LABEL_SOURCE])
                                    // now need to modify CATEGORY_CD before proceeding

                                    // handling DATALABEL in category_cd
                                    if (!cat_cd.contains('DATALABEL')) {
                                        // do this only if category_cd doesn't contain DATALABEL yet
                                        if (v['DATA_LABEL_SOURCE_TYPE'] == 'A')
                                            cat_cd = (cat_cd =~ /^(.+)\+([^\+]+?)$/).replaceFirst('$1+DATALABEL+$2')
                                        else
                                            cat_cd = cat_cd + '+DATALABEL'
                                    }

                                } else {
                                    out['data_label'] = fixColumn(v.DATA_LABEL)
                                }

                                cat_cd = fixColumn(cat_cd)

                                // VISIT_NAME special handling; do it only when VISITNAME is not in category_cd already
                                if (fMappings.VISIT_NAME > 0 && !cat_cd.endsWith('+$') && !(cat_cd.contains('VISITNAME') || cat_cd.contains('+VISITNFST'))) {
                                    if (config.visitNameFirst) {
                                        cat_cd = cat_cd + '+VISITNFST'
                                    }
                                }

                                out['category_cd'] = cat_cd

                                processRow(out, lineNumber)
                            }
                        }
                        table.endCollectForRecord()
                    } else {
                        processRow(output, lineNumber)
                        table.collectForRecord(SUBJ_ID: output.subj_id)
                    }
                }
                it
            }
        }
        return processedCount
    }

    @Override
    public boolean processFiles(Path dir, Sql sql, studyInfo) {
        // read mapping file first
        // then parse files that are specified there (to allow multiple files per study)
        def mappingFileFound = false

        database.truncateTable(sql, 'lt_src_clinical_data')
        if (!sql.connection.autoCommit) {
            sql.commit()
        }
        def tableName = 'lt_src_clinical_data'
        if (database.databaseType == DatabaseType.Oracle) {
            tableName = tableName.toUpperCase()
        }
        def meta = sql.connection.metaData, cols = meta.getColumns(null, null, tableName, null)
        def colsMetaSize = [:]
        while (cols.next())
            colsMetaSize.put(cols.getString('column_name').toUpperCase(), Integer.parseInt(cols.getString('column_size')))

        dir.eachFileMatch(~/(?i).+_Mapping_File\.txt/) {
            CsvLikeFile mappingFile = new CsvLikeFile(it, '#')
            ClinicalDataMapping mapping = ClinicalDataMapping.loadFromCsvLikeFile(mappingFile, colsMetaSize)

            mergeMode = getMergeMode(mappingFile)

            mapping.eachFileMapping { fileMapping ->
                this.processFile(sql, dir.resolve(fileMapping.fileName), fileMapping)
            }
            mappingFileFound = true
        }
        if (!mappingFileFound) {
            throw new DataProcessingException("Mapping file wasn\'t found. Please, check file name.")
        }
        dir.resolve("SummaryStatistic.txt").withWriter { writer ->
            statistic.printReport(writer)
        }

        if (!trySetStudyId(sql, studyInfo)) {
            return false
        }
        checkStudyExist(sql, studyInfo)
        return true
    }

    private MergeMode getMergeMode(CsvLikeFile mappingFile) {
        def metaInfo = (mappingFile as MetaInfoHeader).metaInfo
        String modeName = metaInfo.MERGE_MODE

        if (!modeName)
            return MergeMode.REPLACE

        return MergeMode.valueOf(modeName)
    }

    private void addStatisticVariables(TableStatistic table, CsvLikeFile csvFile, ClinicalDataMapping.FileMapping fMappings) {
        table.withRecordStatisticForVariable('SUBJ_ID', VariableType.ID)
        fMappings._DATA?.each { ClinicalDataMapping.Entry entry ->
            table.withRecordStatisticForVariable(csvFile.header[(entry.COLUMN as int) - 1], entry.variableType, entry.validationRules)
        }
    }

    private void processFile(sql, Path f, ClinicalDataMapping.FileMapping fileMapping) {
        config.logger.log("Processing ${f.fileName}")
        if (Files.notExists(f)) {
            config.logger.log("File ${f.fileName} doesn't exist!")
            throw new DataProcessingException("File ${f.fileName} doesn't exist")
        }

        processFile(f, fileMapping)
    }

    private void processFile(Path f, ClinicalDataMapping.FileMapping fileMapping) {
        DataLoader.start(database, "lt_src_clinical_data", ['STUDY_ID', 'SITE_ID', 'SUBJECT_ID', 'VISIT_NAME',
                                                            'DATA_LABEL', 'DATA_VALUE', 'CATEGORY_CD', 'SAMPLE_CD',
                                                            'VALUETYPE_CD']) { st ->
            long rowsCount = processEachRow(f, fileMapping) { row, lineNumber ->
                try {
                    st.addBatch([row.study_id, row.site_id, row.subj_id, row.visit_name, row.data_label,
                                 row.data_value, row.category_cd, row.sample_cd, row.valuetype_cd])
                } catch (SQLException e) {
                    throw new DataProcessingException("Wrong data close to ${lineNumber} line.\n ${e.getLocalizedMessage()}")
                }
            }
            config.logger.log("Processed ${rowsCount} rows")
        }
    }

    private boolean trySetStudyId(Sql sql, studyInfo) {
// OK, now we need to retrieve studyID & node
        def rows = sql.rows("select study_id, count(*) as cnt from lt_src_clinical_data group by study_id" as String)
        def rsize = rows.size()

        if (rsize > 0) {
            if (rsize == 1) {
                def studyId = rows[0].study_id
                if (studyId) {
                    studyInfo['id'] = studyId
                } else {
                    config.logger.log(LogType.ERROR, "Study ID is null!")
                    return false
                }
            } else {
                config.logger.log(LogType.ERROR, "Multiple StudyIDs are detected!")
                return false
            }
        } else {
            config.logger.log(LogType.ERROR, "Study ID is not specified!")
            return false
        }

        return true;
    }

    @Override
    public String getProcedureName() {
        return config.altClinicalProcName ?: "I2B2_LOAD_CLINICAL_DATA"
    }

    @Override
    public boolean runStoredProcedures(jobId, Sql sql, studyInfo) {
        def studyId = studyInfo['id']
        def studyNode = studyInfo['node']
        if (studyId && studyNode) {
            config.logger.log("Study ID=${studyId}; Node=${studyNode}")
            def highlightFlag = config.highlightClinicalData.is(true) ? 'Y' : 'N'
            def alwaysSetVisitName = config.alwaysSetVisitName.is(true) ? 'Y' : 'N'
            sql.call("{call " + config.controlSchema + "." + getProcedureName() + "(?,?,?,?,?,?,?)}", [studyId, studyNode, config.securitySymbol, highlightFlag, alwaysSetVisitName, jobId, mergeMode.name()])
        } else {
            config.logger.log(LogType.ERROR, "Study ID or Node not defined!")
            return false;
        }

        return true;
    }

    private String fixColumn(String s) {
        if (s == null) return '';

        def res = s.trim()
        res = (res =~ /^\"(.+)\"$/).replaceFirst('$1')
        res = res.replace('\\', '')
        res = res.replace('%', 'PCT')
        res = res.replace('*', '')
        res = res.replace('&', ' and ')
        res = res.replaceAll('[^\\p{ASCII}]', '')

        return res
    }

}
