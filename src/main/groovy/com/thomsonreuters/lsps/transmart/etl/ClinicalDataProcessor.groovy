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

import com.thomsonreuters.lsps.transmart.etl.mappings.ClinicalDataMapping
import com.thomsonreuters.lsps.transmart.etl.statistic.StatisticCollector
import com.thomsonreuters.lsps.transmart.etl.statistic.TableStatistic
import com.thomsonreuters.lsps.transmart.etl.statistic.VariableType
import com.thomsonreuters.lsps.transmart.files.CsvLikeFile
import com.thomsonreuters.lsps.transmart.sql.DatabaseType
import groovy.sql.Sql

class ClinicalDataProcessor extends DataProcessor {
    StatisticCollector statistic = new StatisticCollector()

    public ClinicalDataProcessor(Object conf) {
        super(conf);
    }

    private long processEachRow(File f, fMappings, Closure<List> processRow) {
        def lineNum = 1L
        def _DATA = fMappings['_DATA']

        CsvLikeFile csvFile = new CsvLikeFile(f, '# ', config.allowNonUniqueColumnNames.asBoolean())
        statistic.collectForTable(f.name) { table ->
            addStatisticVariables(table, csvFile, fMappings)
            csvFile.eachEntry { String[] data ->
                def cols = [''] // to support 0-index properly (we use it for empty data values)
                cols.addAll(Arrays.asList(data))

                lineNum++

                if (cols[fMappings['STUDY_ID']]) {
                    // the line shouldn't be empty

                    if (!cols[fMappings['SUBJ_ID']]) {
                        throw new Exception("SUBJ_ID are not defined at line ${lineNum}")
                    }

                    def output = [
                            study_id       : cols[fMappings['STUDY_ID']],
                            site_id        : cols[fMappings['SITE_ID']],
                            subj_id        : cols[fMappings['SUBJ_ID']],
                            visit_name     : cols[fMappings['VISIT_NAME']],
                            sample_cd      : cols[fMappings['SAMPLE_ID']],
                            data_label     : '', // DATA_LABEL
                            data_value     : '', // DATA_VALUE
                            category_cd    : '', // CATEGORY_CD
                            ctrl_vocab_code: ''  // CTRL_VOCAB_CODE - unused
                    ]

                    if (_DATA) {
                        table.startCollectForRecord()
                        table.collectVariableValue('SUBJ_ID', output.subj_id)
                        _DATA.each { v ->
                            def out = output.clone()
                            int valueColumn = v['COLUMN']
                            String value = cols[valueColumn]
                            if (valueColumn > 0) {
                                table.collectVariableValue(csvFile.header[valueColumn - 1], value)
                            }

                            out['data_value'] = fixColumn(value)
                            def cat_cd = v['CATEGORY_CD']

                            if (v['DATA_LABEL_SOURCE'] > 0) {
                                // ok, the actual data label is in the referenced column
                                out['data_label'] = fixColumn(cols[v['DATA_LABEL_SOURCE']])
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
                                out['data_label'] = fixColumn(v['DATA_LABEL'])
                            }

                            cat_cd = fixColumn(cat_cd)

                            // VISIT_NAME special handling; do it only when VISITNAME is not in category_cd already
                            if (!(cat_cd.contains('VISITNAME') || cat_cd.contains('+VISITNFST'))) {
                                if (config.visitNameFirst) {
                                    cat_cd = cat_cd + '+VISITNFST'
                                }
                            }

                            out['category_cd'] = cat_cd

                            processRow(out)
                        }
                        table.endCollectForRecord()
                    } else {
                        processRow(output)
                        table.collectForRecord(SUBJ_ID: output.subj_id)
                    }
                }
            }
        }
        return lineNum
    }

    @Override
    public boolean processFiles(File dir, Sql sql, studyInfo) {
        // read mapping file first
        // then parse files that are specified there (to allow multiple files per study)

        sql.execute("DELETE FROM lt_src_clinical_data" as String)
        if (!sql.connection.autoCommit) {
            sql.commit()
        }

        dir.eachFileMatch(~/(?i).+_Mapping_File\.txt/) {
            ClinicalDataMapping mapping = ClinicalDataMapping.loadFromFile(it)

            mapping.eachFileMapping { fileMapping ->
                this.processFile(sql, new File(dir, fileMapping.fileName), fileMapping)
            }
            if (isStudyExist(sql,studyInfo)){ throw new Exception("Exist dataset with equal Study Id and other path") }
        }
        new File(dir, "SummaryStatistic.txt").withWriter { writer ->
            statistic.printReport(writer)
        }

        return trySetStudyId(sql, studyInfo)
    }

    private void addStatisticVariables(TableStatistic table, CsvLikeFile csvFile, fMappings) {
        table.withRecordStatisticForVariable('SUBJ_ID', VariableType.ID)
        fMappings._DATA?.each { ClinicalDataMapping.Entry entry ->
            table.withRecordStatisticForVariable(csvFile.header[(entry.COLUMN as int) - 1], entry.variableType, entry.validationRules)
        }
    }

    private void processFile(sql, f, fMappings) {
        config.logger.log("Processing ${f.getName()}")
        if (!f.exists()) {
            config.logger.log("File ${f.getName()} doesn't exist!")
            throw new Exception("File ${f.getName()} doesn't exist")
        }

        if (database?.databaseType == DatabaseType.Postgres) {
            processFileForPostgres(f, fMappings)
        } else {
            processFileForGenericDatabase(sql, f, fMappings)
        }
    }

    private void processFileForPostgres(f, fMappings) {
        DataLoader.start(database, "lt_src_clinical_data", ['STUDY_ID', 'SITE_ID', 'SUBJECT_ID', 'VISIT_NAME',
                                                                                 'DATA_LABEL', 'DATA_VALUE', 'CATEGORY_CD', 'SAMPLE_CD']) {
            st ->
                def lineNum = processEachRow(f, fMappings) { row ->
                    st.addBatch([row.study_id, row.site_id, row.subj_id, row.visit_name, row.data_label,
                                 row.data_value, row.category_cd, row.sample_cd])
                }
                config.logger.log("Processed ${lineNum} rows")
        }
    }

    private void processFileForGenericDatabase(sql, f, fMappings) {
        def lineNum = 0

        sql.withTransaction {
            sql.withBatch(100, """\
					INSERT into lt_src_clinical_data
										(STUDY_ID, SITE_ID, SUBJECT_ID, VISIT_NAME, DATA_LABEL, DATA_VALUE, CATEGORY_CD, SAMPLE_CD)
									VALUES (:study_id, :site_id, :subj_id, :visit_name,
										:data_label, :data_value, :category_cd, :sample_cd)
					""") {
                stmt ->
                    lineNum = processEachRow f, fMappings, {
                        stmt.addBatch(it)
                    }
            }
        }
        sql.commit() // TODO: do we need it here?
        config.logger.log("Processed ${lineNum} rows")
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
            def setVisitNameNotNull = config.setVisitNameNotNull.is(true) ? 'N' : 'Y'
            sql.call("{call " + config.controlSchema + "." + getProcedureName() + "(?,?,?,?,?,?)}", [studyId, studyNode, config.securitySymbol, highlightFlag, setVisitNameNotNull, jobId])
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

        return res
    }

}
