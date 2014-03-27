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

import groovy.sql.Sql

class SNPDataProcessor extends DataProcessor {

    public SNPDataProcessor(Object conf) {
        super(conf);
    }

    @Override
    public boolean processFiles(File dir, Sql sql, Object studyInfo) {
        sql.execute('TRUNCATE TABLE tm_lz.lt_src_mrna_subj_samp_map')
        sql.execute('TRUNCATE TABLE tm_lz.lt_src_mrna_data')

        sql.execute('TRUNCATE TABLE tm_lz.lt_snp_calls_by_gsm')
        sql.execute('TRUNCATE TABLE tm_lz.lt_snp_copy_number')

        def platformList = [] as Set

        dir.eachFileMatch(~/(?i).+_Subject_Sample_Mapping_File(_GPL\d+)*\.txt/) {
            platformList.addAll(processMappingFile(it, sql, studyInfo))
        }

        platformList = platformList.toList()

        if (platformList.size() > 0) {
            loadPlatforms(dir, sql, platformList, studyInfo)

            /* dir.eachFileMatch(~/(?i).+_Gene_Expression_Data_[RLTZ](_GPL\d+)*\.txt/) {
                 processExpressionFile(it, sql, studyInfo)
             }*/

            // Load data to tmp tables TM_LZ.LT_SNP_CALLS_BY_GSM  and TM_LZ.LT_SNP_COPY_NUMBER
            def callsFileList = studyInfo['callsFileNameList'] as List
            if (callsFileList.size() > 0) {
                callsFileList.each { String name ->
                    processSnpCallsFile(sql, new File(dir as File, name))
                }
            }

            def copyNumberFileList = studyInfo['copyNumberFileList'] as List
            if (copyNumberFileList.size() > 0) {
                copyNumberFileList.each { String name ->
                    processSnpCopyNumberFile(sql, new File(dir as File, name))
                }
            }

        } else {
            throw new Exception("No platforms defined")
        }

        return true;
    }

    private void processSnpCallsFile(Sql sql, File f) {
        config.logger.log(LogType.MESSAGE, "Processing calls for ${f.getName()}")
        loadFileToTable(sql, f, 'tm_lz.lt_snp_calls_by_gsm', ['GSM_NUM', 'SNP_NAME', 'SNP_CALLS'])
    }

    private void processSnpCopyNumberFile(Sql sql, File f) {
        config.logger.log(LogType.MESSAGE, "Processing copy number for ${f.getName()}")
        loadFileToTable(sql, f, 'tm_lz.lt_snp_copy_number',
                ['GSM_NUM', 'SNP_NAME', 'CHROM', 'CHROM_POS', 'COPY_NUMBER']) {
            [it[0], it[1], it[2], it[3], it[4] as double]
        }
    }

    private void loadFileToTable(Sql sql, File f, String table, columns, Closure prepareEntry = Closure.IDENTITY) {
        String insertCommand = "insert into ${table}(${columns.join(',')}) values (${columns.collect { '?' }.join(',')})"
        long lineNum = 0
        sql.withBatch(500, insertCommand) { stmt ->
            f.splitEachLine('\t') { entry ->
                lineNum++
                if (lineNum <= 1) {
                    return
                }
                config.logger.log(LogType.PROGRESS, "[${lineNum}]")
                stmt.addBatch(prepareEntry(entry))
            }
        }
        config.logger.log(LogType.PROGRESS, '')
        sql.commit()
    }

    @Override
    public boolean runStoredProcedures(Object jobId, Sql sql, Object studyInfo) {
        def studyId = studyInfo['id']
        def studyNode = studyInfo['node']
        def studyDataType = studyInfo['datatype']

        if (studyDataType == 'T' && !config.useT) {
            config.logger.log("Original DataType='T', but using 'Z' instead (workaround); use -t option to alter this behavior")
            studyDataType = 'Z' // temporary workaround due to a bug in Transmart
        }

        if (studyId && studyNode && studyDataType) {
            config.logger.log("Study ID=${studyId}; Node=${studyNode}; Data Type=${studyDataType}")

            if (studyInfo['runPlatformLoad']) {
                sql.call("{call " + config.controlSchema + ".i2b2_load_annotation_deapp()}")
            }

            sql.call("{call " + config.controlSchema + ".i2b2_process_snp_data (?, ?, ?, null, null, '" + config.securitySymbol + "', ?, ?)}",
                    [studyId, studyNode, studyDataType, jobId, Sql.NUMERIC]) {}
        } else {
            config.logger.log(LogType.ERROR, "Study ID or Node or DataType not defined!")
            return false;
        }
        return true;
    }

    @Override
    public String getProcedureName() {
        return "I2B2_PROCESS_SNP_DATA";
    }

    private List processMappingFile(File f, Sql sql, studyInfo) {
        def platformList = [] as Set
        def studyIdList = [] as Set
        def callsFileList = [] as Set
        def copyNumberFileList = [] as Set

        config.logger.log("Mapping file: ${f.name}")

        def lineNum = 0

        sql.withTransaction {
            sql.withBatch(100, """\
				INSERT into tm_lz.lt_src_mrna_subj_samp_map (TRIAL_NAME, SITE_ID, 
					SUBJECT_ID, SAMPLE_CD, PLATFORM, TISSUE_TYPE, 
					ATTRIBUTE_1, ATTRIBUTE_2, CATEGORY_CD, SOURCE_CD) 
				VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'STD')
		""") {
                stmt ->

                    f.splitEachLine("\t") {
                        cols ->

                            lineNum++
                            // cols:0:calls_file_name, 1:copy_number_file_name, 2:study_id, 3:site_id, 4:subject_id,
                            // 5:sample_cd, 6:platform, 7:tissuetype, 8:attr1, 9:attr2, 10:category_cd

                            if (cols[2] && lineNum > 1) {
                                if (!(cols[0] || cols[1])) {
                                    throw new Exception("Incorrect mapping file: calls_file_name or copy_number_file_name is empty")
                                }

                                if (!(cols[4] && cols[5] && cols[6] && cols[10]))
                                    throw new Exception("Incorrect mapping file: mandatory columns not defined")

                                if (cols[0]) {
                                    callsFileList << cols[0]
                                }
                                if (cols[1]) {
                                    copyNumberFileList << cols[1]
                                }
                                platformList << cols[6]
                                studyIdList << cols[2]

                                stmt.addBatch(cols[2..-1])
                            }
                    }
            }
        }

        studyIdList = studyIdList.toList()
        platformList = platformList.toList()

        sql.commit()
        config.logger.log("Processed ${lineNum} rows")

        if (studyIdList.size() > 0) {
            if (studyIdList.size() > 1) {
                throw new Exception("Multiple studies in one mapping file")
            } else {
                def studyId = studyIdList[0]
                if (studyInfo['id'] && studyId != studyInfo['id']) {
                    throw new Exception("Study ID doesn't match clinical data")
                } else {
                    studyInfo['id'] = studyId
                }
            }
        }

        studyInfo['datatype'] = 'L'
        studyInfo['callsFileNameList'] = callsFileList.toList()
        studyInfo['copyNumberFileList'] = copyNumberFileList.toList()

        return platformList
    }

    private void loadSNPGeneMap(Sql sql, File platformFile, String platform) {
        config.logger.log('Loading SNP Gene Map')
        sql.execute('truncate table tm_lz.lt_snp_gene_map')
        config.logger.log('Processing platform file')
        sql.withBatch(500, 'insert into tm_lz.lt_snp_gene_map (snp_name, entrez_gene_id) values (?, ?)') {
            st ->
                eachPlatformEntry(platformFile) {
                    entry ->
                        st.addBatch([entry.probeset_id, entry.entrez_gene_id as long])
                }
        }
        config.logger.log('Updating SNP Gene Map in database')
        sql.execute("""
            insert into deapp.de_snp_gene_map
            (snp_name, entrez_gene_id)
            select t.snp_name, t.entrez_gene_id
            from tm_lz.lt_snp_gene_map t
            left join deapp.de_snp_gene_map gm
            on gm.snp_name = t.snp_name
            where gm.snp_name is null
        """)
        config.logger.log('SNP Gene Map loaded')
        sql.commit()
    }

    private void loadPlatforms(File dir, Sql sql, List platformList, studyInfo) {
        platformList.each {
            String platform ->
                File platformFile = new File(dir, "${platform}.txt")
                loadSNPGeneMap(sql, platformFile, platform)
                loadPlatform(sql, platformFile, platform, studyInfo)
        }
    }

    private void loadPlatform(Sql sql, File platformFile, String platform, studyInfo) {
        sql.execute('TRUNCATE TABLE tm_lz.lt_src_deapp_annot')

        def row = sql.firstRow("SELECT count(*) as cnt FROM " + config.controlSchema + ".annotation_deapp WHERE gpl_id=?",
                [platform])
        if (!row?.cnt) {
            // platform is not defined, loading
            config.logger.log("Loading platform: ${platform}")
            if (!platformFile.exists()) throw new Exception("Platform file not found: ${platformFile.name}")

            def platformTitle
            def platformOrganism

            row = sql.firstRow("select title, organism from deapp.de_gpl_info where platform=${platform}")
            if (!row) {

                config.logger.log("Fetching platform description from GEO")
                def txt = "http://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=${platform}".toURL().getText()

                def m = txt =~ /Title\<\/td\>\s*?\<td.*?\>(?:\[.+?\]\s*)*(.+?)\<\/td\>/
                if (m[0]) {
                    platformTitle = m[0][1]
                }

                m = txt =~ /Organism\<\/td\>\s*?\<td.*?\>\<a.+?\>(.+?)\<\/a\>/
                if (m[0]) {
                    platformOrganism = m[0][1]
                }

                if (platformTitle && platformOrganism) {
                    sql.execute("""\
							INSERT into deapp.de_gpl_info (PLATFORM, TITLE, ORGANISM, ANNOTATION_DATE, MARKER_TYPE)
							VALUES (?, ?, ?, current_timestamp, 'Gene Expression')
						""", [platform, platformTitle, platformOrganism])
                } else {
                    throw new Exception("Cannot fetch platform title & organism for ${platform}")
                }
            } else {
                platformTitle = row.title
                platformOrganism = row.organism
            }

            config.logger.log("Platform: ${platformTitle} (${platformOrganism})")

            def lineNum = 0
            def isEmpty = true

            sql.withTransaction {
                sql.withBatch(500, """\
						INSERT into tm_lz.lt_src_deapp_annot (GPL_ID,PROBE_ID,GENE_SYMBOL,GENE_ID,ORGANISM)
						VALUES (?, ?, ?, ?, ?)
				""") {
                    stmt ->
                        lineNum = eachPlatformEntry(platformFile) {
                            entry ->
                                // line with data
                                isEmpty = false
                                stmt.addBatch([
                                        platform,
                                        entry.probeset_id,
                                        entry.gene_symbol,
                                        entry.entrez_gene_id,
                                        entry.species ?: platformOrganism
                                ])
                        }
                }
            }

            if (isEmpty) throw new Exception("Platform file doesn't contain any EntrezGene IDs")

            sql.commit()
            config.logger.log("Finished loading platform ${platform}, processed ${lineNum} rows")

            studyInfo['runPlatformLoad'] = true
        }
    }

    private long eachPlatformEntry(File platformFile, Closure processEntry) {
        long lineNum = 0
        def header_mappings = [:]
        platformFile.splitEachLine("\t") {
            cols ->
                lineNum++

                if (!cols[0] || cols[0] ==~ /\s*?#.+/) return // skip empty or comment lines

                if (!header_mappings) {
                    // first line is the header if header mappings are not defined yet
                    cols.eachWithIndex {
                        val, idx ->

                            if (val ==~ /(?i)(ENTREZ[\s_]*)*GENE([\s_]*ID)*/) header_mappings['entrez_gene_id'] = idx
                            if (val ==~ /(?i)(GENE[\s_]*)*SYMBOL/) header_mappings['gene_symbol'] = idx
                            if (val ==~ /(?i)SPECIES([\s_]*SCIENTIFIC)([\s_]*NAME)/) header_mappings['species'] = idx
                    }

                    if (!header_mappings['species']) {
                        // OK, trying to get species from the description
                        config.logger.log(LogType.WARNING, "Species not found in the platform file, using description")
                    }

                    if (header_mappings['entrez_gene_id']
                            && header_mappings['gene_symbol']
                    ) {

                        config.logger.log(LogType.DEBUG, "ENTREZ, SYMBOL, SPECIES => " +
                                "${cols[header_mappings['entrez_gene_id']]}, " +
                                "${cols[header_mappings['gene_symbol']]}, " +
                                "${header_mappings.containsKey('species') ? cols[header_mappings['species']] : '(Not specified)'}")

                    } else {
                        throw new Exception("Incorrect platform file header")
                    }
                } else if (cols[header_mappings['entrez_gene_id']] ==~ /\d+/) {
                    config.logger.log(LogType.PROGRESS, "[${lineNum}]")
                    processEntry([
                            probeset_id: cols[0],
                            gene_symbol: cols[header_mappings['gene_symbol']],
                            entrez_gene_id: cols[header_mappings['entrez_gene_id']],
                            species: header_mappings.containsKey('species') ? cols[header_mappings['species']] : null
                    ])
                }
        }
        config.logger.log(LogType.PROGRESS, "")
        return lineNum
    }
}
