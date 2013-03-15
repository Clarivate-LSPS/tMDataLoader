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

import java.io.File
import groovy.sql.Sql

class ClinicalDataProcessor extends DataProcessor {

	public ClinicalDataProcessor(Object conf) {
		super(conf);
	}

	@Override
	public boolean processFiles(File dir, Sql sql, studyInfo) {
		// read mapping file first
		// then parse files that are specified there (to allow multiple files per study)
		
		sql.execute('TRUNCATE TABLE tm_lz.lt_src_clinical_data')
		
		dir.eachFileMatch(~/(?i).+_Mapping_File\.txt/) {
			def mappings = processMappingFile(it)
			
			if (mappings.size() <= 0) {
				config.logger.log(LogType.ERROR, "Empty mappings file!")
				throw new Exception("Empty mapping file")
			}  
			
			mappings.each {
				fName, fMappings ->
				
				config.logger.log("Processing ${fName}")
				def f = new File(dir, fName)
				if (! f.exists() ) {
					config.logger.log("File ${fName} doesn't exist!")
					throw new Exception("File doesn't exist")
				}
				
				def lineNum = 0
				
				sql.withTransaction {
					sql.withBatch(100, """\
					INSERT into lt_src_clinical_data 
										(STUDY_ID, SITE_ID, SUBJECT_ID, VISIT_NAME, DATA_LABEL, DATA_VALUE, CATEGORY_CD)
									VALUES (:study_id, :site_id, :subj_id, :visit_name, 
										:data_label, :data_value, :category_cd)
					""") {
						stmt ->
					
						f.splitEachLine("\t") {		
							def cols = [''] // to support 0-index properly (we use it for empty data values)
							cols.addAll(it)
							
							lineNum++
							if (lineNum < 2) return; // skipping header
							
							if (cols[fMappings['STUDY_ID']]) {
								// the line shouldn't be empty
								
								if (! cols[fMappings['SUBJ_ID']]) {
									throw new Exception("SUBJ_ID are not defined at line ${lineNum}")
								}
							
								def output = [
									study_id : cols[fMappings['STUDY_ID']],
									site_id : cols[fMappings['SITE_ID']],
									subj_id : cols[fMappings['SUBJ_ID']],
									visit_name : cols[fMappings['VISIT_NAME']],
									data_label : '', // DATA_LABEL
									data_value : '', // DATA_VALUE
									category_cd : '', // CATEGORY_CD
									ctrl_vocab_code : ''  // CTRL_VOCAB_CODE - unused
								]
								
								if (fMappings['_DATA']) {
									fMappings['_DATA'].each { 
										k, v ->
										
										def out = output.clone()
										out['data_label'] = fixColumn(k)
										out['data_value'] = fixColumn( cols[v['COLUMN']] )		
										out['category_cd'] = fixColumn(v['CATEGORY_CD'])		
										
										stmt.addBatch(out) 
									}
								}
								else {
									stmt.addBatch(output)
								}
							
							}
							
						}
						
					
					}
				}
				
				config.logger.log("Processed ${lineNum} rows")
				sql.commit() // TODO: do we need it here?
			}
			
		}
		
		// OK, now we need to retrieve studyID & node
		def rows = sql.rows("select study_id, count(*) as cnt from lt_src_clinical_data group by study_id")
		def rsize = rows.size()
		
		if (rsize > 0) {
			if (rsize == 1) {
				def studyId = rows[0].study_id
				if (studyId) {
					studyInfo['id'] = studyId 
				}
				else {
					config.logger.log(LogType.ERROR, "Study ID is null!")
					return false
				}
			}
			else {
				config.logger.log(LogType.ERROR, "Multiple StudyIDs are detected!")
				return false
			}
		}
		else {
			config.logger.log(LogType.ERROR, "Study ID is not specified!")
			return false
		}
		
		return true;
	}
	
	@Override
	public String getProcedureName() {
		return "I2B2_LOAD_CLINICAL_DATA"
	}

	@Override
	public boolean runStoredProcedures(jobId, Sql sql, studyInfo) {
		def studyId = studyInfo['id']
		def studyNode = studyInfo['node']
		if (studyId && studyNode) {
			config.logger.log("Study ID=${studyId}; Node=${studyNode}")
			sql.call("{CALL i2b2_load_clinical_data($studyId,$studyNode,'N','N',$jobId)}")
		}
		else {
			config.logger.log(LogType.ERROR, "Study ID or Node not defined!")
			return false;
		}
		
		return true;
	}
	
	private Object processMappingFile(f) {
		def mappings = [:]
		
		config.logger.log("Mapping file: ${f.name}")
		
		def lineNum = 0
		
		f.splitEachLine("\t") { 
			cols ->
			
			lineNum++
			
			if (cols[0] && lineNum > 1) {
				if (! mappings[cols[0]]) {
					mappings[cols[0]] = [
						STUDY_ID : 0,
						SITE_ID : 0,
						SUBJ_ID : 0,
						VISIT_NAME : 0,
						_DATA : [:]
							// Label => { CATEGORY_CD => '', COLUNN => 1 } - 1-based column numbers
					];
				}
				
				def curMapping = mappings[cols[0]]
				
				def dataLabel = cols[3]
				if (dataLabel != 'OMIT') {
					if (curMapping.containsKey(dataLabel)) {
						curMapping[dataLabel] = cols[2].toInteger()
					}
					else {
						if ( cols[1] && cols[2].toInteger() > 0 ) {
							curMapping['_DATA'][dataLabel] = [
								CATEGORY_CD : cols[1],
								COLUMN : cols[2].toInteger()	
							]
						}
						else {
							config.logger.log(LogType.ERROR, "Category or column number is missing for line ${lineNum}")
							throw new Exception("Error parsing mapping file")
						}
					}
				}
			}
		}
		
		return mappings
	}
	
	private String fixColumn(String s) {
		if ( s == null ) return '';
		
		def res = s.trim()
		res = (res =~ /^\"(.+)\"$/).replaceFirst('$1')
		res = res.replace('\\', '')
		res = res.replace('%', 'PCT')
		res = res.replace('*', '')
		res = res.replace('&', ' and ')
	
		return res
	}

}
