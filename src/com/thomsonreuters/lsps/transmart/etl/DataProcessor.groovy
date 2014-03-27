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

abstract class DataProcessor {

	def config
	
	DataProcessor(conf) {
		config = conf
	}
	
	abstract boolean processFiles(File dir, Sql sql, studyInfo)
	abstract boolean runStoredProcedures(jobId, Sql sql, studyInfo)
	abstract String getProcedureName()

    boolean isPostgresConnection() {
        config.db?.jdbcConnectionString?.startsWith("jdbc:postgresql:")
    }

    boolean isLocalPostgresConnection() {
        isPostgresConnection() && config.db?.jdbcConnectionString?.matches('^jdbc:postgresql:(?://localhost(?::\\d+)?/)?(\\w+)$')
    }

	boolean process(File dir, studyInfo) {
		def res = false
		
		config.logger.log("Connecting to database server")
		def sql = Sql.newInstance( config.db.jdbcConnectionString, config.db.username, config.db.password, config.db.jdbcDriver )
		sql.connection.autoCommit = false
		
		if (processFiles(dir, sql, studyInfo)) {
			// run stored procedure(s) in the background here
			// poll log & watch for stored procedure to finish	
			
			// retrieve job number
			sql.call('{call ' + config.controlSchema + '.cz_start_audit(?,?,?)}', [getProcedureName(), config.db.username, Sql.NUMERIC]) {
				jobId ->
				
				sql.commit() // we need this for PostgreSQL version
				
				config.logger.log("Job ID: ${jobId}")
				
				def t = Thread.start {
					config.logger.log("Run procedures: ${getProcedureName()}")
					res = runStoredProcedures(jobId, sql, studyInfo)
					sql.commit() // we need it for postgreSQL version
				}
				
				def lastSeqId = 0
				
				// opening control connection for the main thread
				def ctrlSql = Sql.newInstance( config.db.jdbcConnectionString, config.db.username, config.db.password, config.db.jdbcDriver )
				
				while (true) {
					// fetch last log message
					ctrlSql.eachRow("SELECT * FROM " + config.controlSchema + ".cz_job_audit WHERE job_id=${jobId} and seq_id>${lastSeqId} order by seq_id") {
						row ->
						
						config.logger.log(LogType.DEBUG, "-- ${row.step_desc} [${row.step_status} / ${row.records_manipulated} recs / ${row.time_elapsed_secs}s]")
						lastSeqId = row.seq_id
					}
					
					if (! t.isAlive() ) break
					
					Thread.sleep(2000)
				}
				
				// closing control connection - don't need it anymore
				ctrlSql.close()
				
				// figuring out if there are any errors in the error log
				sql.eachRow("SELECT * FROM " + config.controlSchema + ".cz_job_error where job_id=${jobId} order by seq_id") {
					config.logger.log(LogType.ERROR, "${it.error_message} / ${it.error_stack} / ${it.error_backtrace}")
					res = false
				}
				
				if (res) {
					config.logger.log("Procedure completed successfully")
					sql.call("{call " + config.controlSchema + ".cz_end_audit(?,?)}", [jobId, 'SUCCESS'])
				}
				else {
					config.logger.log(LogType.ERROR, "Procedure completed with errors!")
					sql.call("{call " + config.controlSchema + ".cz_end_audit(?,?)}", [jobId, 'FAIL'])
				}
				
				sql.commit() // need this for PostgreSQL version
			}
			
		}
		
		sql.close()
		
		return res
	}

}
