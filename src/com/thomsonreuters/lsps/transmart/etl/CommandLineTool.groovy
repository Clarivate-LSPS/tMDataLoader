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

import groovy.util.CliBuilder

class CommandLineTool {

	static main(args) {
		
		def version = "0.6"
		
		def cli = new CliBuilder(usage: 'tm_etl [options] [<data_dir>]')
		cli.with {
			c longOpt: 'config', args: 1, argName: 'config', 'Configuration filename'
			h longOpt: 'help', 'Show usage information'
			i longOpt: 'interactive', 'Interactive (console) mode: progress bar'
			n longOpt: 'no-rename', 'Don\'t rename folders when failed'
			v longOpt: 'version', 'Display version information and exit'
			t longOpt: 'use-t', 'Do not use Z datatype for T expression data (expert option)'
			s longOpt: 'stop-on-fail', 'Stop when upload is failed'
		}
		// TODO: implement stop-on-fail mode!
		def opts = cli.parse(args)
		
		if (opts?.h) {
			cli.usage()
			return		
		}
		
		if (opts?.v) {
			println "Transmart ETL tool, version ${version}\nCopyright (c) 2012, Thomson Reuters"
			return
		}
		
		// read configuration file first
		def config
		def configFileName
		
		if (opts?.c) {
			configFileName = opts.c
			println ">>> USING CONFIG: ${configFileName}"
		}
		else {
			configFileName = System.getProperty('user.home') + File.separator + '.tm_etl' + File.separator + 'Config.groovy'
		}
		
		try {
			config = new ConfigSlurper().parse(
				new File(configFileName).toURI().toURL()
			)
		}
		catch (e) {
			println "Error processing config: ${e}\n"
			return
		}
		
		if (opts?.i) {
			config.isInteractiveMode = true
			println ">>> USING INTERACTIVE MODE"
		}
		
		if (opts?.t) {
			config.useT = true
			println ">>> USING ORIGINAL 'T' DATA TYPE (EXPERT)"
		}
		
		if (opts?.n) config.isNoRenameOnFail = true
		
		if (opts?.s) {
			config.stopOnFail = true
			println ">>> WILL STOP ON FAIL"
		}
		
		if (! config?.db?.jdbcConnectionString) {
			println "Database connection is not specified\n"
			return
		}
		
		def extra_args = opts.arguments()
		def dir = extra_args[0]?:config?.dataDir
		
		if (! dir) {
			println "Directory is not defined!"
			cli.usage()
			return
		}
		
		config.logger = new Logger(config)
		
		config.logger.log("!!! TM_ETL VERSION ${version}")
		
		def processor = new DirectoryProcessor(config)
		if (! processor.process(dir)) {
			config.logger.log(LogType.ERROR, "Stop-On-Fail is active, exiting with status 1")
			System.exit(1)
		}
	}

}
