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

import com.thomsonreuters.lsps.transmart.sql.Database
import com.thomsonreuters.lsps.transmart.sql.DatabaseType
import com.thomsonreuters.lsps.transmart.tools.ProcessLocker

class CommandLineTool {

    static main(args) {
        def version = "1.1.0"

        def cli = new CliBuilder(usage: 'tm_etl [options] [<data_dir>]')
        cli.with {
            c longOpt: 'config', args: 1, argName: 'config', 'Configuration filename'
            h longOpt: 'help', 'Show usage information'
            i longOpt: 'interactive', 'Interactive (console) mode: progress bar'
            n longOpt: 'no-rename', 'Don\'t rename folders when failed'
            v longOpt: 'version', 'Display version information and exit'
            t longOpt: 'use-t', 'Do not use Z datatype for T expression data (expert option)'
            s longOpt: 'stop-on-fail', 'Stop when upload is failed'
            m longOpt: 'move-study', args:2, argName:'old_path new_path', 'Move study'
            _ longOpt: 'highlight-clinical-data', 'Highlight studies with clinical data'
            _ longOpt: 'alt-clinical-proc', args: 1, argName: 'proc_name', 'Name of alternative clinical stored procedure (expert option)'
            _ longOpt: 'alt-control-schema', args: 1, argName: 'schema', 'Name of alternative control schema (TM_CZ) - expert option'
            _ longOpt: 'secure-study', 'Make study securable'
            _ longOpt: 'visit-name-first', 'Put VISIT_NAME before the data value'
            _ longOpt: 'always-set-visit-name', 'Add visit name to concept path even if only one visit found'
            _ longOpt: 'data-value-first', 'Put VISIT NAME after the data value (default behavior, use to override non-standard config)'
            _ longOpt: 'delete-study-by-id', args: 1, argName: 'delete_id', 'Delete study by id'
            _ longOpt: 'delete-study-by-path', args: 1, argName: 'delete_path', 'Delete study by path'
            _ longOpt: 'force-start', 'Force TM Data Loader start (even if another instance is already running)'
            _ longOpt: 'allow-non-unique-columns', 'Allow non-unique column names in clinical data files'
        }
        // TODO: implement stop-on-fail mode!
        def opts = cli.parse(args)

        if (opts?.h) {
            cli.usage()
            return
        }

        if (opts?.v) {
            println "Transmart ETL tool, version ${version}\nCopyright (c) 2012-2013, Thomson Reuters"
            return
        }

        def locker = ProcessLocker.get('tMDataLoader')
        if (!locker.tryLock()) {
            println "Probably another Transmart ETL tool instance is already running. This message may be result of previously incorrectly finished run. In this case, please, check manually if no other instances is running and if none remove ${locker.lockFile.absolutePath}"
            System.exit(-1)
            return
        }

        // read configuration file first
        def config
        def configFileName

        if (opts?.c) {
            configFileName = opts.c
            println ">>> USING CONFIG: ${configFileName}"
        } else {
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

        def database = new Database(config)
        if (opts?.i) {
            config.isInteractiveMode = true
            println ">>> USING INTERACTIVE MODE"
            Logger.setInteractiveMode(true)
        }

        if (opts?.'secure-study') {
            config.securitySymbol = 'Y'
            println ">>> STUDY WILL BE SECURABLE"
        } else {
            config.securitySymbol = 'N'
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

        if (opts?.'highlight-clinical-data') {
            config.highlightClinicalData = true
            println ">>> Studies with Clinical Data will be highlighted"
        }

        if (!config?.db?.jdbcConnectionString) {
            println "Database connection is not specified\n"
            return
        }

        if (opts?.'alt-clinical-proc') {
            config.altClinicalProcName = opts?.'alt-clinical-proc'
            println ">>> USING ALTERNATIVE CLINICAL PROCEDURE: ${opts?.'alt-clinical-proc'}"
        }

        if (opts?.'alt-control-schema') {
            config.controlSchema = opts?.'alt-control-schema'
        }

        if (config.controlSchema) {
            println ">>> USING ALTERNATIVE CONTROL SCHEMA: ${config.controlSchema}"
        } else {
            config.controlSchema = database.databaseType == DatabaseType.Postgres ? 'tm_dataloader' : 'tm_cz'
        }

        if (!config?.containsKey('visitNameFirst')) {
            config.visitNameFirst = false // default behavior
        }

        if (opts?.'visit-name-first') {
            config.visitNameFirst = true
            println ">>> OVERRIDING CONFIG: VISIT_NAME first"
        }

        if (opts?.'data-value-first') {
            config.visitNameFirst = false
            println ">>> OVERRIDING CONFIG: DATA_VALUE first"
        }

        if (config?.visitNameFirst) {
            println '>>> FYI: using VISIT_NAME before DATA_VALUE as default behavior (per config or command line)'
        }

        if (config?.alwaysSetVisitName) {
            println '>>> FYI: using Set VISIT_NAME as NULL'
        }

        if (opts?.'always-set-visit-name'){
            config.alwaysSetVisitName = true;
            println '>>> FYI: using Set VISIT_NAME as NULL'
        }

        if (opts?.'delete-study-by-id') {
            config.deleteStudyById = true;
            config.deleteStudyByIdValue = opts?.'delete-study-by-id';
            config.mdOperation = true;
            println ">>> DELETE DATA BY ID ${opts?.'delete-study-by-id'}"
        }

        if (opts?.'delete-study-by-path') {
            config.deleteStudyByPath = true;
            config.deleteStudyByPathValue = opts?.'delete-study-by-path';
            config.mdOperation = true;
            println ">>> DELETE DATA BY PATH ${opts?.'delete-study-by-path'}"
        }

        if (opts?.'move-study') {
            config.moveStudy = true;
            config.moveStudyOldPath = opts.ms[0]
            config.moveStudyNewPath = opts.ms[1];
            config.mdOperation = true;
            println ">>> MOVE STUDY from ${opts.ms[0]} to ${opts.ms[1]}"
        }

        if (opts?.'allow-non-unique-columns') {
            println ">>> Allow non unique column names"
            config.allowNonUniqueColumnNames = true
        }

        def extra_args = opts.arguments()
        def dir = extra_args[0] ?: config?.dataDir

        if (!dir) {
            println "Directory is not defined!"
            cli.usage()
            return
        }

        config.logger = new Logger()

        config.logger.log("!!! TM_ETL VERSION ${version}")
        config.logger.log("==== STARTED ====")

        boolean succeed
        if (config.mdOperation) {
            def processor = new OperationProcessor(config);
            succeed = processor.process()
        } else {
            def processor = new DirectoryProcessor(config)
            config.logger.log("Using directory: ${dir}")
            succeed = processor.process(dir)
        }
        if (!succeed && config.stopOnFail) {
            config.logger.log(LogType.ERROR, "Stop-On-Fail is active, exiting with status 1")
            System.exit(1)
        }
        config.logger.log("==== COMPLETED ====")
    }

}
